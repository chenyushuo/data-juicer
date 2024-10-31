from pydantic import Field, PositiveInt
import regex
from typing import Optional
from typing_extensions import Annotated
import numpy as np
from loguru import logger
from collections import defaultdict
import pyarrow as pa
import uuid
import ray
from loguru import logger

from data_juicer.utils.constant import HashKeys, Fields
from data_juicer.utils.model_utils import prepare_sentencepiece_model
from ..common.helper_func import ActorUnionFind, split_on_whitespace
from .document_minhash_deduplicator import (
    sha1_hash32, optimal_param, MERSENNE_PRIME, MAX_HASH
)

from ..base_op import Filter, OPERATORS


OP_NAME = 'ray_minhash_deduplicator'


@OPERATORS.register_module(OP_NAME)
class RayMinhashDeduplicator(Filter):
    """
    A basic exact matching deduplicator for RAY.
    Although its functionality is deduplication,
    it is implemented as Filter sub-class.
    """

    # TODO: Set a more reasonable value
    EMPTY_HASH_VALUE = 'EMPTY'
    _batched_op = True

    def __init__(
        self,
        tokenization: str = 'space',
        window_size: PositiveInt = 5,
        lowercase: bool = True,
        ignore_pattern: Optional[str] = None,
        num_permutations: PositiveInt = 256,
        jaccard_threshold: Annotated[float, Field(ge=0, le=1)] = 0.7,
        num_bands: Optional[PositiveInt] = None,
        num_rows_per_band: Optional[PositiveInt] = None,
        tokenizer_model: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param tokenization: tokenization method for sample texts. It
            should be one of [space, punctuation, character,
            sentencepiece]. For English-like languages, we recommend
            to use 'space', for Chinese-like languages, we recommend
            to use 'character', and for multiple languages, we recommend
            to use 'sentencepiece'. If using 'sentencepiece', please
            provided the model path in the 'tokenizer_model' field.
        :param window_size: window size of shingling
        :param lowercase: whether to convert text to lower case first
        :param ignore_pattern: whether to ignore sub-strings with
            specific pattern when computing minhash
        :param num_permutations: number of permutations in minhash
            computing
        :param jaccard_threshold: the min jaccard similarity threshold
            in near-duplicate detection. When the jaccard similarity of
            two sample texts is >= this threshold, they are regarded as
            similar samples and this op will only keep one of them after
            deduplication
        :param num_bands: number of bands in LSH. Default it's None, and
            it will be determined by an optimal params computation
            algorithm by minimize the weighted sum of probs of False
            Positives and False Negatives
        :param num_rows_per_band: number of rows in each band in LSH.
            Default it's None, and it will be determined by an optimal
            params computation algorithm
        :param tokenizer_model: path for the sentencepiece model, used for
            sentencepiece tokenization.
        """
        super().__init__(*args, **kwargs)
        # about minhash computation
        self.tokenization = tokenization
        self.window_size = window_size
        self.lowercase = lowercase
        self.ignore_pattern = ignore_pattern
        if self.ignore_pattern:
            self.ignore_pattern = regex.compile(self.ignore_pattern)

        # check parameters
        if self.ignore_pattern and self.tokenization == 'punctuation':
            logger.warning('Be careful that tokenization with punctuations '
                           'won\'t work if the ignore pattern includes '
                           'punctuations.')
        self.punctuation_pattern = regex.compile(r'\p{P}')

        if self.tokenization == 'sentencepiece':
            if tokenizer_model is None:
                raise ValueError("To use 'sentencepiece' tokenization, "
                                 "'tokenizer_model' is required.")
            self.tokenizer = prepare_sentencepiece_model(tokenizer_model)
        else:
            self.tokenizer = None

        # about deduplication
        self.num_permutation = num_permutations
        self.jaccard_threshold = jaccard_threshold
        self.num_bands = num_bands
        self.num_rows_per_band = num_rows_per_band

        # initialize deduplication parameters
        # check number of bands and rows
        if self.num_bands is None or self.num_rows_per_band is None:
            self.num_bands, self.num_rows_per_band = optimal_param(
                self.jaccard_threshold,
                self.num_permutation,
            )

        # compute hash ranges and create hash tables
        self.hash_ranges = [(i * self.num_rows_per_band,
                             (i + 1) * self.num_rows_per_band)
                            for i in range(self.num_bands)]
        self.hash_tables = [defaultdict(set) for _ in range(self.num_bands)]

        # generate permutations
        gen = np.random.RandomState(seed=42)
        self.perm_a, self.perm_b = np.array(
            [(
                gen.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                gen.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            ) for _ in range(self.num_permutation)],
            dtype=np.uint64,
        ).T

        self.actor_union_find = ActorUnionFind.remote()

    def compute_stats_batched(self, samples, context=False):
        samples_list = samples[self.text_key]
        samples_stats = samples[Fields.stats]
        all_hash_values = [[] for _ in range(self.num_bands)]

        for idx, stat in enumerate(samples_stats):
            stat[HashKeys.uuid] = str(uuid.uuid4())

            text = samples_list[idx]
            if self.lowercase:
                text = text.lower()
            if self.ignore_pattern:
                text = self.ignore_pattern.sub('', text)

            # get tokens for different tokenization method
            tokens = set()
            if self.tokenization == 'character':
                tokens = {
                    str.encode(text[i:i + self.window_size])
                    for i in range(len(text) - self.window_size)
                }
            elif self.tokenization == 'punctuation':
                tokens = self.punctuation_pattern.split(text)
                tokens = {
                    str.encode(' '.join(tokens[i:i + self.window_size]))
                    for i in range(len(tokens) - self.window_size)
                }
            elif self.tokenization == 'space':
                tokens = split_on_whitespace(text)
                tokens = {
                    str.encode(' '.join(tokens[i:i + self.window_size]))
                    for i in range(len(tokens) - self.window_size)
                }
            elif self.tokenization == 'sentencepiece':
                tokens = self.tokenizer.encode(text, out_type=str)
                tokens = {
                    str.encode(''.join(tokens[i:i + self.window_size]))
                    for i in range(len(tokens) - self.window_size)
                }
            else:
                raise NotImplementedError(
                    f'Unimplemented tokenization method [{self.tokenization}]')

            # compute minhash value
            hv = np.array([sha1_hash32(token) for token in tokens],
                        dtype=np.uint64)
            phv = np.bitwise_and(
                ((hv * np.tile(self.perm_a,
                            (len(hv), 1)).T).T + self.perm_b) % MERSENNE_PRIME,
                MAX_HASH)
            hash_values = np.vstack([
                phv,
                np.ones(self.num_permutation, dtype=np.uint64) * MAX_HASH
            ]).min(axis=0)
            for i, (start, end) in enumerate(self.hash_ranges):
                all_hash_values[i].append(
                    bytes(hash_values[start:end].byteswap().data)
                )
        
        for i, hash_values in enumerate(all_hash_values):
            samples[HashKeys.minhash + f"_{i}"] = np.array(hash_values)
        return samples

    def agg_func(self, group: pa.Table) -> None:
        stats = group[Fields.stats]
        first_uuid = str(stats[0][HashKeys.uuid])
        ray.get([
            self.actor_union_find.union.remote(
                first_uuid, str(stat[HashKeys.uuid])
            )
            for stat in stats[1:]
        ])
        return group
    
    def process_batched(self, samples):
        stats = samples[Fields.stats]
        if isinstance(stats, list):
            results = ray.get([
                self.actor_union_find.is_ancestor.remote(
                    str(stat[HashKeys.uuid])
                )
                for stat in stats
            ])
            return results
        else:
            return ray.get(
                self.actor_union_find.is_ancestor.remote(
                    str(stats[HashKeys.uuid])
                )
            )
