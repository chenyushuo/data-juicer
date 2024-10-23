import re
from typing import Dict, Optional

from loguru import logger

from data_juicer.ops.base_op import OPERATORS, UNFORKABLE, Mapper
from data_juicer.utils.lazy_loader import LazyLoader
from data_juicer.utils.model_utils import get_model, prepare_model

torch = LazyLoader('torch', 'torch')
vllm = LazyLoader('vllm', 'vllm')

OP_NAME = 'generate_qa_from_text_mapper'


# TODO: Extend LLM-based OPs into API-based implementation.
@UNFORKABLE.register_module(OP_NAME)
@OPERATORS.register_module(OP_NAME)
class GenerateQAFromTextMapper(Mapper):
    """
    Mapper to generate question and answer pairs from text.
    Recommended model list: [
        'alibaba-pai/pai-llama3-8b-doc2qa',
        'alibaba-pai/pai-baichuan2-7b-doc2qa',
        'alibaba-pai/pai-qwen1_5-4b-doc2qa',
        'alibaba-pai/pai-qwen1_5-7b-doc2qa',
        'alibaba-pai/pai-qwen1_5-1b8-doc2qa',
        'alibaba-pai/pai-qwen1_5-0b5-doc2qa'
    ]
    These recommended models are all trained with Chinese data
    and are suitable for Chinese.
    """

    _accelerator = 'cuda'
    _batched_op = True

    def __init__(self,
                 hf_model: str = 'alibaba-pai/pai-qwen1_5-7b-doc2qa',
                 trust_remote_code: bool = False,
                 pattern: Optional[str] = None,
                 enable_vllm: bool = True,
                 tensor_parallel_size: Optional[int] = None,
                 max_model_len: Optional[int] = None,
                 max_num_seqs: int = 256,
                 sampling_params: Dict = {},
                 *args,
                 **kwargs):
        """
        Initialization method.
        :param hf_model: Hugginface model id.
        :param trust_remote_code: passed to transformers
        :param pattern: regular expression pattern to search for within text.
        :param enable_vllm: Whether to use vllm for inference acceleration.
        :param tensor_parallel_size: It is only valid when enable_vllm is True.
            The number of GPUs to use for distributed execution with tensor
            parallelism.
        :param max_model_len: It is only valid when enable_vllm is True.
            Model context length. If unspecified, will be automatically
            derived from the model config.
        :param max_num_seqs: It is only valid when enable_vllm is True.
            Maximum number of sequences to be processed in a single iteration.
        :param sampling_params: Sampling parameters for text generation.
            e.g {'temperature': 0.9, 'top_p': 0.95}
        :param args: extra args
        :param kwargs: extra args

        The default data format parsed by this interface is as follows:
        Model Input:
            蒙古国的首都是乌兰巴托（Ulaanbaatar）
            冰岛的首都是雷克雅未克（Reykjavik）
        Model Output:
            蒙古国的首都是乌兰巴托（Ulaanbaatar）
            冰岛的首都是雷克雅未克（Reykjavik）
            Human: 请问蒙古国的首都是哪里？
            Assistant: 你好，根据提供的信息，蒙古国的首都是乌兰巴托（Ulaanbaatar）。
            Human: 冰岛的首都是哪里呢？
            Assistant: 冰岛的首都是雷克雅未克（Reykjavik）。
            ...
        """

        super().__init__(*args, **kwargs)
        self.num_proc = 1

        if pattern is None:
            self.pattern = r'Human: (.*?)\nAssistant: (.*?)(?=\nHuman|$)'
        else:
            self.pattern = pattern

        self.enable_vllm = enable_vllm

        if enable_vllm:

            assert torch.cuda.device_count() >= 1, 'must be executed in CUDA'
            if not tensor_parallel_size:
                tensor_parallel_size = torch.cuda.device_count()
                logger.info(f'Set tensor_parallel_size to \
                    {tensor_parallel_size} for vllm.')
            self.model_key = prepare_model(
                model_type='vllm',
                pretrained_model_name_or_path=hf_model,
                trust_remote_code=trust_remote_code,
                tensor_parallel_size=tensor_parallel_size,
                max_model_len=max_model_len,
                max_num_seqs=max_num_seqs)
            self.sampling_params = vllm.SamplingParams(**sampling_params)
        else:
            self.model_key = prepare_model(
                model_type='huggingface',
                pretrained_model_name_or_path=hf_model,
                trust_remote_code=trust_remote_code)
            self.sampling_params = sampling_params

    def _extract_qa(self, output):
        """Extract qestion and answer pair from model output response."""
        qa_list = []

        pat = re.compile(self.pattern, re.DOTALL)
        qa_pairs = pat.findall(output)

        for qa in qa_pairs:
            user, assistant = qa
            qa_list.append((user.strip(), assistant.strip()))

        return qa_list

    def process_batched(self, samples, rank=None):
        model, processor = get_model(self.model_key, rank, self.use_cuda())

        keys = samples.keys()
        first_key = next(iter(keys))
        num_samples = len(samples[first_key])
        out_samples = {
            key: []
            for key in keys | {self.query_key, self.response_key}
        }
        for i in range(num_samples):
            sample = {key: samples[key][i] for key in keys}
            if self.enable_vllm:
                response = model.generate([sample[self.text_key]],
                                          self.sampling_params)
                output = response[0].outputs[0].text
            else:
                inputs = processor(sample[self.text_key],
                                   return_tensors='pt').to(model.device)
                response = model.generate(**inputs, **self.sampling_params)
                output = processor.decode(response.cpu()[0],
                                          skip_special_tokens=True)

            qa_list = self._extract_qa(output)

            if len(qa_list) > 0:
                for q, a in qa_list:
                    for k, v in sample.items():
                        out_samples[k].append(v)
                    out_samples[self.query_key].append(q)
                    out_samples[self.response_key].append(a)
            else:
                logger.info(
                    'No question and answer was extracted from this sample!')

        return out_samples