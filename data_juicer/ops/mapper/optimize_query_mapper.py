from data_juicer.ops.base_op import OPERATORS, UNFORKABLE
from data_juicer.ops.mapper import OptimizeQAMapper
from data_juicer.utils.lazy_loader import LazyLoader

torch = LazyLoader('torch', 'torch')
vllm = LazyLoader('vllm', 'vllm')

OP_NAME = 'optimize_query_mapper'


# TODO: Extend LLM-based OPs into API-based implementation.
@UNFORKABLE.register_module(OP_NAME)
@OPERATORS.register_module(OP_NAME)
class OptimizeQueryMapper(OptimizeQAMapper):
    """
    Mapper to optimize only query in question-answer pairs.
    """

    DEFAULT_SYSTEM_PROMPT = '优化问答对中的问题，将其更加详细具体，但仍可以由原答案回答。只输出优化后的问题，不要输出多余内容。'

    _accelerator = 'cuda'

    def parse_output(self, raw_output):
        return raw_output.strip(), None
