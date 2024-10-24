import unittest
from loguru import logger
from data_juicer.ops.mapper.optimize_query_mapper import OptimizeQueryMapper
from data_juicer.utils.unittest_utils import (SKIPPED_TESTS,
                                              DataJuicerTestCaseBase)

# Skip tests for this OP in the GitHub actions due to disk space limitation.
# These tests have been tested locally.
@SKIPPED_TESTS.register_module()
class OptimizeQueryMapperTest(DataJuicerTestCaseBase):

    def _run_op(self, enable_vllm=False):
        op = OptimizeQueryMapper(
            hf_model='alibaba-pai/Qwen2-7B-Instruct-Refine',
            enable_vllm=enable_vllm
        )

        samples = [{
            'query': '鱼香肉丝怎么做？', 
            'response': '鱼香肉丝是将猪肉丝与胡萝卜、青椒、木耳炒制，调入调味料如酱油、醋和辣豆瓣酱，快速翻炒而成的美味佳肴。'
        }]

        for sample in samples:
            result = op.process(sample)
            logger.info(f'Output results: {result}')
            # Note: If switching models causes this assert to fail, it may not be a code issue; 
            # the model might just have limited capabilities.
            self.assertNotEqual(result['query'], '')
        
    def test(self):
        self._run_op()

    def test_vllm(self):
        self._run_op(enable_vllm=True)


if __name__ == '__main__':
    unittest.main()
