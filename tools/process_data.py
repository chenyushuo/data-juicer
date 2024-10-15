import time
from loguru import logger

from data_juicer.config import init_configs
from data_juicer.core import Executor


@logger.catch(reraise=True)
def main():
    cfg = init_configs()
    if cfg.executor_type == 'default':
        executor = Executor(cfg)
    elif cfg.executor_type == 'ray':
        from data_juicer.core.ray_executor import RayExecutor
        executor = RayExecutor(cfg)
    st = time.time()
    executor.run()
    et = time.time()
    logger.info(f'Total time: {et - st:.2f}s')


if __name__ == '__main__':
    main()
