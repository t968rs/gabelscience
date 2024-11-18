import time
import os
import logging
import typing as T
from decimal import *




def timer_wrap(func, **kwargs):
    log_path = f"./logs/{func.__name__}.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger(func.__name__)
    logging.basicConfig(level=logging.INFO, filename=log_path, filemode="a",
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        getcontext().prec = 2
        if not isinstance(result, T.Union[T.Tuple, T.List]):
            logger.info(f"{func.__name__} took {Decimal(end) - Decimal(start)} s to copy\n")
        else:
            for r in result:
                logger.info(f"{func.__name__} took {Decimal(end) - Decimal(start)} s to copy\n"
                            f"  {result}")
        return result
    print(f"\nTimer: {func.__name__} loggged to {log_path}\n")

    return wrapper



class TimerLogger:
    def __init__(self, log_path):
        self.start_time = None
        self.log_path = log_path
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger('TimerLogger')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(self.log_path)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logger.addHandler(handler)
        return logger

    def start(self):
        self.start_time = time.time()
        self.logger.info("Timer started")

    def log(self, message):
        if self.start_time is None:
            raise ValueError("Timer has not been started. Call start() before logging.")
        elapsed_time = time.time() - self.start_time
        self.logger.info(f"{message} - Elapsed time: {elapsed_time:.2f} seconds")
