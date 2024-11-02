import time
import os
import logging
import typing as T
from decimal import *




def timer(func, **kwargs):
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
