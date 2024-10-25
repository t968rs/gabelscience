import time
import logging
from decimal import *

log_path = f"../{__name__}.log"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename=log_path, filemode="a",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def timer(func, **kwargs):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        getcontext().prec = 2
        logger.info(f"{func.__name__} took {Decimal(end) - Decimal(start)} s to copy\n"
                    f"  {len(result[0])} files, {result[1]} workers, {result[2]} MB")
        return result
    print(f"Timer: {func.__name__} loggged to {log_path}")

    return wrapper
