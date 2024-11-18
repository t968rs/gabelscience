import os
from typing import Union
import numpy as np


def save_list_to_npy(data, file_path) -> Union[str, os.PathLike]:
    if isinstance(data, list):
        data = np.array(data)
    np.save(file_path, np.array(data))
    return file_path


def load_from_numpy(file_path, count_only=False) -> np.array:
    if not count_only:
        return np.load(file_path, allow_pickle=True)
    else:
        array = np.load(file_path, allow_pickle=True)
        count = len(array)
        array = None
        return count
