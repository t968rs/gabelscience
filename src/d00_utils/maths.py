import os


def find_largest_divisible_part(n, k):
    """
    Find the largest integer that divides N into k parts, each part divisible by 128.

    Parameters:
    - n (int): The large number.
    - k (int): The number of parts.

    Returns:
    - M (int): The largest integer that divides N into k parts, each part divisible by 128.
    """
    # Calculate the initial part size
    initial_part_size = n // k

    # Adjust the part size to be divisible by 128
    M = (initial_part_size // 128) * 128

    return M

def get_random_sample(item_list, n):
    import random

    if n > len(item_list):
        raise ValueError("Sample size n cannot be greater than the number of items in the list.")
    return random.sample(item_list, n)


def get_max_num_from_stringlist(str_list):

    # Check if string list contents are paths
    if os.path.exists(str_list[0]):
        str_list = [os.path.basename(path) for path in str_list if os.path.exists(path)]
    # Get max suffix number from digit in string
    numbers = []
    for s in str_list:
        digits = [c for c in s if c.isdigit()]
        numbers.append(int(''.join(digits)))

    return max(numbers) if numbers else 0


def get_union_of_lists(list_of_lists):
    """
    Given a list of lists, returns the union of all unique elements.

    Parameters:
    - list_of_lists (list of lists): The input list containing sublists.

    Returns:
    - union_set (set): A set containing all unique elements from the sublists.
    """
    union_set = set()
    for sublist in list_of_lists:
        union_set.update(sublist)
    return union_set
