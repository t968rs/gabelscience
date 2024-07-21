
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

