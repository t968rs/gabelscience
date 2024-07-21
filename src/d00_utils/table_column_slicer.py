import itertools


def list_slicer(list_to_slice: list, grouping_size: int) -> list:
    # Create groupings of 4 fields

    slices = []
    sliced = []
    while sliced != list_to_slice:
        for i in range(0, len(list_to_slice), grouping_size):
            if list_to_slice[i] is not None:
                g = list(itertools.islice(list_to_slice, i, i + grouping_size))
                print(f"Slice: {g}")
                slices.append(g)
                sliced.extend(g)

    slices = [[t for t in s if t] for s in slices]
    print(f"\n--Slices: {slices}")
    return slices


