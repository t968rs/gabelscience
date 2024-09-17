import os


def get_system_memory() -> int:
    process = os.popen('wmic memorychip get capacity')
    result = process.read().split(" \n")
    for i in range(len(result)):
        result[i] = result[i].replace('\n', '')
        result[i] = result[i].replace(' ', '')
    process.close()
    total_mem = 0
    print(f'Result: {result}\n')
    for m in result:
        # print(f'M: {m}, {type(m)}')
        if m.lower() in ['capacity', '']:
            pass
        else:
            formatted = m  # m.split("  \r\n")[1:-1]
            total_mem += int(formatted)

    system_memory = int(round(total_mem / (1024 ** 3)))
    print(f"Sytem Memory: {system_memory} GB\n")

    return system_memory


def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def file_size(file_path: str) -> [str, float]:
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        size_str = convert_bytes(file_info.st_size)
        size, units = size_str.split(" ")
        sizef = float(size)
        return size_str, sizef, units
    else:
        raise FileNotFoundError(f"File not found: {file_path}")
