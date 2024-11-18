import os
from datetime import datetime, timedelta

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


def increment_file_naming(file_path: str, inc_size=1) -> str:
    """
    this function will increment the file name by one, or user-specifed increment
    """
    if os.path.isfile(file_path):
        file_info = os.path.splitext(file_path)
        path, ext = file_info
        path_parts = path.split("_")
        if path_parts[-1].isdigit():
            path_parts[-1] = str(int(path_parts[-1]) + inc_size)
        else:
            path_parts.append('1')
        new_path = "_".join(path_parts) + ext
        return new_path
    else:
        return file_path


def get_path_parts(path):
    # Normalize the path
    path = os.path.normpath(path)

    # Split the drive letter (if any)
    drive, path = os.path.splitdrive(path)

    # Split the path into parts
    parts = []
    while True:
        path, tail = os.path.split(path)
        if tail:
            parts.append(tail)
        else:
            if path:
                parts.append(path)
            break

    # If there's a drive letter, add it to the parts
    if drive:
        parts.append(drive)

    # Reverse the parts to get the correct order
    parts.reverse()
    return parts

def find_largest_file(file_list=None, system_folder=None, recursive=False):
    if not file_list and not system_folder:
        raise ValueError("Either file_list or system_folder must be provided.")
    if system_folder and not os.path.isdir(system_folder):
        raise FileNotFoundError(f"System folder not found: {system_folder}")
    if recursive and file_list:
        raise ValueError("Cannot use recursive with file_list.")

    if not file_list and not recursive:
        file_list = [os.path.join(system_folder, f) for f in os.listdir(system_folder)
                     if os.path.isfile(os.path.join(system_folder, f))]
    elif not file_list and recursive:
        file_list = []
        for root, dirs, files in os.walk(system_folder):
            for file in files:
                file_list.append(os.path.join(root, file))

    largest_file = None
    max_size = 0
    for file_path in file_list:
        size = os.path.getsize(file_path)
        if size > max_size:
            max_size = size
            largest_file = file_path
    if largest_file:
        return largest_file
    return None

def is_file_recent(file_path, max_age_hours=8):
    if not os.path.exists(file_path):
        return False
    last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
    return datetime.now() - last_modified < timedelta(hours=max_age_hours)


def compare_file_timestamps(file1, file2, hr_tol=1):
    """
    Compare the last modified timestamps of two files.
    :param file1: The first file to compare.
    :param file2: The second file to compare.
    :param hr_tol: The number of hours to consider the files different.
    :return: True if the files were modified more than the tolerance, False otherwise.

    The second file should be the most recent file.
    """
    if os.path.exists(file1) and os.path.exists(file2):
        file1_time = os.path.getmtime(file1)
        file2_time = os.path.getmtime(file2)
        last_mod1 = datetime.fromtimestamp(file1_time)
        last_mod2 = datetime.fromtimestamp(file2_time)
        mod_delta = last_mod2 - last_mod1

        # Print comparison
        print(f"\nFiles last modified-- ")
        print(f"\t{os.path.split(file2)[1]}: {last_mod2}")
        print(f"\t{os.path.split(file1)[1]}: {last_mod1}")
        print(f"\tDelta: {mod_delta}\n")

        if mod_delta > timedelta(hours=hr_tol):
            print(f'\tFile 2 is more recent by more than {hr_tol} hours.')
            return False
        else:
            return True
