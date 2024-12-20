import os
from datetime import date
from zipfile import ZipFile, ZIP_DEFLATED

STRINGS_TO_INCLUDE = ["P02_", ".gdb"]


def archive_gdb(gdb_path, out_path):
    """
    Archives a specified geodatabase (GDB) file into a zip file.

    This function takes the path to a geodatabase file and compresses it into
    a specified output zip file using the `ZipFile` module. The resulting zip
    file will contain the original geodatabase file.

    Args:
        gdb_path (str): The file path of the geodatabase to be archived.
        out_path (str): The destination file path for the output zip file.

    Returns:
        bool: True if the geodatabase file was successfully archived.
    """
    with ZipFile(out_path, 'w', ZIP_DEFLATED) as archive:
        archive.write(gdb_path, os.path.basename(gdb_path))
        return True


def check_string_digit_count(string, tgt_count=None):
    """
    Check the count of digits in a string and identify consecutive digits

    This function analyzes a given string to compute the total count of
    digit characters. It optionally validates the digit count against a
    target count if provided. The function also extracts the longest
    sequence of consecutive digits starting from the beginning of the string.

    Parameters:
        string (str): The input string to be checked.
        tgt_count (Optional[int]): The optional target count to check if
            the total digit count exceeds it.

    Returns:
        Union[int, str]: Returns an integer `0` if any of the following
        occurs:
            (1) No digits are found in the string,
            (2) The digit count exceeds the target count.
        Otherwise, returns a string containing the identified consecutive
        digits from the input starting from the first digit.

    """
    if not any(char.isdigit() for char in string):
        return 0, ""
    number_digits = sum(char.isdigit() for char in string)

    consecuitive_numbers = ""
    last_digit = None
    for i, char in enumerate(string):
        if char.isdigit():
            print(f"{i}: {char}")
            if not last_digit:
                consecuitive_numbers += str(char)
                last_digit = i
            elif last_digit + 1 == i:
                consecuitive_numbers += str(char)
                last_digit = i
            else:
                break

    return len(consecuitive_numbers), consecuitive_numbers


def find_tgt_paths_matching(root_dir, digit_count=None, strings_to_include=None) -> str:
    """
        Searches and yields file paths within a directory tree that match specific criteria.

        This function iterates through a given directory tree, examines file names,
        and yields file paths that contain specified substrings and digit counts.

        Parameters:
        root_dir : str
            The root directory from which to begin the directory traversal.
        digit_count : Optional[int]
            The number of digits that the file name must contain
        strings_to_include : Optional[List[str]]
            A list of substrings that must be present in the file name

        Yields:
        str :
            Full path of files that match the specified conditions.
    """
    print(f"Searching for files in {root_dir}")
    for root, dirs, files in os.walk(root_dir):
        for folder in dirs:
            if all(s in folder for s in strings_to_include):
                print(f"\tIncludes the strings: {folder}")
                if digit_count:
                    string_to_check = folder
                    for s in strings_to_include:
                        string_to_check = string_to_check.replace(s, "")
                    counted, consecuitive_numbers = check_string_digit_count(string_to_check, digit_count)
                    print(f'\tCounted: {counted}')
                    if counted and counted == digit_count:
                        print(f"Found: {os.path.join(root, folder)}")
                        yield os.path.join(root, folder), consecuitive_numbers


def zip_a_gdb(input_path: str, output_path: str):
    # Derive the .gdb folder name from output file name (minus the .zip extension)
    gdb_folder_name = os.path.splitext(os.path.basename(output_path))[0]

    with ZipFile(output_path, 'w', ZIP_DEFLATED) as zipf:
        # Walk through all files in input_path
        for root, dirs, files in os.walk(input_path):
            for file in files:
                full_path = os.path.join(root, file)
                # Compute the relative path inside the input directory
                rel_path = os.path.relpath(full_path, input_path)
                # Place all files inside the gdb_folder_name directory in the ZIP
                arcname = os.path.join(gdb_folder_name, rel_path)

                zipf.write(full_path, arcname=arcname)

        test_result = zipf.testzip()
        if not test_result:
            return True
        else:
            print(f"Failed: {test_result}")
            return False

class SearchAndZip:

    def __init__(self, in_root, out_folder, strings2include=None, digit_n=None,
                 after_digits=None):
        self.input_root = in_root
        self.output_folder = out_folder
        self.strings_to_include = strings2include
        self.digit_count = digit_n
        self.after_digits = after_digits if after_digits else ""
        os.makedirs(self.output_folder, exist_ok=True)
        print(self.__dict__)

    def _create_in_out_lookup(self):

        """
        Creates a mapping of input paths to corresponding output paths for archiving purposes.

        This method searches for paths matching the required string patterns and digit
        count within the provided input root directory, determines their corresponding
        output paths based on the output folder (if specified) or default location,
        and archives the found paths into ZIP files. The function then associates
        the original input paths with their generated output paths in a dictionary.

        Returns:
            dict: A dictionary containing the matched input paths (as keys) and their
            corresponding output archive paths (as values).
        """
        in_out_lookup = {}

        # Find paths matching the string-needs and # of consequitive digits required
        for gdb_path, digit_str in find_tgt_paths_matching(self.input_root,
                                                           self.digit_count,
                                                           self.strings_to_include):

            # Create output name based on specific convention
            indir, filename = os.path.split(gdb_path)
            gdb_name, ext = os.path.splitext(filename)
            pre_digit_name = gdb_name.split(digit_str)[0]
            outname = f"{pre_digit_name}{digit_str}_{self.after_digits}{ext}.zip"

            # Determine output location
            if not self.output_folder:
                out_path = os.path.join(os.path.dirname(gdb_path), outname)
            else:
                out_path = os.path.join(self.output_folder, outname)

            # archive the GDB
            archive_gdb(gdb_path, out_path)
            in_out_lookup[gdb_path] = out_path
        return in_out_lookup

    def search_and_zip(self):
        in_out_lookup = self._create_in_out_lookup()
        for gdb_path, out_path in in_out_lookup.items():
            success = zip_a_gdb(gdb_path, out_path)
            if success:
                print(f"Zipped: {gdb_path} to {out_path}")
            else:
                print(f"Failed: {gdb_path} to {out_path}")


if __name__ == "__main__":
    input_root = r"Z:\Iowa_1A\03_delivery"
    output_folder = r"Z:\Iowa_1A\03_delivery\P02_GDB"
    strings_to_include = STRINGS_TO_INCLUDE
    digit_count = 8
    after_digits_suffix = date.today().strftime("%Y%m%d")
    initialize = SearchAndZip(input_root, output_folder, strings_to_include, digit_count, after_digits_suffix)
    initialize.search_and_zip()