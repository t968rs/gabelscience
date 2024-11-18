# src/d00_utils/module_logger_loc.py

import logging
from pathlib import Path
from typing import Optional


# Module name
MODULENAME = "gabelscience"

class ModuleLoggerLoc:
    @staticmethod
    def get_lib_root() -> Path:
        """
        Finds the root directory of the 'gabelscience' module/library based on the folder name.

        Returns:
            Path: The root directory path.

        Raises:
            FileNotFoundError: If the 'gabelscience' directory is not found in the path hierarchy.
        """
        # Start from the directory where this file is located
        current_path = Path(__file__).parent.resolve()

        # Traverse up the directory tree to find the 'gabelscience' directory
        for parent in [current_path] + list(current_path.parents):
            if parent.name.lower() == MODULENAME.lower():
                return parent

        # If not found, raise an error
        raise FileNotFoundError(f"Could not find '{MODULENAME}' directory in the path hierarchy.")

    @classmethod
    def get_logger_dir(cls) -> Path:
        """
        Retrieves the 'logs' directory path at the root of the 'gabelscience' module/library.
        Creates the directory if it does not exist.

        Returns:
            Path: The path to the 'logs' directory.
        """
        lib_root = cls.get_lib_root()
        # print(f'Lib Root: {lib_root}')
        log_dir = lib_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir
