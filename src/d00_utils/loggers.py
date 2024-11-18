import logging
from pathlib import Path
from src.d00_utils.module_logger_loc import ModuleLoggerLoc


class StreamLoggerSetup:
    def __init__(self, name: str = __name__, level: int = logging.WARNING):
        """
        Initializes a stream logger that outputs logs to the console.

        Parameters:
            name (str): The name of the logger.
            level (int): The logging level.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Prevent adding multiple handlers to the same logger
        if not any(isinstance(handler, logging.StreamHandler) for handler in self.logger.handlers):
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def get_logger(self) -> logging.Logger:
        """
        Retrieves the configured stream logger.

        Returns:
            logging.Logger: The configured logger instance.
        """
        return self.logger

class FileLoggerSetup:
    def __init__(self, name: str = __name__, level: int = logging.INFO):
        """
        Initializes a file logger that writes logs to a file in the 'logs' directory.

        Parameters:
            name (str): The name of the logger.
            level (int): The logging level.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Prevent adding multiple handlers to the same logger
        if not any(isinstance(handler, logging.FileHandler) for handler in self.logger.handlers):
            # print(f'Name: {name}')
            # print(f'Getting logger dir')
            log_dir = ModuleLoggerLoc.get_logger_dir()
            # print(f'Log Dir: {log_dir}')
            log_file = log_dir / f"{name}.log"
            handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def get_logger(self) -> logging.Logger:
        """
        Retrieves the configured file logger.

        Returns:
            logging.Logger: The configured logger instance.
        """
        return self.logger


def getalogger(name: str, level: int = logging.INFO, logger_type: str = 'file') -> logging.Logger:
    """
    Retrieves a configured logger based on the specified type.

    Parameters:
        name (str): The name of the logger.
        level (int): The logging level.
        logger_type (str): Type of logger ('file' or 'stream').

    Returns:
        logging.Logger: The configured logger instance.
    """
    if logger_type == 'file':
        return FileLoggerSetup(name, level).get_logger()
    elif logger_type == 'stream':
        return StreamLoggerSetup(name, level).get_logger()
    else:
        raise ValueError("logger_type must be either 'file' or 'stream'")