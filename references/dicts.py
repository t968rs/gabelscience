from src.specs import literals
from dataclasses import dataclass
import typing as T


@dataclass
class ReturnPeriodLookup:
    """this is a dataclass that holds the return period lookup"""

    return_period: str
    return_file_string: str
    perc: float

    def __post_init__(self):

        if not isinstance(self.return_period, (str, float)):
            raise ValueError(f"Expected return_period to be a string or float, got {type(self.return_period)}")
        if not isinstance(self.return_file_string, (str, float)):
            raise ValueError(
                f"Expected return_file_string to be a string or float, got {type(self.return_file_string)}")

        if self.return_period not in literals.RETURNPERIOD:
            raise ValueError(f"Expected return_period to be one of {literals.RETURNPERIOD}, got {self.return_period}")
        if self.return_file_string not in literals.RETURNFILESTRING:
            raise ValueError(
                f"Expected return_file_string to be one of {literals.RETURNFILESTRING}, got {self.return_file_string}")
        if self.perc not in literals.PERC:
            raise ValueError(f"Expected perc to be one of {literals.PERC}, got {self.perc}")


def perc_from_str(arg):
    return round(float(arg.replace("_", ".").replace("pct", "")) / 100, 3)


def period_from_str(arg):
    return arg.replace("_", ".").replace("pct", "%")


def str_from_period(arg):
    return arg.replace(".", "_").replace("%", "pct")


def str_from_perc(arg):
    return str(round(arg * 100, 2)).replace(".", "_") + "pct"


def create_return_period(input_key: T.Union[str, float]) -> ReturnPeriodLookup:
    """
    Creates a ReturnPeriodLookup object from the provided return period.

    Parameters:
        input_key (str, float): Return period to use for the lookup.

    Returns:
        ReturnPeriodLookup: ReturnPeriodLookup object with the specified return period.
    """
    if isinstance(input_key, float):  # input_key is perc
        perc = input_key
        return_file_string = str_from_perc(perc)

        return_period = period_from_str(return_file_string)
        print(f'perc: {perc}, return_file_string: {return_file_string}, return_period: {return_period}')
    elif "%" in input_key:  # input_key is return_period
        return_period = input_key
        return_file_string = str_from_period(return_period)
        perc = perc_from_str(return_file_string)
    else:  # input_key is return_file_string
        return_file_string = input_key
        perc = perc_from_str(return_file_string)
        return_period = period_from_str(return_file_string)

    return ReturnPeriodLookup(return_period=return_period, return_file_string=return_file_string, perc=perc)
