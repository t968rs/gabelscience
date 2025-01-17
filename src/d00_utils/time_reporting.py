import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, Any, Union
import json
import time


@dataclass
class TimeInterval:
    """
    Represents a time interval with hours, minutes, and seconds.
    Provides formatted string output and comparison operations.
    """
    hours: int
    minutes: int
    seconds: float

    def __str__(self) -> str:
        return f"{self.hours} hours, {self.minutes} minutes, {self.seconds:.2f} seconds"

    @classmethod
    def from_seconds(cls, total_seconds: float) -> 'TimeInterval':
        """Creates a TimeInterval from total seconds."""
        hours = int(total_seconds // 3600)
        remaining = total_seconds - (hours * 3600)
        minutes = int(remaining // 60)
        seconds = remaining - (minutes * 60)
        return cls(hours, minutes, seconds)


@dataclass
class StopWatch:
    """
    A class for managing and recording execution times during processing operations.
    Handles time tracking, reporting, and persistence of timing information.
    """
    workspace: Union[Path, str, os.PathLike]
    options: Dict[str, Any]
    times_dictionary: Dict[str, float] = field(default_factory=dict)
    start_time: Optional[float] = None
    report_file: Optional[Path] = None

    def __post_init__(self):
        """Initialize paths and files after instance creation."""
        self.workspace = Path(self.workspace)
        self.report_file = self.workspace / "time_results.txt"
        self._ensure_workspace()

    def _update_iteration_options(self, options: Dict[str, Any]) -> None:
        """Update options for the current iteration."""
        self.options.update(options)

    def _ensure_workspace(self) -> None:
        """Ensure workspace and necessary files exist."""
        self.workspace.mkdir(parents=True, exist_ok=True)

        # Initialize report file with header if it doesn't exist
        if not self.report_file.exists():
            self._write_report_header()

    def _write_report_header(self) -> None:
        """Write initial header to the report file."""
        with self.report_file.open('a') as f:
            f.write("=== Processing Time Report ===\n")
            f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    def start_new_iteration(self) -> None:
        """Start tracking a new processing iteration."""
        self.start_time = time.time()
        self.times_dictionary = {'Start': self.start_time}
        self._log_iteration_start()

    def _log_iteration_start(self) -> None:
        """Log the start of a new iteration with configuration options."""
        with self.report_file.open('a') as f:
            f.write("\n\n=== New Iteration ===\n")
            f.write(f"Start Time: {datetime.fromtimestamp(self.start_time)}\n")
            f.write("\nConfiguration:\n")
            for option_name, value in self.options.items():
                f.write(f"  {option_name}: {value}\n")
            f.write("\nTimings:\n")

    def record_time(self, checkpoint_name: str) -> TimeInterval:
        """
        Record a timing checkpoint and calculate elapsed time.

        Args:
            checkpoint_name: Name of the timing checkpoint

        Returns:
            TimeInterval representing elapsed time since start
        """
        if self.start_time is None:
            self.start_new_iteration()

        current_time = time.time()
        self.times_dictionary[checkpoint_name] = current_time

        elapsed = TimeInterval.from_seconds(current_time - self.start_time)
        self._log_checkpoint(checkpoint_name, elapsed)
        return elapsed

    def import_times(self, check_point_dictionary: dict):
        """Import times from a dictionary with 'checkpoint_name' as key and 'time_val' as value.'
        Args:
            check_point_dictionary: Dictionary with 'checkpoint_name' as key and 'time_val' as value.

        Returns
            None
        """

        if self.start_time is None:
            self.start_new_iteration()

        if not check_point_dictionary:
            return
        elif all(v is None for v in check_point_dictionary.values()):
            return

        for name, time_val in check_point_dictionary.items():
            if not isinstance(time_val, (int, float)):
                continue

            self.times_dictionary[name] = time_val
            elapsed = TimeInterval.from_seconds(time_val - self.start_time)
            self._log_checkpoint(name, elapsed)


    def _log_checkpoint(self, checkpoint_name: str, elapsed: TimeInterval) -> None:
        """Log a timing checkpoint to the report file."""
        with self.report_file.open('a') as f:
            f.write(f"\n{checkpoint_name}:\n  Elapsed: {elapsed}\n")

    def get_elapsed_times(self) -> Dict[str, TimeInterval]:
        """
        Calculate elapsed times for all checkpoints.

        Returns:
            Dictionary mapping checkpoint names to TimeIntervals
        """
        if not self.start_time:
            return {}

        return {
            name: TimeInterval.from_seconds(time_val - self.start_time)
            for name, time_val in self.times_dictionary.items()
            if name.lower() != 'start'
        }

    def save_timing_data(self, output_file: Optional[str] = None) -> None:
        """
        Save complete timing data to a JSON file for later analysis.

        Args:
            output_file: Optional custom path for the JSON file
        """
        if not output_file:
            output_file = self.workspace / "timing_data.json"

        data = {
            'start_time': self.start_time,
            'checkpoints': self.times_dictionary,
            'configuration': self.options,
            'elapsed_times': {
                name: str(interval)
                for name, interval in self.get_elapsed_times().items()
            }
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

    def summarize(self) -> str:
        """
        Generate a summary of all timing information.

        Returns:
            Formatted string containing timing summary
        """
        elapsed_times = self.get_elapsed_times()
        if not elapsed_times:
            return "No timing data available"

        summary = ["=== Timing Summary ===",
                   f"Start Time: {datetime.fromtimestamp(self.start_time)}",
                   "\nCheckpoints:"]

        for name, interval in elapsed_times.items():
            summary.append(f"  {name}: {interval}")

        return "\n".join(summary)