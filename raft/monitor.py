#!/usr/bin/env python

import os
import argparse
from datetime import datetime
from pydantic import BaseModel


class LogMessage(BaseModel):
    server_name: str
    timestamp: datetime
    level: str
    message: str

    def __str__(self):
        return f"{self.timestamp} {self.server_name.ljust(10)} {self.level.ljust(10)} {self.message}"


def filename_to_server_name(file: str):
    return file.split(".")[0]


def parse_log(server_name: str, line: str) -> LogMessage | None:
    parts = line.split(" ")
    try:
        # example: 2024-08-19 01:07:31,426
        timestamp = datetime.strptime(" ".join(parts[:2]), "%Y-%m-%d %H:%M:%S,%f")
        level = parts[2]
        message = " ".join(parts[3:])
        return LogMessage(
            server_name=server_name, timestamp=timestamp, level=level, message=message
        )
    except Exception:
        print(f"Invalid log message format, skipped (server {server_name}):{line}")
        return None


def merge_logs(input_directory: str, output_file: str):
    all_lines: list[LogMessage] = []

    # Read all log files in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith(".log"):
            with open(
                os.path.join(input_directory, filename), "r", encoding="utf-8"
            ) as file:
                lines = file.readlines()
                server_name = filename_to_server_name(filename)
                logs = [parse_log(server_name, line) for line in lines]
                logs = [x for x in logs if x is not None]
                all_lines.extend(logs)

    # Sort lines based on timestamps
    sorted_logs = sorted(all_lines, key=lambda x: x.timestamp)

    # Write sorted lines to the output file
    with open(output_file, "w") as outfile:
        lines = [str(x) for x in sorted_logs]
        outfile.writelines(lines)


def parse_cml_args():
    parser = argparse.ArgumentParser(
        prog="Log Merger",
        description="Log Merger",
        epilog="Merge log files",
    )
    parser.add_argument(
        "--log_dir",
        default="logs",
        help="Path to the plant log dir",
    )
    parser.add_argument(
        "--output",
        default="all.log",
        help="Path to dump the merged log file",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_cml_args()
    merge_logs(args.log_dir, args.output)


if __name__ == "__main__":
    main()