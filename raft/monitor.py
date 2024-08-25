#!/usr/bin/env python

import os
import argparse
import logging
from datetime import datetime

from pydantic import BaseModel

#############################################
## Logging
#############################################

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

#############################################
## Log Parsing
#############################################


class LogMessage(BaseModel):
    server_name: str
    timestamp: datetime
    level: str
    message: str
    trails: list[str]


def filename_to_server_name(file: str):
    return file.split(".")[0]


def parse_log(server_name: str, line: str) -> LogMessage:
    parts = line.split(" ")
    # example: 2024-08-19 01:07:31,426
    timestamp = datetime.strptime(" ".join(parts[:2]), "%Y-%m-%d %H:%M:%S,%f")
    level = parts[2]
    message = " ".join(parts[3:])
    return LogMessage(
        server_name=server_name,
        timestamp=timestamp,
        level=level,
        message=message,
        trails=[],
    )


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
                logs: list[LogMessage] = []
                for line in lines:
                    try:
                        log = parse_log(server_name, line)
                        logs.append(log)
                    except Exception as e:
                        # Invalid log message format, adding to the trail of previous message
                        assert len(logs) > 0
                        logs[-1].trails.append(line)
                all_lines.extend(logs)

    # Sort lines based on timestamps
    sorted_logs = sorted(all_lines, key=lambda x: x.timestamp)

    # Write sorted lines to the output file
    with open(output_file, "w") as outfile:
        lines: list[str] = []
        for log in sorted_logs:
            line = f"{log.timestamp} {log.server_name.ljust(10)} {log.level.ljust(10)} {log.message}"
            for trail_line in log.trails:
                line += f"\t{trail_line}"
            lines.append(line)
        outfile.writelines(lines)


#############################################
## Main
#############################################


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
