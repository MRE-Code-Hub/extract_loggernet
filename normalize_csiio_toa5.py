#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime
from pathlib import Path


def read_tob3_header(path: Path) -> list[list[str]]:
    rows: list[list[str]] = []
    with path.open("rb") as source:
        for _ in range(5):
            line = source.readline().decode("utf-8").rstrip("\r\n")
            rows.append(next(csv.reader([line])))
    return rows


def normalize(source_path: Path, converted_path: Path, output_path: Path) -> int:
    source_header = read_tob3_header(source_path)
    table_name = source_header[1][0]
    file_metadata = source_header[0][:7]
    file_metadata[0] = "TOA5"
    output_header = [
        file_metadata + [table_name],
        ["TIMESTAMP", "RECORD", *source_header[2]],
        ["TS", "RN", *source_header[3]],
        ["", "", *source_header[4]],
    ]

    row_count = 0
    previous_timestamp: datetime | None = None
    with converted_path.open("r", encoding="utf-8", newline="") as converted:
        with output_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, lineterminator="\n", quoting=csv.QUOTE_ALL)
            writer.writerows(output_header)

            for line_number, line in enumerate(converted, start=1):
                if line_number <= 4:
                    continue

                timestamp_text, separator, remaining = line.rstrip("\r\n").partition(",")
                if not separator:
                    raise ValueError(f"Converted row {line_number} has no fields")

                timestamp = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")
                if previous_timestamp is not None and timestamp < previous_timestamp:
                    raise ValueError(f"Converted timestamps are not ordered at row {line_number}")

                output.write(f'"{timestamp_text}",{remaining}\n')
                previous_timestamp = timestamp
                row_count += 1

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize csiio TOA5 output for extract_loggernet")
    parser.add_argument("source", type=Path, help="Original TOB3 file")
    parser.add_argument("converted", type=Path, help="TOA5 file produced by csiio")
    parser.add_argument("output", type=Path, help="Normalized TOA5 output file")
    args = parser.parse_args()

    rows = normalize(args.source, args.converted, args.output)
    print(f"Wrote {rows} rows to {args.output}")


if __name__ == "__main__":
    main()
