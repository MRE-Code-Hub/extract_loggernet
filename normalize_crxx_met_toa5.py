#!/usr/bin/env python3

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

FIELD_NAMES = [
    "GPSYear",
    "GPSMon",
    "GPSDay",
    "GPSUTC",
    "GPSlat",
    "GPSlon",
    "Batt_Volt_Avg",
    "Batt_Bank1_12v_Avg",
    "Batt_Bank2_12v_Avg",
    "Batt_Bank1_44v_Avg",
    "Batt_Bank2_44v_Avg",
    "InternalTemp_Avg",
    "AirTC_Avg",
    "BP_mbar_Avg",
    "Rad1Raw_Avg",
    "Rad1_Avg",
    "Rad2Raw_Avg",
    "Rad2_Avg",
    "U_Wnd_Avg",
    "V_Wnd_Avg",
    "WS_ms_Avg",
    "WS_ms_Std",
    "True_WindDir",
    "Wdir_rel",
    "Wdir_stationary",
    "heading",
]

FIELD_UNITS = [
    "",
    "",
    "",
    "",
    "degrees",
    "degrees",
    "Volts",
    "",
    "",
    "",
    "",
    "",
    "Deg C",
    "mbar",
    "",
    "W/m²",
    "",
    "W/m²",
    "",
    "",
    "m/s",
    "m/s",
    "",
    "",
    "",
    "",
]

FIELD_PROCESSING = [
    "Smp",
    "Smp",
    "Smp",
    "Smp",
    "Smp",
    "Smp",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Avg",
    "Std",
    "Smp",
    "Smp",
    "Smp",
    "Smp",
]


def parse_crxx_timestamp(row: list[str]) -> datetime:
    year = int(row[1])
    day_of_year = int(row[2])
    hhmm = int(row[3])
    seconds = int(row[4])
    hour, minute = divmod(hhmm, 100)
    return datetime(year, 1, 1) + timedelta(
        days=day_of_year - 1,
        hours=hour,
        minutes=minute,
        seconds=seconds,
    )


def normalize(source_path: Path, output_path: Path, site: str) -> int:
    metadata = [
        "TOA5",
        f"{site}_001",
        "CR1000",
        "unknown",
        "converted",
        "CPU:converted",
        "42040",
        "Met",
    ]
    header = [
        metadata,
        ["TIMESTAMP", "RECORD", *FIELD_NAMES],
        ["TS", "RN", *FIELD_UNITS],
        ["", "", *FIELD_PROCESSING],
    ]

    decoded_rows: list[tuple[datetime, list[str]]] = []
    with source_path.open("r", encoding="utf-8", newline="") as source:
        for line_number, row in enumerate(csv.reader(source), start=1):
            if len(row) != 31:
                raise ValueError(f"Source row {line_number} has {len(row)} fields; expected 31")
            decoded_rows.append((parse_crxx_timestamp(row), row[5:]))

    decoded_rows.sort(key=lambda item: item[0])

    with output_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n", quoting=csv.QUOTE_ALL)
        writer.writerows(header)
        for record, (timestamp, payload) in enumerate(decoded_rows):
            writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), record, *payload])

    return len(decoded_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert legacy CRXX Met data to TOA5")
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--site", choices=("PWN", "PWS"), required=True)
    args = parser.parse_args()

    rows = normalize(args.source, args.output, args.site)
    print(f"Wrote {rows} rows to {args.output}")


if __name__ == "__main__":
    main()
