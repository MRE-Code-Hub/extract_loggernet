#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
import re
import os
import yaml
import time


def read_config(path):
    with open(path, "r") as conf:
        try:
            return yaml.safe_load(conf)
        except yaml.YAMLError as e:
            raise e


def extract_time(line):
    global previous_timestamp

    # Extract time from CR3000 or CR1000 loggernet files
    if CDL_TYPE == "CRXXXX" or CDL_TYPE == "CR3000" or CDL_TYPE == "CR1000":
        date_string = re.match("^\"(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)", line)
        if (date_string):
            year, month, day, hour, minute, second = date_string.groups()
            return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        return

    elif CDL_TYPE == "CRXX" or CDL_TYPE == "CR23" or CDL_TYPE == "CR10":
        parsed_date = re.match("^\d+,(\d+),(\d+),(\d+),", line)
        if parsed_date:
            # Date format for CRXX:
            # <random number>, yyyy, dd, hhmm
            # The hhmm may just be hmm if the hour
            # is a single digit. dd is the day of the year.
            # EX: line[0,4] returns [213, 2010, 49, 204]
            # year: 2010, 49th day of the year, hour: 2, min: 04
            year, yday, hhmm = list(map(int, parsed_date.groups()))

            # The nth day of the year includes
            # Jan 1, so subtract 1 day.
            # EX: the 48th day of a normal year is Feb 17
            # so date(yyyy, 1, 1) + delta(day=48-1) is Feb 17
            yday -= 1
            hh = int(hhmm / 100)
            mm = int(hhmm % 100)

            date = datetime(year, 1, 1)
            delta = timedelta(days=yday, hours=hh, minutes=mm)
            return date + delta

        return


def extract_header_info(file):
    file.seek(0)
    header = ""
    for line in file:
        if extract_time(line):
            file.seek(0)
            return header
        header += line


def write_new_hourly_file(head, data, timestamp):
    filename = FILE_NAME_FORMAT
    filename = re.sub("PREFIX", PREFIX, filename)
    filename = re.sub("EXT", EXTENSION, filename)
    year, month, day, hour, minute, second = re.split('-|T|:', timestamp.isoformat())
    filename = re.sub("YYYY", year, filename)
    filename = re.sub("MM", month, filename)
    filename = re.sub("DD", day, filename)
    filename = re.sub("hh", hour, filename)
    filename = re.sub("mm", minute, filename)
    filename = re.sub("ss", second, filename)

    # Check if there is a file with this filename already.
    # If there is, then append this data to that file.
    filepath = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath, "a") as temp:
            temp.write(data)
    else:
        # create new file with header
        with open(filepath, "w") as temp:
            temp.write(head + data)


def set_file_handle(current_pos):
    filehandle = os.path.join(INPUT_PATH, "file_handle.dat")
    with open(filehandle, "w") as f:
        f.write(str(current_pos))


def parse_file_handle():
    filehandle = os.path.join(INPUT_PATH, "file_handle.dat")
    if os.path.isfile(filehandle):
        with open(filehandle, "r") as f:
            store = f.read()
            if len(store) > 0:
                return int(store)
    return 0


def process_file(file):
    header_info = extract_header_info(file)
    temp_data_lines = ""
    previous_timestamp = None
    current_file_position = parse_file_handle()

    file.seek(current_file_position)

    while True:
        line = file.readline()
        if not line:
            # If we haven't crossed an hour boundary
            # before reaching the end of the file,
            # then save any leftover data anyway
            # and append to the file with the rest
            # of the data later.
            if previous_timestamp:
                write_new_hourly_file(header_info, temp_data_lines, previous_timestamp)
                set_file_handle(current_file_position)
            print("end of file")
            break

        t = extract_time(line)
        print(line)
        if t:
            # Check for changes in hour and
            # split data on the hour.
            # (Detect changes in day and
            # split on the day if specified).
            current_timestamp = t.replace(minute=0, second=0, microsecond=0)
            if SPLIT_INTERVAL == "DAILY":
                current_timestamp = current_timestamp.replace(hour=0)
            if previous_timestamp and current_timestamp > previous_timestamp:
                print()
                print('hour break')
                print(previous_timestamp)
                print(t.replace(minute=0, second=0, microsecond=0))
                print()
                write_new_hourly_file(header_info, temp_data_lines, previous_timestamp)
                set_file_handle(current_file_position)
                temp_data_lines = ""

            previous_timestamp = current_timestamp
            temp_data_lines += line
        # update the file position
        current_file_position = file.tell()

    file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="extract_loggernet",
        description="""The extract_loggernet script is used to read the infinitely growing
        data files created by Loggernet and extract each hour of data into a separate timestamped file.
        This script requires an `extrat_loggernet.conf.yaml` configuration
        file to be located in the specified input directory."""
    )

    parser.add_argument('input_path', help="Directory containing the input loggernet file and an extract_loggernet_conf.yaml file")
    args = parser.parse_args()
    INPUT_PATH = args.input_path

    ## Read the config file
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"No such file or directory: '{INPUT_PATH}'")

    conf_path = os.path.join(INPUT_PATH, "./extract_loggernet_conf.yaml")
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"Could not find `extract_loggernet_conf.yaml` in the given directory")

    conf = read_yaml(os.path.join(INPUT_PATH, "./extract_loggernet_conf.yaml"))

    # Set global variables from the config file
    CDL_TYPE = conf["CDL_TYPE"]
    OUTPUT_DIR = conf["OUTPUT_DIR"]
    SPLIT_INTERVAL = conf["SPLIT_INTERVAL"]
    FILE_NAME_FORMAT = conf["FILE_NAME_FORMAT"]
    INPUT_FILE = conf["INPUT_FILE"]

    # get file prefix and extension
    PREFIX, EXTENSION = re.split("\.|\/", INPUT_FILE)[-2:]

    # If RENAME_PREFIX or RENAME_EXTENSION are included in the
    # conf file, then rename the prefix and extension
    PREFIX = conf["RENAME_PREFIX"] if "RENAME_PREFIX" in conf.keys() else PREFIX
    EXTENSION = conf["RENAME_EXTENSION"] if "RENAME_EXTENSION" in conf.keys() else EXTENSION

    path = os.path.join(INPUT_PATH, INPUT_FILE)
    file = open(path, "r")

    process_file(file)