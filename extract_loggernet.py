#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
import re
import os
import yaml
import time


def read_yaml(path):
    with open(path, "r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise e


def extract_time(line, cdl_type):
    # Extract time from CR3000 or CR1000 loggernet files
    if cdl_type == "CRXXXX" or cdl_type == "CR3000" or cdl_type == "CR1000":
        date_string = re.match(r"^\"(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)", line)
        if (date_string):
            year, month, day, hour, minute, second = date_string.groups()
            return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        return

    elif cdl_type == "CRXX" or cdl_type == "CR23" or cdl_type == "CR10":
        parsed_date = re.match(r"^\d+,(\d+),(\d+),(\d+),", line)
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
    else:
        raise Exception("CDL_TYPE must be in format CRXXXX or CRXX")


def extract_header_info(file, cdl_type):
    file.seek(0)
    header = ""
    for line in file:
        if extract_time(line, cdl_type):
            file.seek(0)
            return header
        header += line


def write_new_hourly_file(output_dir, file_name_format, prefix, extension, header, data, timestamp):
    filename = file_name_format
    filename = re.sub("PREFIX", prefix, filename)
    filename = re.sub("EXT", extension, filename)
    year, month, day, hour, minute, second = re.split('-|T|:', timestamp.isoformat())
    filename = re.sub("YYYY", year, filename)
    filename = re.sub("MM", month, filename)
    filename = re.sub("DD", day, filename)
    filename = re.sub("hh", hour, filename)
    filename = re.sub("mm", minute, filename)
    filename = re.sub("ss", second, filename)

    # Check if there is a file with this filename already.
    # If there is, then append this data to that file.
    filepath = os.path.join(output_dir, filename)
    if os.path.exists(filepath):
        with open(filepath, "a") as temp:
            temp.write(data)
    else:
        # create new file with header
        with open(filepath, "w") as temp:
            temp.write(header + data)


def set_file_handle(input_path, input_file, current_pos):
    filehandle = os.path.join(input_path, ".extract_loggernet_file_position.yaml")
    with open(filehandle, "w") as f:
        data = {
            "INPUT_FILE": input_file,
            "FILE_POSITION": current_pos
        }
        yaml.dump(data, f)


def parse_file_handle(input_path, input_file):
    filehandle = os.path.join(input_path, ".extract_loggernet_file_position.yaml")
    if os.path.isfile(filehandle):
        data = read_yaml(filehandle)
        if data["INPUT_FILE"] == input_file:
            return int(data["FILE_POSITION"])
    return 0


def process_file(input_path, input_file, cdl_type, output_dir, split_interval, file_name_format, rename_prefix=None, rename_extension=None):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input path does not exist: '{input_path}'")

    if not os.path.exists(output_dir):
        raise FileNotFoundError(f"Output directory does not exist: '{output_dir}'")

    # get file prefix and extension
    prefix, extension = re.split(r"\.|\/", input_file)[-2:]

    prefix = rename_prefix if rename_prefix else prefix
    extension = rename_extension if rename_extension else extension

    path = os.path.join(input_path, input_file)
    # If the input path exists, but the file doesn't...
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file does not exist: '{input_file}'")

    print(f"running extract loggernet on {input_path}")

    file = open(path, "r")

    header_info = extract_header_info(file, cdl_type)
    temp_data_lines = ""
    previous_timestamp = None
    current_file_position = parse_file_handle(input_path, input_file)

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
                write_new_hourly_file(output_dir, file_name_format, prefix, extension, header_info, temp_data_lines, previous_timestamp)
                set_file_handle(input_path, input_file, current_file_position)
            # print("end of file")
            break

        t = extract_time(line, cdl_type)
        if t:
            # print(line)
            # Check for changes in hour and
            # split data on the hour.
            # (Detect changes in day and
            # split on the day if specified).
            current_timestamp = t.replace(minute=0, second=0, microsecond=0)
            if split_interval == "DAILY":
                current_timestamp = current_timestamp.replace(hour=0)
            if previous_timestamp and current_timestamp > previous_timestamp:
                # print()
                # print('hour break')
                # print(previous_timestamp)
                # print(t.replace(minute=0, second=0, microsecond=0))
                # print()
                write_new_hourly_file(output_dir, file_name_format, prefix, extension, header_info, temp_data_lines, previous_timestamp)
                set_file_handle(input_path, input_file, current_file_position)
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
        raise FileNotFoundError(f"Input path does not exist: '{INPUT_PATH}'")

    # # print(os.getcwd())
    # input()
    # # print('changed')

    # # Change the current directory to the
    # # input path.
    # INPUT_PATH = os.path.abspath(args.input_path)
    # os.chdir(INPUT_PATH)


    # # print(os.getcwd())
    # input()

    conf_path = os.path.join(INPUT_PATH, "./extract_loggernet_conf.yaml")
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"Could not find `extract_loggernet_conf.yaml` in the given directory")

    conf = read_yaml(conf_path)

    # Set global variables from the config file
    CDL_TYPE = conf["CDL_TYPE"]
    OUTPUT_DIR = conf["OUTPUT_DIR"]
    SPLIT_INTERVAL = conf["SPLIT_INTERVAL"]
    FILE_NAME_FORMAT = conf["FILE_NAME_FORMAT"]
    INPUT_FILE = conf["INPUT_FILE"]

    # If RENAME_PREFIX or RENAME_EXTENSION are included in the
    # conf file, then rename the prefix and extension
    RENAME_PREFIX = conf["RENAME_PREFIX"] if "RENAME_PREFIX" in conf.keys() else None
    RENAME_EXTENSION = conf["RENAME_EXTENSION"] if "RENAME_EXTENSION" in conf.keys() else None

    process_file(INPUT_PATH, INPUT_FILE, CDL_TYPE, OUTPUT_DIR, SPLIT_INTERVAL, FILE_NAME_FORMAT, rename_prefix=RENAME_PREFIX, rename_extension=RENAME_EXTENSION)