#!/usr/bin/env python3

"""
extract_loggernet.py is used to read the infinitely growing
data files of a Campbell Data Logger and extract each specified
interval of data into a separate timestamped file. It does not
make any changes to the original logger file; when it runs it will
begin reading the file after the line where it finished the previous
time. (It will only read data that has been added to the logger file
since the last time it ran). This data will be saved in a hidden
`.extract_loggernet_file_position.yaml` file within the given input directory.
This script requires an `extract_loggernet_conf.yaml` configuration file to
be located in the specified input directory
where the logger data file is located.

This script can be run from the command line, or you can
import this file as a module and call the `process_file`
function.

"""

import argparse
from datetime import datetime, timedelta
import re
import os
import yaml


def read_yaml(path):
    '''
    Parse yaml file and return dictionary.

    Parameters
    ----------
    path : str
        The path to the yaml file.

    Returns
    -------
    dict
        The dictionary representation of yaml file.
    '''
    with open(path, "r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise e


def extract_time(line, cdl_type="CR1000X"):
    '''
    Extract time from CRXXX (i.e. CR3000 or CR1000)
    or CRXX (i.e. CR23 or CR10) loggernet files

    Parameters
    ----------
    line : str
        The line of a logger file to extract
        the time from. (i.e. file.readline())
    cdl_type : str
        The type of Campel Data Logger. Default is CR1000X.
        Set to 'CR23' to read CR23 data logger files (since
        they have a different file format).

    Returns
    -------
    Object
        datetime object representing the parsed timestamp.
    '''
    if cdl_type.upper() in ("CR1000X", "CR1000", "CR3000", "CRXXXX"):
        pattern = r"^\"(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)"
        date_string = re.match(pattern, line)
        if date_string:
            yr, mo, day, hr, minute, sec = list(map(int, date_string.groups()))
            return datetime(
                yr,
                mo,
                day,
                hr,
                minute,
                sec
            )
        return

    if cdl_type.upper() in ("CR23", "CR10", "CRXX"):
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

    raise Exception("Cannot process files for CDL_TYPE: {cdl_type}")


def extract_header_info(file, cdl_type="CR1000X"):
    '''
    Returns every line in the file that
    preceeds the first line with a timestamp.

    For example, if the file were like so:

        "Blah blah header info here"
        "2009-11-30 23:59:00",19,272.7,272,96.6,150.9,31.52"

    This would return "Blah blah header info here"

    Parameters
    ----------
    file : <class '_io.TextIOWrapper'>
        The file object of the logger file to read from.
    cdl_type : str
        The type of Campel Data Logger. Default is CR1000X.
        Set to 'CR23' to read CR23 data logger files (since
        they have a different file format).

    Returns
    -------
    str
        String containing any lines that
        preceed the first line with a timestamp.
    '''
    file.seek(0)
    header = ""
    for line in file:
        if extract_time(line, cdl_type):
            file.seek(0)
            return header
        header += line


def write_new_hourly_file(
        output_dir,
        file_name_format,
        prefix,
        extension,
        header,
        data,
        timestamp
        ):
    '''
    Writes the given interval of data to a separate timestamped file.
    If the file already exists, then append the given data to the file.

    Parameters
    ---------
    output_dir : str
        The directory to place the extracted file in.
    file_name_format : str
        A string specifying the format for naming the output files.
        'PREFIX' will be replaced with the given prefix parameter.
        Likewise with 'EXT'. 'YYYY' will be replaced with the year,
        'MM' with the month, 'DD' with the day, 'hh' with the hour,
        'mm' with the minute, and 'ss' with the second of the given
        timestamp parameter. (e.g. 'PREFIX.YYYYMMDDhhmmss.EXT')
    prefix : str
        The string to replace 'PREFIX' in the given file_name_format string.
    extension : str
        The string to replace 'EXT' in the given file_name_format string.
    header : str
        The header lines of the original logger file,
        extracted by calling `extract_header_info`.
    data : str
        The data to write to the extracted file.
    timestamp : datetime.datetime object
        The datetime timestamp to substitute in to the file_name_format string.
    '''
    filename = file_name_format
    filename = re.sub("PREFIX", prefix, filename)
    filename = re.sub("EXT", extension, filename)
    parsed_t_stamp = re.split('-|T|:', timestamp.isoformat())
    year, month, day, hour, minute, second = parsed_t_stamp
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
    '''
    Save the current file handle position to a hidden
    `.extract_loggernet_file_position.yaml` file within
    the specified input_path.

    Parameters
    ----------
    input_path : str
        The string to the input path where the loggernet
        file resides. This is where the file position data
        must be saved.
    input_file : str
        The name of the input file for which to save the file position.
    current_pos : int
        The current position of the file handle. (i.e. file.tell())
    '''
    filehandle = os.path.join(
        input_path,
        ".extract_loggernet_file_position.yaml"
    )
    with open(filehandle, "w") as f:
        data = {
            "INPUT_FILE": input_file,
            "FILE_POSITION": current_pos
        }
        yaml.dump(data, f)


def parse_file_handle(input_path, input_file):
    '''
    Return the file position found in any
    `.extract_loggernet_file_position.yaml`
    file within the specified input_path for
    the given input_file. If none is found, return 0.

    Parameters
    ----------
    input_path : str
        The input path where the loggernet file resides.
    input_file : str
        The name of the input file for which to read the file position.
        This must match the name of the input file specified in any
        `.extract_loggernet_file_position.yaml` file found.

    Returns
    -------
    int
        The file position read. If none is found, this defaults to 0.
    '''
    filehandle = os.path.join(
        input_path,
        ".extract_loggernet_file_position.yaml"
    )
    if os.path.isfile(filehandle):
        data = read_yaml(filehandle)
        if data["INPUT_FILE"] == input_file:
            return int(data["FILE_POSITION"])
    return 0


def process_file(
        input_path,
        input_file,
        output_dir,
        cdl_type="CR1000X",
        split_interval="HOURLY",
        file_name_format="PREFIX.YYYYMMDDhhmmss.EXT",
        rename_prefix=None,
        rename_extension=None):
    '''
    Read the given input_file within the specified input_path and extract
    each hour of data into a separate timestamped file. Extracted files
    will be saved in the given output_dir with the specified file_name_format.

    Parameters
    ----------
    input_path : str
        The path in which the input file from the Campbell Data Logger resides.
    input_file : str
        The name of the input loggernet file to read.
    output_dir : str
        The path of the directory to place the extracted files in.
    cdl_type : str
        The type of Campel Data Logger. Default is CR1000X.
        Set to 'CR23' to read CR23 data logger files (since
        they have a different file format).
    split_interval : str
        Set to "DAILY" to extract daily summaries
        instead of the default hourly summaries.
    file_name_format : str
        A string specifying the format for naming the output files.
        'PREFIX' will be replaced with the prefix of the input file,
        or with the given rename_prefix parameter.
        Likewise with 'EXT'. 'YYYY' will be replaced with the year,
        'MM' with the month, 'DD' with the day, 'hh' with the hour,
        'mm' with the minute, and 'ss' with the second of the given
        timestamp parameter. (e.g. 'PREFIX.YYYYMMDDhhmmss.EXT')
    rename_prefix : str
        If set, this will replace original file's prefix
        when naming the extracted output files.
    rename_extension : str
        If set, this will replace the original input file's extension when
        naming the extracted output files.
    '''
    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Input path does not exist: '{input_path}'"
        )

    if not os.path.exists(output_dir):
        raise FileNotFoundError(
            f"Output directory does not exist: '{output_dir}'"
        )

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
                write_new_hourly_file(
                    output_dir,
                    file_name_format,
                    prefix,
                    extension,
                    header_info,
                    temp_data_lines,
                    previous_timestamp
                )
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
                write_new_hourly_file(
                    output_dir,
                    file_name_format,
                    prefix,
                    extension,
                    header_info,
                    temp_data_lines,
                    previous_timestamp
                )
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
        description="""The extract_loggernet script is used to
        read the infinitely growing data files created by Loggernet
        and extract each hour of data into a separate timestamped file.
        This script requires an `extract_loggernet_conf.yaml` configuration
        file to be located in the specified input directory."""
    )

    parser.add_argument(
        'input_path',
        help="""Directory containing the input
        loggernet file and an extract_loggernet_conf.yaml file"""
    )
    args = parser.parse_args()
    INPUT_PATH = args.input_path

    # Read the config file
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input path does not exist: '{INPUT_PATH}'")

    conf_path = os.path.join(INPUT_PATH, "./extract_loggernet_conf.yaml")
    if not os.path.exists(conf_path):
        raise FileNotFoundError(
            f"""No file named `extract_loggernet_conf.yaml`
            in the {INPUT_PATH}"""
        )

    conf = read_yaml(conf_path)

    # Required config file parameters
    try:
        OUTPUT_DIR = conf["OUTPUT_DIR"]
    except KeyError as exc:
        raise KeyError(
            "No key named OUTPUT_DIR in the `extract_loggernet_conf.yaml` file"
            ) from exc
    try:
        INPUT_FILE = conf["INPUT_FILE"]
    except KeyError as exc:
        raise KeyError(
            "No key named INPUT_FILE in the `extract_loggernet_conf.yaml` file"
            ) from exc

    # Optional config file parameters
    CDL_TYPE = conf.get("CDL_TYPE", "CR1000X")
    SPLIT_INTERVAL = conf.get("SPLIT_INTERVAL", "HOURLY")
    FILE_NAME_FORMAT = conf.get(
        "FILE_NAME_FORMAT",
        "PREFIX.YYYYMMDDhhmmss.EXT"
        )

    # If RENAME_PREFIX or RENAME_EXTENSION are included in the
    # conf file, then rename the prefix and extension
    RENAME_PREFIX = conf.get("RENAME_PREFIX")
    RENAME_EXTENSION = conf.get("RENAME_EXTENSION")

    process_file(
        INPUT_PATH,
        INPUT_FILE,
        OUTPUT_DIR,
        cdl_type=CDL_TYPE,
        split_interval=SPLIT_INTERVAL,
        file_name_format=FILE_NAME_FORMAT,
        rename_prefix=RENAME_PREFIX,
        rename_extension=RENAME_EXTENSION
    )
