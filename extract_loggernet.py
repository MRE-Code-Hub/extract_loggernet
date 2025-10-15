#!/usr/bin/env python3

"""
extract_loggernet.py is used to read the infinitely growing
data files of a Campbell Data Logger and extract each hour
of data into a separate timestamped file.

It does not make any changes to the original logger file; when it runs it will
begin reading the file after the line where it finished the previous
time. (It will only read data that has been added to the logger file
since the last time it ran). This data will be saved in a hidden
`.extract_loggernet_cache/.{input_file_name}_file_position.yaml` file
within the given input directory.

This script can be run from the command line by
giving it the path to a YAML configuration file
(see `extract_loggernet_conf_example.yaml`), or you can
import this file as a module and call the `process_file`
function.

"""

import argparse
from datetime import datetime, timedelta
import re
import os
from typing import Any, Optional
import yaml

# Global variable for cache path
CACHE_PATH: Optional[str] = None


def read_yaml(path: str) -> Any:
    """
    Parse yaml file and return dictionary.

    Parameters
    ----------
    path : str
        The path to the yaml file.

    Returns
    -------
    dict
        The dictionary representation of yaml file.
    """
    with open(path, "r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise e


def extract_time(line: str, cdl_type: str = "CR1000X") -> Optional[datetime]:
    """
    Extract time from CRXXX (i.e. CR3000 or CR1000)
    or CRXX (i.e. CR23 or CR10) loggernet files

    Parameters
    ----------
    line : str
        The line of a logger file to extract
        the time from. (i.e. file.readline())
    cdl_type : str
        The type of Campbell Data Logger. Default is CR1000X.
        Set to 'CR23' to read CR23 data logger files (since
        they have a different file format).

    Returns
    -------
    Object
        datetime object representing the parsed timestamp.
    """
    if cdl_type.upper() in ("CR1000X", "CR1000", "CR3000", "CRXXXX"):
        pattern = r"^\"(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)"
        date_string = re.match(pattern, line)
        if date_string:
            yr, mo, day, hr, minute, sec = list(map(int, date_string.groups()))
            return datetime(yr, mo, day, hr, minute, sec)
        return None

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
        return None

    raise Exception("Cannot process files for CDL_TYPE: {cdl_type}")


def extract_header_info(file: Any, cdl_type: str = "CR1000X") -> str:
    """
    Returns every line in the file that
    precedes the first line with a timestamp.

    For example, if the file were like so:

        "Blah blah header info here"
        "2009-11-30 23:59:00",19,272.7,272,96.6,150.9,31.52"

    This would return "Blah blah header info here"

    Parameters
    ----------
    file : <class '_io.TextIOWrapper'>
        The file object of the logger file to read from.
    cdl_type : str
        The type of Campbell Data Logger. Default is CR1000X.
        Set to 'CR23' to read CR23 data logger files (since
        they have a different file format).

    Returns
    -------
    str
        String containing any lines that
        precede the first line with a timestamp.
    """
    file.seek(0)
    header = ""
    for line in file:
        if extract_time(line, cdl_type):
            file.seek(0)
            return header
        header += line
    file.seek(0)
    return header


def write_new_hourly_file(
    output_dir: str,
    file_name_format: str,
    prefix: str,
    extension: str,
    header: str,
    data: str,
    timestamp: datetime,
) -> None:
    """
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
        You can also include directory separators to create nested
        directory structures (e.g. 'YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT'
        will create year and month subdirectories).
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
    """
    filename = file_name_format
    filename = re.sub("PREFIX", prefix, filename)
    filename = re.sub("EXT", extension, filename)
    parsed_t_stamp = re.split("-|T|:", timestamp.isoformat())
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

    # Create parent directories if they don't exist
    # (e.g., when FILE_NAME_FORMAT contains directory separators like YYYY/MM/)
    parent_dir = os.path.dirname(filepath)
    if parent_dir:  # Only create if there's a parent directory
        os.makedirs(parent_dir, exist_ok=True)

    if os.path.exists(filepath):
        with open(filepath, "a") as temp:
            temp.write(data)
    else:
        # create new file with header
        with open(filepath, "w") as temp:
            temp.write(header + data)


def set_file_handle(input_path: str, input_file: str, current_pos: int) -> None:
    """
    Save the current file handle position to a hidden
    `.{input_file}_file_position.yaml` file within
    a hidden `.extract_loggernet_cache` folder inside
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
    """
    prefix = re.split(r"\.|\/", input_file)[-2:][0]
    cache = f"{CACHE_PATH}{input_path}/.extract_loggernet_cache/"
    filehandle = os.path.join(cache, f".{prefix}_file_position.yaml")
    # make sure there is a .cache/ folder
    if not os.path.exists(cache):
        os.mkdir(cache)

    # write the file position
    with open(filehandle, "w") as f:
        data = {"INPUT_FILE": input_file, "FILE_POSITION": current_pos}
        yaml.dump(data, f)


def parse_file_handle(input_path: str, input_file: str) -> int:
    """
    Return the file position found in any
    `.extract_loggernet_cache/.{input_file}_file_position.yaml`
    file within the specified input_path for
    the given input_file. If none is found, return 0.

    Parameters
    ----------
    input_path : str
        The input path where the loggernet file resides.
    input_file : str
        The name of the input file for which to read the file position.
        This must match the name of the input file specified in any
        `.{input_file}_file_position.yaml` file found.

    Returns
    -------
    int
        The file position read. If none is found, this defaults to 0.
    """
    prefix = re.split(r"\.|\/", input_file)[-2:][0]
    cache_dir = f"{CACHE_PATH}{input_path}/.extract_loggernet_cache/"
    filehandle = os.path.join(cache_dir, f".{prefix}_file_position.yaml")

    if os.path.exists(filehandle):
        data = read_yaml(filehandle)
        if data["INPUT_FILE"] == input_file:
            return int(data["FILE_POSITION"])
    return 0


def process_file(
    input_file_path: str,
    output_dir: str,
    cdl_type: str = "CR1000X",
    split_interval: str = "HOURLY",
    file_name_format: str = "PREFIX.YYYYMMDDhhmmss.EXT",
    rename_prefix: Optional[str] = None,
    rename_extension: Optional[str] = None,
) -> None:
    """
    Read the input file from the given input_file_path and extract
    each hour of data into a separate timestamped file. Extracted files
    will be saved in the given output_dir with the specified file_name_format.

    Parameters
    ----------
    input_file_path : str
        The path to the loggernet file to extract chunks from.
    output_dir : str
        The path of the directory to place the extracted files in.
    cdl_type : str
        The type of Campbell Data Logger. Default is CR1000X.
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
        You can also include directory separators to create nested
        directory structures (e.g. 'YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT'
        will create year and month subdirectories).
    rename_prefix : str
        If set, this will replace original file's prefix
        when naming the extracted output files.
    rename_extension : str
        If set, this will replace the original input file's extension when
        naming the extracted output files.
    """
    # split the input directory into head and tail parts
    input_path, input_file = os.path.split(input_file_path)

    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"Input file path does not exist: '{input_file}'")

    if not os.path.exists(output_dir):
        raise FileNotFoundError(f"Output directory does not exist: '{output_dir}'")

    # get file prefix and extension
    prefix, extension = re.split(r"\.|\/", input_file)[-2:]
    prefix = rename_prefix if rename_prefix else prefix
    extension = rename_extension if rename_extension else extension

    print(f"running extract loggernet on {input_path}")

    with open(input_file_path, "r") as file:

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
                        previous_timestamp,
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
                        previous_timestamp,
                    )
                    set_file_handle(input_path, input_file, current_file_position)
                    temp_data_lines = ""

                previous_timestamp = current_timestamp
                temp_data_lines += line
            # update the file position
            current_file_position = file.tell()


def main() -> None:
    """Main function for command line execution."""
    parser = argparse.ArgumentParser(
        prog="extract_loggernet",
        description="""The extract_loggernet script is used to
        read the infinitely growing data files created by Loggernet
        and extract each hour of data into a separate timestamped file.
        This script requires a yaml configuration file containing the
        path to the input file, the path of the output
        directory to put extracted files in,
        and any other optional parameters.""",
    )

    parser.add_argument(
        "config_file", help="""Directory to a yaml configuration file"""
    )
    args = parser.parse_args()
    conf_path = args.config_file

    # Read the config file
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"""Path does not exist: {conf_path}""")

    conf = read_yaml(conf_path)

    # Required config file parameters
    try:
        output_dir = conf["OUTPUT_DIR"]
    except KeyError as exc:
        raise KeyError(
            "No key named OUTPUT_DIR in the yaml configuration file"
        ) from exc
    try:
        input_file_path = conf["INPUT_FILE_PATH"]
    except KeyError as exc:
        raise KeyError(
            "No key named INPUT_FILE_PATH in the yaml configuration file"
        ) from exc

    # Optional config file parameters
    cdl_type = conf.get("CDL_TYPE", "CR1000X")
    split_interval = conf.get("SPLIT_INTERVAL", "HOURLY")
    file_name_format = conf.get("FILE_NAME_FORMAT", "PREFIX.YYYYMMDDhhmmss.EXT")

    # If RENAME_PREFIX or RENAME_EXTENSION are included in the
    # conf file, then rename the prefix and extension
    rename_prefix = conf.get("RENAME_PREFIX")
    rename_extension = conf.get("RENAME_EXTENSION")

    # Set global cache path
    global CACHE_PATH
    CACHE_PATH = conf.get("CACHE_PATH", "")

    for infile in input_file_path:
        process_file(
            infile,
            output_dir,
            cdl_type=cdl_type,
            split_interval=split_interval,
            file_name_format=file_name_format,
            rename_prefix=rename_prefix,
            rename_extension=rename_extension,
        )


if __name__ == "__main__":
    main()
