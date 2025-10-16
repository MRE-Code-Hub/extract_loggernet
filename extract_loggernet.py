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
from typing import Any, Dict, List, Optional, Tuple
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


def resolve_input_files(
    input_config: Any, search_root: str = "/"
) -> List[Tuple[str, Dict[str, str]]]:
    """
    Resolve input file paths, supporting both simple paths/lists and
    pattern-based matching with named capture groups.

    Parameters
    ----------
    input_config : str, list, or dict
        Can be:
        - A single file path (str)
        - A list of file paths
        - A dict with 'pattern' key containing a regex pattern with named groups,
          and optional 'search_root' to limit where to search for files
    search_root : str
        Root directory to start searching when using pattern matching.
        Only used if input_config is a dict with 'pattern'.
        Defaults to "/" (entire filesystem).

    Returns
    -------
    list of tuples
        Each tuple contains (file_path, captured_groups_dict)
        where captured_groups_dict contains any named groups from pattern matching.
        Results are sorted by file path for predictable processing order.

    Examples
    --------
    Simple path:
        input_config = "/path/to/file.dat"
        returns: [("/path/to/file.dat", {})]

    Pattern with named groups (absolute path):
        input_config = {
            "pattern": r"^/data/(?P<site>\\w+)/(?P<logger>\\w+)/.*\\.dat$"
        }
        matches: /data/site1/logger1/file.dat
        returns: [("/data/site1/logger1/file.dat",
                  {"site": "site1", "logger": "logger1"})]

    Pattern relative to search_root (recommended):
        input_config = {
            "pattern": r"^(?P<site>\\w+)/(?P<logger>\\w+)/.*\\.dat$",
            "search_root": "/data"
        }
        matches: /data/site1/logger1/file.dat (searches only within /data,
                 pattern matches relative path: site1/logger1/file.dat)
        returns: [("/data/site1/logger1/file.dat",
                  {"site": "site1", "logger": "logger1"})]

        This approach avoids redundancy between pattern and search_root.
    """
    if isinstance(input_config, str):
        # Simple single file path
        return [(input_config, {})]

    if isinstance(input_config, list):
        # List of file paths
        return [(path, {}) for path in input_config]

    if isinstance(input_config, dict) and "pattern" in input_config:
        # Pattern-based matching
        pattern_str = input_config["pattern"]
        # Allow user to specify search_root to limit search scope
        search_dir = input_config.get("search_root", search_root)

        # Compile the regex pattern
        pattern = re.compile(pattern_str)

        # Find all files in the directory tree
        results = []
        for root, dirs, files in os.walk(search_dir):
            for filename in files:
                filepath = os.path.join(root, filename)

                # Try matching against full path first (for backward compatibility)
                match = pattern.match(filepath)

                # If no match and search_root is specified, try relative path
                if not match and "search_root" in input_config:
                    # Get path relative to search_root
                    rel_path = os.path.relpath(filepath, search_dir)
                    match = pattern.match(rel_path)

                if match:
                    # Extract named groups
                    captured_groups = match.groupdict()
                    results.append((filepath, captured_groups))

        # Sort results by filepath for predictable processing order
        return sorted(results, key=lambda x: x[0])

    # If we get here, unsupported format
    raise ValueError(
        f"INPUT_FILE_PATH must be a string, list, or dict with 'pattern' key. "
        f"Got: {type(input_config)}"
    )


# Define available placeholder functions
PLACEHOLDER_FUNCTIONS = {
    "lower": str.lower,
    "upper": str.upper,
    "title": str.title,
    "capitalize": str.capitalize,
}


def apply_placeholder_function(value: str, func_spec: str) -> str:
    """
    Apply a function to a placeholder value.

    Parameters
    ----------
    value : str
        The value to transform
    func_spec : str
        Function specification, e.g. "lower", "upper", "replace:_:-"

    Returns
    -------
    str
        Transformed value

    Examples
    --------
    >>> apply_placeholder_function("SiteA", "lower")
    'sitea'
    >>> apply_placeholder_function("site-b", "upper")
    'SITE-B'
    >>> apply_placeholder_function("my_site", "title")
    'My_Site'
    """
    # Check for functions with arguments (e.g., "replace:_:-")
    if ":" in func_spec:
        func_name, args_str = func_spec.split(":", 1)
        args = args_str.split(":")

        if func_name == "replace":
            if len(args) >= 2:
                old, new = args[0], args[1]
                return value.replace(old, new)
            else:
                raise ValueError(
                    f"replace function requires 2 arguments, got {len(args)}"
                )
        else:
            raise ValueError(f"Unknown function with arguments: {func_name}")

    # Simple function (no arguments)
    if func_spec in PLACEHOLDER_FUNCTIONS:
        return PLACEHOLDER_FUNCTIONS[func_spec](value)

    raise ValueError(f"Unknown placeholder function: {func_spec}")


def substitute_output_dir(
    output_dir_template: str, captured_groups: Dict[str, str]
) -> str:
    """
    Substitute captured group values into OUTPUT_DIR template.

    Supports functions via pipe syntax: {key|function} or {key|func1|func2}

    Parameters
    ----------
    output_dir_template : str
        Output directory path with placeholders like {site}, {logger}, etc.
        Can include functions: {site|lower}, {logger|upper}, etc.
    captured_groups : dict
        Dictionary of captured group names and their values

    Returns
    -------
    str
        Output directory path with placeholders replaced

    Examples
    --------
    >>> substitute_output_dir("/out/{site}/{logger}", {"site": "A", "logger": "B"})
    '/out/A/B'
    >>> substitute_output_dir("/out/{site|lower}", {"site": "SiteA"})
    '/out/sitea'
    >>> substitute_output_dir("/out/{site|lower}/{logger|upper}",
    ...                       {"site": "SiteA", "logger": "cr1000"})
    '/out/sitea/CR1000'
    """
    result = output_dir_template

    # Find all placeholders with optional functions: {key} or {key|func|func2}
    pattern = r"\{([^}|]+)(?:\|([^}]+))?\}"

    def replacer(match: Any) -> str:
        key = match.group(1)
        functions = match.group(2)  # Could be None or "lower" or "lower|upper"

        if key not in captured_groups:
            # Not a captured group, leave it as-is for other substitution
            return str(match.group(0))

        value = captured_groups[key]

        # Apply functions if specified
        if functions:
            for func_spec in functions.split("|"):
                func_spec = func_spec.strip()
                value = apply_placeholder_function(value, func_spec)

        return value

    result = re.sub(pattern, replacer, result)
    return result


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


def substitute_placeholders(
    template: str,
    timestamp: datetime,
    prefix: str = "",
    extension: str = "",
    captured_groups: Optional[Dict[str, str]] = None,
) -> str:
    """
    Substitute all placeholders in a template string.

    Supports both new {bracket} syntax and legacy BARE syntax for compatibility.
    Functions can be applied using pipe syntax: {YYYY|lower}, {PREFIX|upper}

    Parameters
    ----------
    template : str
        Template string with placeholders
    timestamp : datetime
        Timestamp for date/time placeholders
    prefix : str
        Original input filename prefix (without extension)
    extension : str
        Original input file extension
    captured_groups : dict, optional
        Captured groups from pattern matching

    Returns
    -------
    str
        Template with all placeholders substituted

    Examples
    --------
    New syntax (recommended):
        '{site}/{logger}/{YYYY}/{MM}/data.{YYYY}{MM}{DD}{hh}{mm}{ss}.csv'

    With functions:
        '{site|lower}/{logger|upper}/{YYYY}/data.{PREFIX|lower}.csv'

    Legacy syntax (still supported):
        'YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT'

    Mixed syntax (works):
        '{site}/YYYY/MM/{logger}_data.YYYYMMDDhhmmss.csv'
    """
    result = template

    # First, substitute captured groups (which already supports functions)
    if captured_groups:
        result = substitute_output_dir(result, captured_groups)

    # Parse timestamp
    parsed_t_stamp = re.split("-|T|:", timestamp.isoformat())
    year, month, day, hour, minute, second = parsed_t_stamp

    # Create mapping for bracket syntax with function support
    bracket_values = {
        "YYYY": year,
        "MM": month,
        "DD": day,
        "hh": hour,
        "mm": minute,
        "ss": second,
        "PREFIX": prefix,
        "EXT": extension,
    }

    # Substitute bracket syntax with optional functions: {YYYY|lower}
    pattern = r"\{([^}|]+)(?:\|([^}]+))?\}"

    def replacer(match: Any) -> str:
        key = match.group(1)
        functions = match.group(2)

        if key not in bracket_values:
            # Unknown placeholder, leave as-is
            return str(match.group(0))

        value = bracket_values[key]

        # Apply functions if specified
        if functions:
            for func_spec in functions.split("|"):
                func_spec = func_spec.strip()
                value = apply_placeholder_function(value, func_spec)

        return value

    result = re.sub(pattern, replacer, result)

    # Legacy syntax: YYYY, MM, etc. (for backward compatibility)
    result = re.sub("YYYY", year, result)
    result = re.sub("MM", month, result)
    result = re.sub("DD", day, result)
    result = re.sub("hh", hour, result)
    result = re.sub("mm", minute, result)
    result = re.sub("ss", second, result)
    result = re.sub("PREFIX", prefix, result)
    result = re.sub("EXT", extension, result)

    return result


def write_new_hourly_file(
    output_file_path: str,
    prefix: str,
    extension: str,
    header: str,
    data: str,
    timestamp: datetime,
    captured_groups: Optional[Dict[str, str]] = None,
) -> None:
    """
    Writes the given interval of data to a separate timestamped file.
    If the file already exists, then append the given data to the file.

    Parameters
    ---------
    output_file_path : str
        Template for the full output file path (combines directory and filename).
        Supports placeholders:
        - Timestamp: {YYYY}, {MM}, {DD}, {hh}, {mm}, {ss} (or bare YYYY, MM, etc.)
        - Input file: {PREFIX}, {EXT} (or bare PREFIX, EXT)
        - Captured groups: {group_name} for any named groups from pattern

        Examples:
        - '/output/PREFIX.{YYYY}{MM}{DD}{hh}{mm}{ss}.EXT'
        - '/output/{site}/{logger}/{YYYY}/{MM}/data.{YYYY}{MM}{DD}.csv'
        - '/output/YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT' (legacy)
    prefix : str
        The original input file's prefix (filename without extension).
        Used to replace PREFIX placeholder.
    extension : str
        The original input file's extension.
        Used to replace EXT placeholder.
    header : str
        The header lines of the original logger file,
        extracted by calling `extract_header_info`.
    data : str
        The data to write to the extracted file.
    timestamp : datetime.datetime object
        The datetime timestamp to substitute in to the output_file_path.
    captured_groups : dict, optional
        Dictionary of captured group names and values from pattern matching.
        These can be used as {name} placeholders in output_file_path.
    """
    # Substitute all placeholders
    filepath = substitute_placeholders(
        output_file_path, timestamp, prefix, extension, captured_groups
    )

    # Create parent directories if they don't exist
    parent_dir = os.path.dirname(filepath)
    if parent_dir:
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
    # make sure there is a .cache/ folder (including any parent directories)
    os.makedirs(cache, exist_ok=True)

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
    output_dir: str = "",
    cdl_type: str = "CR1000X",
    split_interval: str = "HOURLY",
    file_name_format: str = "PREFIX.YYYYMMDDhhmmss.EXT",
    rename_prefix: Optional[str] = None,
    rename_extension: Optional[str] = None,
    captured_groups: Optional[Dict[str, str]] = None,
    output_file_path: Optional[str] = None,
    write_incomplete_periods: bool = True,
) -> None:
    """
    Read the input file from the given input_file_path and extract
    each hour of data into a separate timestamped file.

    Parameters
    ----------
    input_file_path : str
        The path to the loggernet file to extract chunks from.
    output_dir : str, optional
        DEPRECATED: Use output_file_path instead.
        The path of the directory to place the extracted files in.
    cdl_type : str
        The type of Campbell Data Logger. Default is CR1000X.
        Set to 'CR23' to read CR23 data logger files (since
        they have a different file format).
    split_interval : str
        Set to "DAILY" to extract daily summaries
        instead of the default hourly summaries.
    file_name_format : str, optional
        DEPRECATED: Use output_file_path instead.
        A string specifying the format for naming the output files.
    rename_prefix : str, optional
        DEPRECATED: Use literal values in output_file_path instead.
    rename_extension : str, optional
        DEPRECATED: Use literal values in output_file_path instead.
    captured_groups : dict, optional
        Dictionary of captured group names and values from pattern matching.
        These can be used as {name} placeholders in output_file_path.
    output_file_path : str, optional
        RECOMMENDED: Template for the complete output file path.
        Combines directory and filename with full placeholder support.

        Supports placeholders (new {bracket} or legacy BARE syntax):
        - Timestamp: {YYYY}/{MM}/{DD}/{hh}/{mm}/{ss}
        - Input file: {PREFIX}, {EXT}
        - Captured groups: {group_name}

        Examples:
        - '/output/{site}/{logger}/{YYYY}/{MM}/data.{YYYY}{MM}{DD}.csv'
        - '/output/YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT' (legacy syntax works)

        If not provided, will construct from output_dir + file_name_format
        for backward compatibility.
    write_incomplete_periods : bool, optional
        If True (default), writes incomplete hours/days at end of file.
        If False, only writes complete periods, holding back the most recent
        incomplete data until the period is complete. Default is True for
        backward compatibility.
    """
    # Backward compatibility: construct output_file_path from old parameters
    if output_file_path is None:
        if not output_dir:
            raise ValueError("Either output_file_path or output_dir must be provided")
        output_file_path = os.path.join(output_dir, file_name_format)

    # split the input directory into head and tail parts
    input_path, input_file = os.path.split(input_file_path)

    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"Input file path does not exist: '{input_file}'")

    # For backward compatibility, check output_dir if provided
    # But with output_file_path, we'll create directories as needed
    if output_dir and not os.path.exists(output_dir):
        raise FileNotFoundError(f"Output directory does not exist: '{output_dir}'")

    # get file prefix and extension
    prefix, extension = re.split(r"\.|\/", input_file)[-2:]
    prefix = rename_prefix if rename_prefix else prefix
    extension = rename_extension if rename_extension else extension

    print(f"running extract loggernet on {input_file_path}")

    try:
        with open(input_file_path, "r", encoding="utf-8") as file:

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
                    if previous_timestamp and write_incomplete_periods:
                        write_new_hourly_file(
                            output_file_path,
                            prefix,
                            extension,
                            header_info,
                            temp_data_lines,
                            previous_timestamp,
                            captured_groups,
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
                            output_file_path,
                            prefix,
                            extension,
                            header_info,
                            temp_data_lines,
                            previous_timestamp,
                            captured_groups,
                        )
                        set_file_handle(input_path, input_file, current_file_position)
                        temp_data_lines = ""

                    previous_timestamp = current_timestamp
                    temp_data_lines += line
                # update the file position
                current_file_position = file.tell()

    except UnicodeDecodeError as e:
        print(
            f"ERROR: File '{input_file_path}' contains invalid UTF-8 encoding "
            f"at position {e.start}. File may be corrupted. Skipping this file."
        )
        return
    except OSError as e:
        print(
            f"ERROR: Failed to read file '{input_file_path}': {e}. "
            f"Skipping this file."
        )
        return


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

    # Check for new unified OUTPUT_FILE_PATH parameter
    output_file_path_template = conf.get("OUTPUT_FILE_PATH")

    # Backward compatibility: support old OUTPUT_DIR + FILE_NAME_FORMAT
    if output_file_path_template is None:
        # Required config file parameters (old way)
        try:
            output_dir_template = conf["OUTPUT_DIR"]
        except KeyError as exc:
            raise KeyError(
                "Configuration must have either OUTPUT_FILE_PATH or OUTPUT_DIR"
            ) from exc
        file_name_format = conf.get("FILE_NAME_FORMAT", "PREFIX.YYYYMMDDhhmmss.EXT")
    else:
        # New unified approach
        output_dir_template = None  # Not used with OUTPUT_FILE_PATH
        file_name_format = None  # Not used with OUTPUT_FILE_PATH

    try:
        input_file_config = conf["INPUT_FILE_PATH"]
    except KeyError as exc:
        raise KeyError(
            "No key named INPUT_FILE_PATH in the yaml configuration file"
        ) from exc

    # Optional config file parameters
    cdl_type = conf.get("CDL_TYPE", "CR1000X")
    split_interval = conf.get("SPLIT_INTERVAL", "HOURLY")
    write_incomplete_periods = conf.get("WRITE_INCOMPLETE_PERIODS", True)

    # Legacy parameters (deprecated)
    rename_prefix = conf.get("RENAME_PREFIX")
    rename_extension = conf.get("RENAME_EXTENSION")

    # Set global cache path
    global CACHE_PATH
    CACHE_PATH = conf.get("CACHE_PATH", "")

    # Resolve input files (supports patterns and captured groups)
    input_files = resolve_input_files(input_file_config)

    # Process each matched file
    for infile, captured_groups in input_files:
        try:
            if output_file_path_template is not None:
                # New unified approach: substitute captured groups into path
                output_file_path = substitute_output_dir(
                    output_file_path_template, captured_groups
                )

                process_file(
                    infile,
                    output_file_path=output_file_path,
                    cdl_type=cdl_type,
                    split_interval=split_interval,
                    rename_prefix=rename_prefix,
                    rename_extension=rename_extension,
                    captured_groups=captured_groups,
                    write_incomplete_periods=write_incomplete_periods,
                )
            else:
                # Backward compatibility: old OUTPUT_DIR + FILE_NAME_FORMAT
                output_dir = substitute_output_dir(output_dir_template, captured_groups)

                process_file(
                    infile,
                    output_dir,
                    cdl_type=cdl_type,
                    split_interval=split_interval,
                    file_name_format=file_name_format,
                    rename_prefix=rename_prefix,
                    rename_extension=rename_extension,
                    captured_groups=captured_groups,
                    write_incomplete_periods=write_incomplete_periods,
                )
        except Exception as e:
            print(
                f"ERROR: Unexpected error processing file '{infile}': {e}. "
                f"Skipping this file and continuing with next file."
            )
            continue


if __name__ == "__main__":
    main()
