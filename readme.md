# Extract Loggernet

[![Static Badge](https://img.shields.io/badge/DOI-10.11578/dc.20240322.9-blue)](https://doi.org/10.11578/dc.20240322.9)

[![License](https://img.shields.io/badge/License-BSD_2--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)

## Description

`extract_loggernet.py` is used to read the infinitely growing data files created by a Campbell Data Logger and extract each hour of data into a separate timestamped file.

Since the Campbell Data Loggers continuously append to a single `.dat` file, `extract_loggernet.py` will only read data that has been added to the `.dat` file since the last time it executed. To do this, it creates a `.extract_loggernet_cache/` folder within the input directory where the `.dat` file is located. It will then write the file position to a hidden
`{filename_prefix}_file_position.yaml` file within that folder. (`{filename_prefix}` is the input file name with the extension removed).

**NOTE: This means that `extract_loggernet.py` may write partial hourly files. If it is run again and there are more records in the `.dat` file, it will append to them to the appropriate file.**

This script does not make any changes to the original logger file. Each hour of data is extracted into a separate file, named using the provided `file_name_format` parameter, and placed in the given `output_dir`.

This script can be run from the command line, or you can import this file as a python module and call the `process_file` function.

## Installation and Setup

### Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver. If you have `uv` installed, you can run the tool directly without manual setup:

```bash
# Run with a config file
uv run extract_loggernet.py /path/to/config.yaml

# Run tests
uv run pytest
```

`uv` will automatically create a virtual environment and install all dependencies from `pyproject.toml`.

### Manual Installation

Alternatively, you can manually create a virtual environment and install dependencies:

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the project
pip install -e .
```

## Usage

### Command Line

Run from the terminal by passing in the path to a YAML conf file. Pattern this conf file after the `extract_loggernet_conf_example.yaml` file. It must contain values for `INPUT_FILE_PATH` and either `OUTPUT_FILE_PATH` (recommended) or `OUTPUT_DIR`.

**Using uv:**

```bash
uv run extract_loggernet.py /path/to/config.yaml
```

**Traditional:**

```bash
./extract_loggernet.py /path/to/config.yaml
```

#### Testing Pattern Matching

Use the `--list-matches` flag to preview which files will be matched by your `INPUT_FILE_PATH` pattern without actually processing them:

```bash
uv run extract_loggernet.py /path/to/config.yaml --list-matches
```

This is especially useful when:

- Testing regex patterns before running extraction
- Verifying that your pattern matches the expected files
- Debugging pattern matching issues

Example output:

```text
Found 3 matching file(s):

  /data/site1/logger1/data.dat
    Captured groups: site=site1, logger=logger1
  /data/site2/logger2/data.dat
    Captured groups: site=site2, logger=logger2
  /data/site3/logger3/data.dat
    Captured groups: site=site3, logger=logger3

Total: 3 file(s)
```

### Python Module

or

Import as a python module and run it by calling the `process_file` function like so:

```python
from extract_loggernet import extract_loggernet

extract_loggernet.process_file(
    input_file_path="/path/to/file.dat",
    output_dir="/where/to/put/extracted/files/",
    cdl_type="CR1000",
    split_interval="HOURLY",
    file_name_format="PREFIX.YYYYMMDDhhmmss.EXT"
)
```

## Advanced Features

### Pattern Matching with Dynamic Output Directories and Filenames

You can use regex patterns with named capture groups to process multiple files and automatically organize outputs based on their input paths. **Captured groups can be used in both OUTPUT_DIR and FILE_NAME_FORMAT!**

**Example Configuration:**

```yaml
INPUT_FILE_PATH:
  pattern: '^/data/(?P<site>\w+)/(?P<logger>\w+)/.*\.dat$'
  search_root: /data # Optional: improves performance

OUTPUT_DIR: /output/{site}/{logger}

FILE_NAME_FORMAT: "{site}_{logger}_data.YYYYMMDDhhmmss.csv"
```

This configuration will:

1. Search for files matching the pattern (starting from `search_root` if specified)
2. Extract the `site` and `logger` names from each file's path using named capture groups
3. Use those captured values in **both** the output directory and the filename

For instance, a file at `/data/siteA/CR3000/measurements.dat` will have its extracted files created as:

- `/output/siteA/CR3000/siteA_CR3000_data.20251015120000.csv`
- `/output/siteA/CR3000/siteA_CR3000_data.20251015130000.csv`

**Pattern Syntax:**

- Use Python regex with named groups: `(?P<name>...)`
- Start patterns with `^` to match from the beginning of the full file path
- Captured group names can be used as `{name}` placeholders in both `OUTPUT_DIR` and `FILE_NAME_FORMAT`
- The optional `search_root` limits where to search for files (recommended for performance)

See `extract_loggernet_pattern_example.yaml` for a complete example with detailed comments.

### FILE_NAME_FORMAT Options

The `FILE_NAME_FORMAT` parameter supports multiple placeholder types:

**Timestamp placeholders:** `YYYY`, `MM`, `DD`, `hh`, `mm`, `ss`
**Input file placeholders:** `PREFIX` (original filename), `EXT` (original extension)
**Captured groups:** `{group_name}` from pattern matching

**Examples:**

```yaml
# Use original filename
FILE_NAME_FORMAT: PREFIX.YYYYMMDDhhmmss.EXT

# Use captured groups
FILE_NAME_FORMAT: '{site}_{logger}_data.YYYYMMDDhhmmss.csv'

# Nested directories with captured groups
FILE_NAME_FORMAT: 'YYYY/MM/{site}/data.YYYYMMDDhhmmss.csv'

# Mix everything together
FILE_NAME_FORMAT: 'YYYY/{site}/MM/{logger}_PREFIX.YYYYMMDDhhmmss.EXT'
```

**Note:** `RENAME_PREFIX` and `RENAME_EXTENSION` are deprecated. Use literal values in `FILE_NAME_FORMAT` instead.

### Nested Directory Structures

You can create nested directory structures in your output files by including path separators in `FILE_NAME_FORMAT`:

```yaml
FILE_NAME_FORMAT: YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT
```

This will organize files like:

```text
output_dir/
  2024/
    01/
      data.20240115120000.csv
    02/
      data.20240201080000.csv
```

Template tokens (YYYY, MM, DD, etc.) can be repeated in both the path and filename.

## Tests

Each subdirectory within `test/test_files` contains test files representing a single growing file, as well as a directory for expected output and actual output.

The test class in `/test/test_extract_loggernet.py` will simulate a single growing file, and verify that the output is accurate and that the file position is saving correctly. It does this by reading each of the test files consecutively and comparing the output with the expected for each file. The test files are preceded by a number, which is removed for each test to create the effect of a single growing loggernet file. For example, it will process

```text
1-CR1000x_PWS_002_IPconnect_Met.dat,
2-CR1000x_PWS_002_IPconnect_Met.dat,
3-CR1000x_PWS_002_IPconnect_Met.dat,
4-CR1000x_PWS_002_IPconnect_Met.dat
```

in that order, comparing the files in the `/out` directory with those in the `expected` directory before processing the next file.

To run the tests:

**Using uv (recommended):**

```bash
uv run pytest
```

**Traditional:**

```bash
pytest
```

You could also write your own test files, following the same directory structure, and include them in the `test/test_files` directory. To add them to the pytest class, simply add another 'block' to the fixture of the `parameters` function in the test class.
