# Extract Loggernet

[![Static Badge](https://img.shields.io/badge/DOI-10.11578/dc.20240322.9-blue)](https://doi.org/10.11578/dc.20240322.9)

[![License](https://img.shields.io/badge/License-BSD_2--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)


## Description

`extract_loggernet.py` is used to read the infinitely growing
data files created by a Campbell Data Logger and extract each
hour of data into a separate timestamped file.

Since the Campbell Data Loggers continuously append to a single `.dat` file,
`extract_loggernet.py` will only read data that has been added to
the `.dat` file since the last time it executed. To do this, it creates a `.extract_loggernet_cache/` folder within the
input directory where the `.dat` file is located. It will then write the file position to a hidden `.{filename_prefix}_file_position.yaml` file within that folder. (`{filename_prefix}` is
the input file name with the extension removed).

**NOTE: This means that `extract_loggernet.py` may write partial hourly files. If it is run again and there are more records in the `.dat` file, it will append to them to the appropriate file.**

This script does not make any changes to the original logger file.
Each hour of data is extracted into a separate file, named
using the provided `file_name_format` parameter, and placed
in the given `output_dir`.

This script can be run from the command line, or you can
import this file as a python module and call the `process_file`
function.

## Installation and Setup
- Clone this repo and install dependencies using pip (recommended): `pip install -r requirements.txt`. You could instead create a conda environment if needed: `conda env create -f environment.yaml`.

## Usage
Run from the terminal by passing in the path to a YAML conf file. Pattern this
conf file after the `extract_loggernet_conf_example.yaml` file. It must contain
values for `INPUT_FILE_PATH` and `OUTPUT_DIR`. (EX: `./extract_loggernet.py /path/to/config.yaml`),

or

Import as a python module and run it by calling the `process_file` function like so:

```
from extract_loggernet import extract_loggernet

extract_loggernet.process_file(
    input_file_path="/path/to/file.dat",
    output_dir="/where/to/put/extracted/files/",
    cdl_type="CR1000",
    split_interval="HOURLY",
    file_name_format="PREFIX.YYYYMMDDhhmmss.EXT"
)
```


## Tests

Each subdirectory within `test_files` contains test files representing a single growing file, as well as a directory for expected output and actual output.

The test class in `/tests/test_extract_loggernet.py` will simulate a single growing file, and verify that the output is accurate and that the file position is saving correctly.
It does this by reading each of the test files consecutively and comparing the output with the expected for each file. The test files are preceded by a number, which is removed for each test to create the effect of a single growing loggernet file.
For example, it will process
```
1-CR1000x_PWS_002_IPconnect_Met.dat,
2-CR1000x_PWS_002_IPconnect_Met.dat,
3-CR1000x_PWS_002_IPconnect_Met.dat,
4-CR1000x_PWS_002_IPconnect_Met.dat
``````
in that order, comparing the files in the `/out` directory with those in the `expected` directory before processing the next file.

To run the tests, simply navigate to the top level directory of this repo, and run `pytest` in the terminal.

You could also write your own test files, following the same directory structure, and include them in the test_files directory. To add them to the pytest class, simply add another 'block' to the fixture of the `parameters` function in the test class.

