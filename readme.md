# Extract Loggernet

## Description

`extract_loggernet.py` is used to read the infinitely growing
data files created by a Campbell Data Logger and extract each
hour of data into a separate timestamped file.

This script requires an `extract_loggernet_conf.yaml` configuration
file to be located in the specified input directory where the data
logger file is located.

Since the Campbell Data Loggers continuously append to a single `.dat` file,
`extract_loggernet.py` will only read data that has been added to
the `.dat` file since the last time it executed. To do this, it saves
its file position in a hidden `.extract_loggernet_file_position.yaml`
file within the input directory where the `.dat` file is located.

This script does not make any changes to the original logger file.
Each hour of data is extracted into a separate file, named
using the provided `file_name_format` parameter, and placed
in the given `output_dir`.

This script can be run from the command line, or you can
import this file as a module and call the `process_file`
function.

## Installation and Setup
- Clone this repo and install dependencies either using pip (recommended): `pip install -r requirements.txt`. You could instead create a conda environment if needed: `conda env create -f environment.yaml`.
- To run extract_loggernet from the terminal **you must create a new `extract_loggernet_conf.yaml` file
in the directory of the file you want to extract data from.** Pattern your conf file based on the `extract_loggernet_conf_example.yaml` file.

## Usage
Run from the terminal (EX: `./extract_loggernet.py /path/to/input/directory`),

or

Import as a python module and run it by calling the `process_file` function like so:

```
from extract_loggernet import extract_loggernet

extract_loggernet.process_file(
    input_path="/path/to/input/directory",
    input_file="name_of_file",
    output_dir="/where/to/put/extracted/files",
    cdl_type="CR1000",
    split_interval="HOURLY",
    file_name_format="PREFIX.YYYYMMDDhhmmss.EXT"
)
```


## Tests

Each subdirectory within `test_files` contains test files representing a single growing file, as well as a directory for expected output and actual output.

The test class in `/tests/test_extract_loggernet.py` will simulate a single growing file, and verify that the output is accurate and that the file position is saving correctly.
It does this by reading each of the test files consecutively and comparing the output with the expected for each file.
For example, it will process
`1-CR1000x_PWS_002_IPconnect_Met.dat`,
`2-CR1000x_PWS_002_IPconnect_Met.dat`,
`3-CR1000x_PWS_002_IPconnect_Met.dat`,
and `4-CR1000x_PWS_002_IPconnect_Met.dat`
in that order, comparing the files in the `/out` directory with those in the `expected` directory before processing the next file.

To run the tests, simply navigate to the top level directory of this repo, and run `pytest` in the terminal.

