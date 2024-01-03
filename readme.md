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
the `.dat` file since the last time it was run. To do this, it saves
its file position in a hidden `.extract_loggernet_file_position.yaml`
file within the input directory where the `.dat` file is located.

`extract_loggernet.py` does not make any changes to the original logger file.
Each hour of data is extracted into a separate file, named
using the provided `file_name_format` parameter, and placed
in the given `output_dir`.

This script can be run from the command line, or you can
import this file as a module and call the `process_file`
function.

## Installation and Setup
- Clone the repo
- Create a conda environment from the `environment.yaml` file: `conda env create -f environment.yaml`.
- Activate the conda environment: `conda activate extract_loggernet`.
- Create a new `extract_loggernet_conf.yaml` file in the directory of the file you want to extract
data from. Pattern your conf file based on the `extract_loggernet_conf_example.yaml` file.
- Run `extract_loggernet` using `python3 extract_loggernet.py /path/to/input/directory`. You could also
import `extract_loggernet` as a python module and run it by calling the `process_file` function like so:

```
from extract_loggernet import extract_loggernet

extract_loggernet.process_file(
    input_path=input_path,
    input_file=input_file,
    output_dir=output_dir,
    cdl_type=cdl_type,
    split_interval="HOURLY",
    file_name_format="PREFIX.YYYYMMDDhhmmss.EXT"
)
```

## Usage

## Credits


## License

