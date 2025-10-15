"""
This will test that extract_loggernet.py is working correctly.
This will run through the test_files, and verify that
extract_loggernet.py is outputting as expected.

Run `pytest` in the terminal to run this test.
Run `pytest -s` to see extra printed output.
"""

import filecmp
import os
import re
from typing import Any, Generator, Tuple
import pytest
from extract_loggernet import extract_loggernet

# Change the directory to the top level directory
# of the extract_loggernet package
os.chdir(os.path.dirname(extract_loggernet.__file__))

# Initialize CACHE_PATH global variable for tests
extract_loggernet.CACHE_PATH = ""


def test_correct_dir() -> None:
    """
    Verify that code changed the directory to the
    module's top level directory.
    """
    assert os.getcwd() == os.path.dirname(extract_loggernet.__file__)


class TestGroup:
    """
    Runs extract_loggernet.py on a consecutive set of tests
    for each type of Campbell Data Logger and verifies that the
    output matches the expected number of files, name of each
    file, and content of each file. The test files are preceded
    by a number, which is removed for each test, to create the
    effect of a single growing loggernet file.
    """

    # Rather than use `pytest.mark.parametrize on the
    # TestGroup class, use a class scoped fixture.
    # This way we can remove redundancy by running
    # the extract loggernet script once for each file,
    # rather than once per test.
    @pytest.fixture(
        scope="class",
        params=[
            # (input_file_path, cdl_type)
            ("./test_files/CR3000/1-CR3000_Table213.dat", "CR3000"),
            ("./test_files/CR3000/2-CR3000_Table213.dat", "CR3000"),
            ("./test_files/CR3000/3-CR3000_Table213.dat", "CR3000"),
            ("./test_files/CR3000/4-CR3000_Table213.dat", "CR3000"),
            ("./test_files/CR23/1-GndRadData.dat", "CR23"),
            ("./test_files/CR23/2-GndRadData.dat", "CR23"),
            ("./test_files/CR23/3-GndRadData.dat", "CR23"),
            ("./test_files/CR23/4-GndRadData.dat", "CR23"),
            ("./test_files/CR23/5-GndRadData.dat", "CR23"),
            ("./test_files/CR23/6-GndRadData.dat", "CR23"),
            ("./test_files/PWS_002/1-CR1000x_PWS_002_IPconnect_Met.dat", "CR1000X"),
            ("./test_files/PWS_002/2-CR1000x_PWS_002_IPconnect_Met.dat", "CR1000X"),
            ("./test_files/PWS_002/3-CR1000x_PWS_002_IPconnect_Met.dat", "CR1000X"),
            ("./test_files/PWS_002/4-CR1000x_PWS_002_IPconnect_Met.dat", "CR1000X"),
            # add additional test_files to test here:
        ],
    )
    def parameters(self, request: Any) -> Any:
        return request.param

    @pytest.fixture(scope="class")
    def input_file(self, parameters: Tuple[str, str]) -> str:
        # just the filename
        return os.path.split(parameters[0])[1]

    @pytest.fixture(scope="class")
    def input_path(self, parameters: Tuple[str, str]) -> str:
        # just the path
        return os.path.split(parameters[0])[0]

    @pytest.fixture(scope="class")
    def cdl_type(self, parameters: Tuple[str, str]) -> str:
        return parameters[1]

    @pytest.fixture(scope="class")
    def output_dir(self, input_path: str) -> str:
        return os.path.join(input_path, "out")

    @pytest.fixture(scope="class")
    def expected_dir(self, input_path: str, input_file: str) -> str:
        return os.path.join(input_path, "expected", input_file)

    @pytest.fixture(scope="class")
    def setup(
        self,
        input_path: str,
        input_file: str,
        cdl_type: str,
        output_dir: str,
        expected_dir: str,
    ) -> Generator[Tuple[Any, Any], None, None]:
        print(f"running fixture {input_file}")
        # remove the number from the file name
        original_input_file_name = input_file
        match = re.search(r"\d-(.*)", input_file)
        if match is None:
            raise ValueError(f"Could not extract filename from {input_file}")
        input_file = match.groups()[0]
        os.rename(
            os.path.join(input_path, original_input_file_name),
            os.path.join(input_path, input_file),
        )

        # If this is the first of the tests
        # then clear any output files and saved data
        # to prep for the test.
        if original_input_file_name.startswith("1"):
            print(f"Removing out files and saved data for {input_file}")
            # remove saved file_position data
            prefix = re.split(r"\.|\/", input_file)[-2:][0]
            fn = f".{prefix}_file_position.yaml"
            saved_file_pos_path = os.path.join(
                input_path, ".extract_loggernet_cache/", fn
            )
            if os.path.exists(saved_file_pos_path):
                os.remove(saved_file_pos_path)

            # remove any files in the /out directory
            try:
                for filename in os.listdir(output_dir):
                    os.remove(os.path.join(output_dir, filename))
            except FileNotFoundError:
                os.mkdir(output_dir)

        # run the script on the renamed test_file
        extract_loggernet.process_file(
            input_file_path=os.path.join(input_path, input_file),
            output_dir=output_dir,
            cdl_type=cdl_type,
            split_interval="HOURLY",
            file_name_format="PREFIX.YYYYMMDDhhmmss.EXT",
        )

        out_filenames = sorted(
            [f for f in os.listdir(output_dir) if f.endswith(".dat")]
        )
        # Handle cases where expected directory doesn't exist
        # (e.g., first file that just initializes the system)
        if os.path.exists(expected_dir):
            expected_filenames = sorted(
                [f for f in os.listdir(expected_dir) if f.endswith(".dat")]
            )
        else:
            expected_filenames = []

        # return value and run test
        yield (out_filenames, expected_filenames)

        # ---------- CLEANUP (restore input_file name) -------
        os.rename(
            os.path.join(input_path, input_file),
            os.path.join(input_path, original_input_file_name),
        )

    def test_correct_number_of_files_created(self, setup: Tuple[Any, Any]) -> None:
        out_filenames, expected_filenames = setup
        expected = len([f for f in expected_filenames if f.endswith(".dat")])
        observed = len([f for f in out_filenames if f.endswith(".dat")])
        assert expected == observed

    def test_files_named_correctly(self, setup: Tuple[Any, Any]) -> None:
        out_filenames, expected_filenames = setup
        assert out_filenames == expected_filenames

    def test_files_have_correct_content(
        self, output_dir: str, expected_dir: str, setup: Tuple[Any, Any]
    ) -> None:
        out_filenames, expected_filenames = setup
        for name in expected_filenames:
            expected_file = os.path.join(expected_dir, name)
            out_file = os.path.join(output_dir, name)
            # compare contents of files
            if not filecmp.cmp(expected_file, out_file, shallow=False):
                print(f"{name} is not the same")
                assert False
        assert True


class TestDirectoryStructure:
    """
    Tests that FILE_NAME_FORMAT can create nested directory structures
    using date components like YYYY/MM in the path.
    """

    def test_nested_directory_creation(self) -> None:
        """
        Test that files are created in nested YYYY/MM directory structure
        when FILE_NAME_FORMAT includes directory separators.
        """
        import tempfile
        import shutil

        # Create a temporary directory for this test
        test_dir = tempfile.mkdtemp()
        try:
            # Use the first CR3000 test file
            input_file_path = "./test_files/CR3000/1-CR3000_Table213.dat"
            input_path, original_filename = os.path.split(input_file_path)

            # Copy the test file to temp directory to avoid affecting other tests
            temp_input = os.path.join(test_dir, "test_input.dat")
            shutil.copy(input_file_path, temp_input)

            output_dir = os.path.join(test_dir, "out")
            os.makedirs(output_dir, exist_ok=True)

            # Run process_file with nested directory format
            extract_loggernet.process_file(
                input_file_path=temp_input,
                output_dir=output_dir,
                cdl_type="CR3000",
                split_interval="HOURLY",
                file_name_format="YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT",
                rename_prefix="test_file",
                rename_extension="dat",
            )

            # Verify that YYYY/MM directories were created
            # The test file has data from 2009-11-30 (November only)
            year_2009_dir = os.path.join(output_dir, "2009")
            assert os.path.exists(
                year_2009_dir
            ), f"Year directory {year_2009_dir} was not created"
            assert os.path.isdir(year_2009_dir), f"{year_2009_dir} is not a directory"

            month_11_dir = os.path.join(year_2009_dir, "11")
            assert os.path.exists(
                month_11_dir
            ), f"Month directory {month_11_dir} was not created"
            assert os.path.isdir(month_11_dir), f"{month_11_dir} is not a directory"

            # Verify that files were created in the correct directory
            files_in_nov = os.listdir(month_11_dir)

            assert len(files_in_nov) > 0, "No files created in November directory"

            # Check that filenames follow the expected pattern
            for filename in files_in_nov:
                assert filename.startswith(
                    "test_file.200911"
                ), f"File {filename} doesn't match expected pattern"
                assert filename.endswith(
                    ".dat"
                ), f"File {filename} doesn't have .dat extension"

            print(f"✓ Created {len(files_in_nov)} files in 2009/11/")

        finally:
            # Cleanup: remove temporary directory
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_deep_nested_directory_creation(self) -> None:
        """
        Test that files can be created in deeply nested directory structures
        like YYYY/MM/DD.
        """
        import tempfile
        import shutil

        test_dir = tempfile.mkdtemp()
        try:
            # Use the first CR3000 test file
            input_file_path = "./test_files/CR3000/1-CR3000_Table213.dat"

            # Copy the test file to temp directory
            temp_input = os.path.join(test_dir, "test_input.dat")
            shutil.copy(input_file_path, temp_input)

            output_dir = os.path.join(test_dir, "out")
            os.makedirs(output_dir, exist_ok=True)

            # Run process_file with deeply nested directory format
            extract_loggernet.process_file(
                input_file_path=temp_input,
                output_dir=output_dir,
                cdl_type="CR3000",
                split_interval="HOURLY",
                file_name_format="YYYY/MM/DD/PREFIX_hh.EXT",
                rename_prefix="data",
                rename_extension="csv",
            )

            # Verify that YYYY/MM/DD directories were created
            # The test file has data from 2009-11-30
            deep_path = os.path.join(output_dir, "2009", "11", "30")
            assert os.path.exists(
                deep_path
            ), f"Deep directory structure {deep_path} was not created"
            assert os.path.isdir(deep_path), f"{deep_path} is not a directory"

            # Verify files were created
            files_in_dir = os.listdir(deep_path)
            assert len(files_in_dir) > 0, "No files created in deeply nested directory"

            # Check filename pattern
            for filename in files_in_dir:
                assert filename.startswith(
                    "data_"
                ), f"File {filename} doesn't start with 'data_'"
                assert filename.endswith(
                    ".csv"
                ), f"File {filename} doesn't have .csv extension"

            print(f"✓ Created {len(files_in_dir)} files in deeply nested 2009/11/30/")

        finally:
            # Cleanup
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_repeated_template_parts(self) -> None:
        """
        Test that template parts (YYYY, MM, etc.) can be repeated in both
        the directory path and filename, e.g., YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT
        """
        import tempfile
        import shutil

        test_dir = tempfile.mkdtemp()
        try:
            input_file_path = "./test_files/CR3000/1-CR3000_Table213.dat"
            temp_input = os.path.join(test_dir, "test_input.dat")
            shutil.copy(input_file_path, temp_input)

            output_dir = os.path.join(test_dir, "out")
            os.makedirs(output_dir, exist_ok=True)

            # Use format with YYYY and MM repeated in both path and filename
            extract_loggernet.process_file(
                input_file_path=temp_input,
                output_dir=output_dir,
                cdl_type="CR3000",
                split_interval="HOURLY",
                file_name_format="YYYY/MM/PREFIX.YYYYMMDDhhmmss.EXT",
                rename_prefix="data",
                rename_extension="dat",
            )

            # Verify directory structure was created
            year_dir = os.path.join(output_dir, "2009")
            month_dir = os.path.join(year_dir, "11")
            assert os.path.exists(month_dir), f"Directory {month_dir} was not created"

            # Check files in the directory
            files = os.listdir(month_dir)
            assert len(files) > 0, "No files created"

            # Verify that filenames also contain YYYY and MM
            for filename in files:
                # File should start with "data.200911" (prefix + YYYY + MM)
                assert filename.startswith(
                    "data.200911"
                ), f"File {filename} doesn't have repeated YYYY/MM in name"
                assert filename.endswith(
                    ".dat"
                ), f"File {filename} doesn't have .dat extension"
                # Verify full pattern: data.YYYYMMDDhhmmss.dat
                # Should be like: data.20091130220000.dat
                parts = filename.split(".")
                assert (
                    len(parts) == 3
                ), f"Filename {filename} doesn't have expected structure"
                assert parts[0] == "data", "Prefix not correct"
                assert (
                    len(parts[1]) == 14
                ), f"Timestamp part {parts[1]} should be 14 chars (YYYYMMDDhhmmss)"
                assert parts[1].startswith(
                    "200911"
                ), f"Timestamp {parts[1]} should start with 200911 (YYYYMM)"

            print(
                f"✓ Successfully created files with repeated template parts: "
                f"{len(files)} files in 2009/11/ with YYYY/MM in filenames"
            )

        finally:
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)
