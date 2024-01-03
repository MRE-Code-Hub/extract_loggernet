'''
This will test that extract_loggernet.py is working correctly.
This will run through the test_files, and verify that
extract_loggernet.py is outputing as expected.

Run `pytest` in the terminal to run this test.
Run `pytest -s` to see extra printed output.
'''

import filecmp
import os
import re
import pytest
from extract_loggernet import extract_loggernet
# Change the directory to the top level directory
# of the extract_loggernet package
os.chdir(os.path.dirname(extract_loggernet.__file__))


def test_correct_dir():
    '''
    Verify that code changed the directory to the
    module's top level directory.
    '''
    assert os.getcwd() == "/Users/rasm841/extract_loggernet"


class TestGroup:
    '''
    Runs extract_loggernet.py on a consecutive set of tests
    for each type of Campbell Data Logger and verifies that the
    output matches the expected number of files, name of each
    file, and content of each file. The test files are preceeded
    by a number, which is removed for each test, to create the
    effect of a single growing loggernet file.
    '''

    # Rather than use `pytest.mark.parametrize on the
    # TestGroup class, use a class scoped fixture.
    # This way we can remove redundancy by running
    # the extract loggernet script once for each file,
    # rather than once per test.
    @pytest.fixture(scope="class", params=[
        # (input_path, input_file, cdl_type)
        # ('/Users/rasm841/extract_loggernet/test_files/CR3000', '1-CR3000_Table213.dat', 'CR3000'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR3000', '2-CR3000_Table213.dat', 'CR3000'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR3000', '3-CR3000_Table213.dat', 'CR3000'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR3000', '4-CR3000_Table213.dat', 'CR3000'),

        # ('/Users/rasm841/extract_loggernet/test_files/CR23', '1-GndRadData.dat', 'CR23'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR23', '2-GndRadData.dat', 'CR23'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR23', '3-GndRadData.dat', 'CR23'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR23', '4-GndRadData.dat', 'CR23'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR23', '5-GndRadData.dat', 'CR23'),
        # ('/Users/rasm841/extract_loggernet/test_files/CR23', '6-GndRadData.dat', 'CR23'),

        ('./test_files/PWS_002', '1-CR1000x_PWS_002_IPconnect_Met.dat', 'CR1000X'),
        ('./test_files/PWS_002', '2-CR1000x_PWS_002_IPconnect_Met.dat', 'CR1000X'),
        ('./test_files/PWS_002', '3-CR1000x_PWS_002_IPconnect_Met.dat', 'CR1000X'),
        ('./test_files/PWS_002', '4-CR1000x_PWS_002_IPconnect_Met.dat', 'CR1000X'),

    ])
    def parameters(self, request):
        return request.param

    @pytest.fixture(scope="class")
    def input_path(self, parameters):
        return os.path.abspath(parameters[0])

    @pytest.fixture(scope="class")
    def input_file(self, parameters):
        return parameters[1]

    @pytest.fixture(scope="class")
    def cdl_type(self, parameters):
        return parameters[2]

    @pytest.fixture(scope="class")
    def output_dir(self, input_path):
        return os.path.join(input_path, 'out')

    @pytest.fixture(scope="class")
    def expected_dir(self, input_path, input_file):
        return os.path.join(input_path, 'expected', input_file)

    @pytest.fixture(scope="class")
    def setup(
        self,
        input_path,
        input_file,
        cdl_type,
        output_dir,
        expected_dir
    ):
        print(f'running fixture {input_file}')

        # If this is the first of the tests
        # then clear any output files and saved data
        # to prep for the test.
        if input_file.startswith('1'):
            print(f"Removing out files and saved data for {input_file}")
            # remove saved file_position data
            fn = '.extract_loggernet_file_position.yaml'
            saved_file_pos_path = os.path.join(input_path, fn)
            if os.path.exists(saved_file_pos_path):
                os.remove(saved_file_pos_path)

            # remove any files in the /out directory
            try:
                for filename in os.listdir(output_dir):
                    os.remove(os.path.join(output_dir, filename))
            except FileNotFoundError:
                os.mkdir(output_dir)

        # remove the number from the file name
        original_input_file_name = input_file
        input_file = re.search(r"\d-(.*)", input_file).groups()[0]
        os.rename(
            os.path.join(input_path, original_input_file_name),
            os.path.join(input_path, input_file)
        )

        # run the script on the test_file
        extract_loggernet.process_file(
            input_path=input_path,
            input_file=input_file,
            output_dir=output_dir,
            cdl_type=cdl_type,
            split_interval="HOURLY",
            file_name_format="PREFIX.YYYYMMDDhhmmss.EXT"
        )

        out_filenames = sorted(
            [f for f in os.listdir(output_dir) if f.endswith('.dat')]
        )
        expected_filenames = sorted(
            [f for f in os.listdir(expected_dir) if f.endswith('.dat')]
        )

        # return value and run test
        yield (out_filenames, expected_filenames)

        # ---------- CLEANUP (restore input_file name) -------
        os.rename(
            os.path.join(input_path, input_file),
            os.path.join(input_path, original_input_file_name)
        )

    def test_correct_number_of_files_created(self, setup):
        out_filenames, expected_filenames = setup
        expected = len([f for f in expected_filenames if f.endswith('.dat')])
        observed = len([f for f in out_filenames if f.endswith('.dat')])
        assert expected == observed

    def test_files_named_correctly(self, setup):
        out_filenames, expected_filenames = setup
        assert out_filenames == expected_filenames

    def test_files_have_correct_content(self, output_dir, expected_dir, setup):
        out_filenames, expected_filenames = setup
        for name in expected_filenames:
            expected_file = os.path.join(expected_dir, name)
            out_file = os.path.join(output_dir, name)
            # compare contents of files
            if not filecmp.cmp(expected_file, out_file, shallow=False):
                print(f"{name} is not the same")
                assert False
        assert True
