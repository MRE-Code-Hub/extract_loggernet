import filecmp
import sys
import os
import shutil
import pytest
import re
from extract_loggernet import extract_loggernet
# Change the directory to the top level directory
# of the extract_loggernet package
os.chdir(os.path.dirname(extract_loggernet.__file__))

def test_correct_dir():
    assert os.getcwd() == "/Users/rasm841/extract_loggernet"



@pytest.mark.parametrize(
    "input_path, input_file, cdl_type",
    [
        ('/Users/rasm841/extract_loggernet/test_files/CR3000', '1-CR3000_Table213.dat', 'CR3000'),
        ('/Users/rasm841/extract_loggernet/test_files/CR3000', '2-CR3000_Table213.dat', 'CR3000'),
        ('/Users/rasm841/extract_loggernet/test_files/CR3000', '3-CR3000_Table213.dat', 'CR3000'),
        ('/Users/rasm841/extract_loggernet/test_files/CR3000', '4-CR3000_Table213.dat', 'CR3000'),

        ('/Users/rasm841/extract_loggernet/test_files/CR23', '1-GndRadData.dat', 'CR23'),
        ('/Users/rasm841/extract_loggernet/test_files/CR23', '2-GndRadData.dat', 'CR23'),
        ('/Users/rasm841/extract_loggernet/test_files/CR23', '3-GndRadData.dat', 'CR23'),
        ('/Users/rasm841/extract_loggernet/test_files/CR23', '4-GndRadData.dat', 'CR23'),
        ('/Users/rasm841/extract_loggernet/test_files/CR23', '5-GndRadData.dat', 'CR23'),
        ('/Users/rasm841/extract_loggernet/test_files/CR23', '6-GndRadData.dat', 'CR23'),
    ],
)
class TestGroup:

    @pytest.fixture
    def output_dir(self, input_path):
        return os.path.join(input_path, 'out')

    @pytest.fixture
    def expected_dir(self, input_path, input_file):
        return os.path.join(input_path, 'expected', input_file)

    @pytest.fixture()
    def fixt(self, input_path, input_file, cdl_type, output_dir, expected_dir):
        print(f'running fixture {input_file}')

        # remove the number from the file name
        original_input_file_name = input_file
        input_file = re.search(r"\d-(.*)", input_file).groups()[0]
        os.rename(os.path.join(input_path, original_input_file_name), os.path.join(input_path, input_file))

        # remove saved file_position data
        saved_file_pos_path = os.path.join(input_path, '.extract_loggernet_file_position.yaml')
        if os.path.exists(saved_file_pos_path):
            os.remove(saved_file_pos_path)

        # remove any files in the /out directory
        try:
            for filename in os.listdir(output_dir):
                os.remove(os.path.join(output_dir, filename))
        except FileNotFoundError:
            os.mkdir(output_dir)

        # run the script on the test_file
        extract_loggernet.process_file(input_path=input_path, input_file=input_file, cdl_type=cdl_type, output_dir=output_dir, split_interval="HOURLY", file_name_format="PREFIX.YYYYMMDDhhmmss.EXT")

        out_filenames = sorted([f for f in os.listdir(output_dir) if f.endswith('.dat')])
        expected_filenames = sorted([f for f in os.listdir(expected_dir) if f.endswith('.dat')])

        # return value and run test
        yield (out_filenames, expected_filenames)

        # cleanup (restore input_file name)
        os.rename(os.path.join(input_path, input_file), os.path.join(input_path, original_input_file_name))



    def test_correct_number_of_files_created(self, fixt):
        out_filenames, expected_filenames = fixt
        expected = len([f for f in expected_filenames if f.endswith('.dat')])
        observed = len([f for f in out_filenames if f.endswith('.dat')])
        assert expected == observed


    def test_files_named_correctly(self, fixt):
        out_filenames, expected_filenames = fixt
        assert out_filenames == expected_filenames


    def test_files_have_correct_content(self, output_dir, expected_dir, fixt):
        out_filenames, expected_filenames = fixt
        for i, name in enumerate(expected_filenames):
            expected_file = os.path.join(expected_dir, name)
            out_file = os.path.join(output_dir, name)
            # compare contents of files
            if not filecmp.cmp(expected_file, out_file, shallow=False):
                assert False
        assert True

    # def test_saves_file_position_and_picks_up_where_it_left_off(self, input_path, input_file, output_dir, expected_dir, fixt):
    #     # Add new data to the input file
    #     part1 = ""
    #     part2 = ""


    #     with open()

    #     # run the script again


    #     # test output

    # def test_files_combined_together_have_same_contents_as_original_for_CR3000(self, input_path, input_file, output_dir, fixt):
    #     out_filenames, expected_filenames = fixt
    #     data = ""
    #     for file in out_filenames:
    #         path = os.path.join(output_dir, file)
    #         with open(path, 'r') as f:
    #             data += f.read()

    #     with open(os.path.join(output_dir, 'recombined.dat'), 'w') as f:
    #         f.write(data)

    #     original = ""
    #     with open(os.path.join(input_path, input_file)) as f:
    #         original = f.read()

    #     assert data == original