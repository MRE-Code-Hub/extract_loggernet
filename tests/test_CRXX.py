# import filecmp
# import sys
# import os
# import pytest
# from extract_loggernet import extract_loggernet
# # Change the directory to the top level directory
# # of the extract_loggernet package
# os.chdir(os.path.dirname(extract_loggernet.__file__))

# input_path = './test_files/CR23'
# input_file = '3-GndRadData.dat'
# cdl_type = 'CR23'

# output_dir = os.path.join(input_path, 'out')
# expected_dir = os.path.join(input_path, 'expected')


# def test_correct_dir():
#     assert os.getcwd() == "/Users/rasm841/extract_loggernet"

# # remove saved file_position data
# saved_file_pos_path = os.path.join(input_path, '.extract_loggernet_file_position.yaml')
# if os.path.exists(saved_file_pos_path):
#     os.remove(saved_file_pos_path)

# # remove any files in the /out directory
# try:
#     for filename in os.listdir(output_dir):
#         os.remove(os.path.join(output_dir, filename))
# except FileNotFoundError:
#     os.mkdir(output_dir)

# # run the script on the test_file
# extract_loggernet.process_file(input_path=input_path, input_file=input_file, cdl_type=cdl_type, output_dir=output_dir, split_interval="HOURLY", file_name_format="PREFIX.YYYYMMDDhhmmss.EXT")

# out_filenames = os.listdir(output_dir)
# expected_filenames = os.listdir(expected_dir)


# def test_correct_number_of_files_created_for_CR23():
#     expected = len([f for f in expected_filenames if f.endswith('.dat')])
#     observed = len([f for f in out_filenames if f.endswith('.dat')])
#     assert expected == observed


# def test_files_named_correctly_for_CR23():
#     assert out_filenames == expected_filenames


# def test_files_have_correct_content_for_CR23():
#     for i, name in enumerate(expected_filenames):
#         expected_file = os.path.join(expected_dir, name)
#         out_file = os.path.join(output_dir, name)
#         # compare contents of files
#         if not filecmp.cmp(expected_file, out_file, shallow=False):
#             assert False
#     assert True
