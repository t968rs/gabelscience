import os

directory_list = ["0_2pct", "01pct", "01plus", "02pct", "04pct", "10pct"]


input_folder = r"A:\Iowa_3B\01_data\10170203_Sioux\repo"

for return_string in directory_list:
    fullpath = os.path.join(input_folder, return_string)
    if not os.path.exists(fullpath):
        os.makedirs(fullpath)
    print(f' - Created {return_string}')

print(f'Done.')