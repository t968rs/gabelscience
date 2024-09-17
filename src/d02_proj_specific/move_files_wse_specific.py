import os
import shutil


class MoveFiles:

    def __init__(self, input_folder, output_folder, sting_tolookfor, string_02, ext):

        self.input_folder = input_folder
        self.output_folder = output_folder
        self.string_tolookfor = sting_tolookfor
        self.string_tolookfor2 = string_02
        self.ext = ext

        self.raster_grid_dictionary = {}

    @staticmethod
    def is_it_integer(n):
        try:
            float(n)
        except ValueError:
            return False
        else:
            return int(float(n))

    def find_matching_files(self):

        raster_grids = {}
        matching_list = os.walk(self.input_folder)
        for root, folders, files in matching_list:
            for file in files:
                if self.string_tolookfor.lower() in file.lower():
                    path = os.path.join(root, os.path.join(root, file))
                    if self.ext in file:
                        outname = file
                        outname = outname.replace("__", "_")
                        print(f'Out Name (prgs): {outname}')
                        path_u = path.replace("\\", "_")
                        parts = path_u.split("_")
                        # print(parts)
                        huc_suffix = None
                        if len(parts) > 0:
                            for part in parts:
                                # print(f'Testing: {part}')
                                if 6 > len(part) > 3:
                                    print(f'Part: {part}')
                                    HUC_integer = self.is_it_integer(part)
                                    if HUC_integer:
                                        print("YES")
                                        huc_suffix = HUC_integer
                                        if len(str(huc_suffix)) == 3:
                                            huc_suffix = f"0{huc_suffix}"

                                        print(f'HUC Suffix: {huc_suffix}')
                                        break

                            if huc_suffix and huc_suffix not in file:
                                outname = outname + f"_{huc_suffix}"
                                print(f"Out Name: {outname}")
                                raster_grids[outname] = \
                                    {"in path": path, "out path": os.path.join(self.output_folder, outname + ".tif")}
                            else:
                                raster_grids[outname] = \
                                    {"in path": path, "out path": os.path.join(self.output_folder, outname + ".tif")}
            self.raster_grid_dictionary = raster_grids
        print(self.raster_grid_dictionary)

    def copy_files(self):
        self.find_matching_files()

        for name, info in self.raster_grid_dictionary.items():
            if not os.path.exists(info["out path"]):
                shutil.copy2(info["in path"], info["out path"])
                print(f' Finished {name}')
            else:
                print(f' Skipped {name}. Exists')


if __name__ == "__main__":
    string_list = ["0_2pct", "01pct", "01plus", "02pct", "04pct", "10pct"]
    infolder = r"A:\Iowa_1A\01_data\07060006_Maquoketa"
    outfolder = r"A:\Iowa_1A\01_data\07060006_Maquoketa\repository"
    limiting_string = "wse_"
    extension = ".tif"
    for i, lstring in enumerate(string_list):
        print(f' Moving... {(i + 1) / len(string_list)}')
        initialize = MoveFiles(infolder, outfolder, sting_tolookfor=limiting_string, string_02=lstring,
                               ext=extension)
        initialize.copy_files()
