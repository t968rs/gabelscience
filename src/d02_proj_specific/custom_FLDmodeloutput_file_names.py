import os

exts = [".tif"]


def create_raster_grid_list(folder):
    bad_ext = [".xml", ".ovr", ".vat", ".mapmeta", "vrt"]
    unwanted_grids = ["velocity", "depth"]
    raster_grids = {}
    tiff_list = os.walk(folder)
    target = False
    for root, folders, files in tiff_list:
        for file in files:
            path = os.path.join(root, file)
            # print(f'File: {file}')
            for ext in exts:
                if ext in path.lower():
                    target = True
                    for _ in bad_ext:
                        if _ in path.lower():
                            print(f'Bad: {file}')
                            target = False
                            break
                else:
                    target = False
            if target:
                print(f'Good score: {root}')
                path = os.path.join(root, file)
                base, name = os.path.split(path)
                pbase, parent = os.path.split(base)
                if not parent.isdigit() and ("wse" in file.lower() or "mse" in file.lower()):
                    # print(f'Found: {path}')
                    for _ in unwanted_grids:
                        if _ in path.lower():
                            target = False
                            break
                else:
                    target = False
            if target:
                raster_grids[path] = file
            else:
                print(f'Has unwanted {path}')
    return raster_grids


def batch_rename(file_dict):
    for path, file in file_dict.items():
        base, filename = os.path.split(path)
        upone, parent_folder = os.path.split(base)
        uptwo, pparent = os.path.split(upone)

        new_name = parent_folder.lower()
        if "mse" in new_name:
            new_name = new_name.replace("_mse_", "").replace("nrcs", "")
        new_name = new_name.replace("precip", "pluvial")
        new_name = "wse_" + new_name
        for i in range(5):
            if f"{str(i)}." in new_name:
                #print(f'{new_name}')
                new_name = new_name.replace(f"_{str(i)}.0", f"_0{str(i)}")
                #print(f'New: {new_name}')
                break
            elif f"_{str(i)}_" in new_name:
                new_name = new_name.replace(f"_{str(i)}_", f"_0{str(i)}")
                break
            else:
                pass

        new_name = new_name.replace("_pct", "pct")
        new_name = new_name.replace("pct_plus", "plus")
        new_name = new_name.replace("plus_pct", "plus")
        new_name = new_name.replace("pluspct", "plus")
        # new_name = file.replace(".Terrain", "").replace("_1", "_01").replace("_pct", "pct").replace("_Terrain", "")
        # new_name = new_name.replace("_Max", "")
        new_name = f'{new_name}_{pparent}'
        if ".tif" not in new_name:
            new_name = new_name + ".tif"

        name_pieces = new_name.split("_")
        # print(name_pieces)
        piece_copy = name_pieces.copy()
        for i, piece in enumerate(piece_copy):
            if "plus" in piece:
                if i != 1:
                    # to_replace = piece_copy[1]
                    if "01" not in piece:
                        name_pieces.insert(1, piece)
                    else:
                        name_pieces.insert(1, piece)
        # name_pieces = order
        print(f'\n{name_pieces}')
        new_name = f"{name_pieces[0]}_{name_pieces[2]}_{name_pieces[1]}_{name_pieces[3]}"
        if len(name_pieces) > 4:
            new_name = new_name + f"_{name_pieces[4]}"
            if len(name_pieces) > 5:
                new_name = new_name + f"_{name_pieces[5]}"
        if "plus" in new_name:
            if "fluvial" in path.lower():
                new_name = "wse_01plus" + f'_fluvial_{pparent}.tif'
            else:
                new_name = "wse_01plus" + f'_pluvial_{pparent}.tif'
        new_name = new_name.replace("10.0", "10")
        new_name = new_name.replace("0.2", "0_2")
        # print(f'File: {file}')
        print(f'Old path: {path}')
        # print(f'New Name: {new_name}')

        newpath = os.path.join(upone, new_name)

        if not os.path.exists(newpath):
            print(f'New Path: {newpath}')
            os.rename(path, newpath)
        else:
            print(f'Skipping {file}')


inloc = r'E:\Iowa_3B\01_data\10230002_Floyd\grid_dl'
raster_dict = create_raster_grid_list(folder=inloc)
for k, v in raster_dict.items():
    print(f'Found: {k}')
print(f'\n\n')
batch_rename(file_dict=raster_dict)
