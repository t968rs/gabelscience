import h5py
import os


def hdf5_walk(hdf5_file):
    infolder, fname = os.path.split(hdf5_file)
    fname = fname.split(".")[0]
    printerpath = os.path.join(infolder, f"{fname}_contents.txt")
    with open(printerpath, 'w') as printer:
        def visit_func(name, node):
            if isinstance(node, h5py.Group):
                printer.write(f"Group: {name}\n\n")
            elif isinstance(node, h5py.Dataset):
                printer.write(f"Dataset: {name}\n")

        with h5py.File(hdf5_file, 'r') as file:
            file.visititems(visit_func)


if __name__ == "__main__":
    hdf_path = r"E:\Iowa_1A\01_data\07060002\0706000204\0706000204.p10.hdf"
    hdf5_walk(hdf_path)
