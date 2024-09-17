import os
import json

processing_folder = os.path.join(os.getcwd(), "proc_results")
json_file = os.path.join(processing_folder, "processing.json")
avg_file = os.path.join(processing_folder, "proc_avg_per_GB.json")


def accept_path(fnc_name, path, elapased):
    file_size = os.stat(path).st_size * 1E-09
    update_and_average({"Operation": fnc_name,
                        "file_size": file_size, "elapsed": elapased})


def update_and_average(data):
    os.makedirs(processing_folder, exist_ok=True)

    existing_data = []

    if os.path.isfile(json_file):
        with open(json_file) as infile:
            existing_data = json.load(infile)

    existing_data.append(data)
    with open(json_file, 'w') as outfile:
        json.dump(existing_data, outfile)

    # calculate average
    averages = {}
    for operation in set(d['Operation'] for d in existing_data):
        filtered = [d for d in existing_data if d["Operation"] == operation]
        total_elapsed = sum(d["elapsed"] for d in filtered)
        total_file_size = sum(d["file_size"] for d in filtered)

        avg_per_mb = total_elapsed / total_file_size if total_file_size else 0
        averages[operation] = avg_per_mb

    with open(avg_file, 'w') as outfile:
        json.dump(averages, outfile)


def accept_process_info(fnc_name, total_size, elapased):
    update_and_average({"Operation": fnc_name,
                        "file_size": total_size * 1E-09,
                        "elapsed": elapased})
