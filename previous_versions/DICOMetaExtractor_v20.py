import os
import json
import sys
import argparse
import pydicom
from tqdm.auto import tqdm
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import dask
import dask.dataframe as dd
import warnings

warnings.filterwarnings("ignore", module="pydicom")
dask.config.set({'dataframe.query-planning': True})


def dicom_tag_to_dict(dicom_file_path):
    dicom_file = pydicom.dcmread(dicom_file_path, stop_before_pixels=True, force=True)
    results_tag_dict = {}

    def process_element(element, prefix=""):
        if isinstance(element, pydicom.DataElement) and not isinstance(element.value, pydicom.sequence.Sequence):
            tag_key = f"{prefix}{element.name} {element.tag}"
            results_tag_dict[tag_key] = element.value
        elif isinstance(element, pydicom.Dataset) or isinstance(element.value, pydicom.sequence.Sequence):
            prefix = f"{prefix}{element.name} {element.tag} - " if isinstance(element, pydicom.DataElement) else prefix
            for sub_element in (element if isinstance(element, pydicom.Dataset) else element.value):
                process_element(sub_element, prefix)

    process_element(dicom_file)
    return results_tag_dict

def stringify_value(value):
    if value is None:
        return "N/A"
    elif isinstance(value, bytes):
        return value.hex()
    else:
        try:
            return json.dumps(value) if isinstance(value, (list, dict, set)) else str(value)
        except Exception as e:
            return f"Error: {str(e)}"

def process_file(filepath):
    try:
        dicom_data = dicom_tag_to_dict(filepath)
        for key, value in dicom_data.items():
            dicom_data[key] = stringify_value(value)
        dicom_data["DicomPath"] = filepath
        return pd.DataFrame([dicom_data])
    except Exception as e:
        print(f"Error processing file {filepath}: {str(e)}")
        return pd.DataFrame([{"DicomPath": filepath, "Error": str(e)}])

def process_folder(folder):
    filepaths = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.dcm')]
    chunk_size = 6

    with ProcessPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(process_files_chunk, filepaths[i:i + chunk_size]) for i in range(0, len(filepaths), chunk_size)]
        all_data = pd.concat([future.result() for future in as_completed(futures)])

    return dd.from_pandas(all_data, npartitions=1)

def process_files_chunk(filepaths):
    return pd.concat([process_file(filepath) for filepath in filepaths])

def collect_dicom_data(root_dir):
    dcm_folders = []
    for subdir, _, _ in tqdm(os.walk(root_dir), desc="Scanning directories"):
        if any(file.endswith('.dcm') for file in os.listdir(subdir)):
            dcm_folders.append(subdir)

    results = []
    for folder in tqdm(dcm_folders, desc="Processing folders"):
        folder_results = process_folder(folder)
        results.append(folder_results)

    if results:
        all_data_dask = dd.concat(results)
    else:
        all_data_dask = dd.from_pandas(pd.DataFrame(), npartitions=1)

    return all_data_dask

def main():
    parser = argparse.ArgumentParser(description="Process DICOM files and save data to CSV.")
    parser.add_argument("path", help="Path to the root directory containing DICOM files")
    parser.add_argument("-o", "--output", help="Output CSV file path", default="dicom_data.csv")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        print(f"The provided path '{args.path}' is not a directory or does not exist.", file=sys.stderr)
        sys.exit(1)

    print("Collecting DICOM metadata...")
    all_data_dask = collect_dicom_data(args.path)

    print(f"Saving collected data to {args.output}...")
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    all_data_dask.to_csv(args.output, index=False, single_file=True)

    print("Done.")

if __name__ == "__main__":
    main()
