from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import sys
import argparse
import pydicom
import polars as pl
from tqdm.auto import tqdm
import json
import warnings

warnings.filterwarnings("ignore", module="pydicom")

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
        return dicom_data
    except Exception as e:
        print(f"Error processing file {filepath}: {str(e)}")
        return {"DicomPath": filepath}

def process_folder(folder):
    filepaths = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.dcm')]
    chunk_size = 6 
    chunks = [filepaths[i:i + chunk_size] for i in range(0, len(filepaths), chunk_size)]
    all_results = []

    with ProcessPoolExecutor(max_workers=6) as executor:
        future_to_chunk = {executor.submit(process_files_chunk, chunk): chunk for chunk in chunks}
        for future in as_completed(future_to_chunk):
            all_results.extend(future.result())

    return all_results

def process_files_chunk(filepaths):
    return [process_file(filepath) for filepath in filepaths]

def collect_dicom_data(root_dir):
    dcm_folders = []
    for subdir, dirs, files in tqdm(os.walk(root_dir), desc='Scanning directories'):
        if any(file.endswith('.dcm') for file in files):
            dcm_folders.append(subdir)
    all_data = []

    for folder in tqdm(dcm_folders, desc="Processing folders"):
        folder_results = process_folder(folder)
        all_data.extend(folder_results)

    return pl.DataFrame(all_data)

def main():
    parser = argparse.ArgumentParser(description="Process DICOM files and save data to CSV.")
    parser.add_argument("path", help="Path to the root directory containing DICOM files")
    parser.add_argument("-o", "--output", help="Output CSV file path", default="dicom_data.csv")

    args = parser.parse_args()
    if not os.path.isdir(args.path):
        print(f"The provided path '{args.path}' is not a directory or does not exist.", file=sys.stderr)
        sys.exit(1)

    print("Collecting DICOM metadata...")
    df = collect_dicom_data(args.path)
    
    print(f"Saving collected data to {args.output}...")
    df.write_csv(args.output)
    
    print("Done.")

if __name__ == "__main__":
    main()
