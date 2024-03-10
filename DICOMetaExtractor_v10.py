from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import os
import sys
import argparse
import pydicom
import polars as pl
from tqdm.auto import tqdm
import json
import warnings

# Suppress all warnings from pydicom
warnings.filterwarnings("ignore", module="pydicom")

def dicom_tag_to_dict(dicom_file_path):
    dicom_file = pydicom.dcmread(dicom_file_path, stop_before_pixels=True, force=True)
    results_tag_dict = {}

    def process_element(element, prefix=""):
        if isinstance(element, pydicom.DataElement):
            if not isinstance(element.value, pydicom.sequence.Sequence):
                tag_key = f"{prefix}{element.name} {element.tag}"
                results_tag_dict[tag_key] = element.value
            else:
                for seq_element in element.value:
                    seq_prefix = f"{prefix}{element.name} {element.tag} - "
                    process_sequence(seq_element, seq_prefix)
        elif isinstance(element, pydicom.Dataset):
            for sub_element in element:
                process_element(sub_element, prefix)

    def process_sequence(sequence, prefix):
        if isinstance(sequence, pydicom.Dataset):
            for sub_element in sequence:
                process_element(sub_element, prefix)

    for parent_element in dicom_file:
        process_element(parent_element)

    return results_tag_dict

# Function to convert complex data types to a string representation
def stringify_value(value):
    if value is None:
        return "N/A"
    elif isinstance(value, bytes):
        return value.hex()
    elif isinstance(value, (list, dict, set)):
        return json.dumps(value)
    else:
        try:
            return str(value)
        except Exception as e:
            return f"Error: {str(e)}"

# Updated function to process individual DICOM files
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

# Function to collect DICOM metadata from a directory structure
def collect_dicom_data(root_dir):
    dcm_folders = []
    for subdir, dirs, files in tqdm(os.walk(root_dir), desc='Scanning directories'):
        if any(file.endswith('.dcm') for file in files):
            dcm_folders.append(subdir)

    all_data = []  # Accumulate all processed DICOM metadata

    def process_folder(folder):
        filepaths = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.dcm')]
        with ProcessPoolExecutor(max_workers=64) as executor:
            results = list(executor.map(process_file, filepaths))
        return results

    for folder in tqdm(dcm_folders, desc="Processing folders"):
        folder_results = process_folder(folder)
        all_data.extend(folder_results)  # Collect results from all folders

    # Convert the list of dictionaries to a DataFrame
    df = pl.DataFrame(all_data)

    return df


def main():
    parser = argparse.ArgumentParser(description="Process DICOM files and save data to CSV.")
    parser.add_argument("path", help="Path to the root directory containing DICOM files")
    parser.add_argument("-o", "--output", help="Output CSV file path", default="dicom_data.csv")

    args = parser.parse_args()
    root_dir = args.path
    output_path = args.output

    # Ensure the root directory exists
    if not os.path.isdir(root_dir):
        print(f"The provided path '{root_dir}' is not a directory or does not exist.", file=sys.stderr)
        sys.exit(1)

    print("Collecting DICOM metadata...")
    df = collect_dicom_data(root_dir)
    
    print(f"Saving collected data to {output_path}...")
    df.write_csv(output_path)
    
    print("Done.")

if __name__ == "__main__":
    main()

