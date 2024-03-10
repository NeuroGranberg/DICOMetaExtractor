import os
import sys
import argparse
import pydicom
import polars as pl
from tqdm.auto import tqdm
import concurrent.futures

def get_tag_value(dicom_data, tag, default="unavailable"):
    if tag in dicom_data:
        tag_value = dicom_data[tag].value
        # Check if the value is one of the simple data types (e.g., not a Sequence)
        if isinstance(tag_value, (str, int, float)):
            return tag_value
        else:
            # For other types, convert to string or handle differently as needed
            try:
                return str(tag_value)
            except Exception as e:
                print(f"Error converting tag value to string: {e}")
                return default
    return default

def process_file(filepath):
    try:
        dicom_data = pydicom.dcmread(filepath)
        return {
            "PatientID": get_tag_value(dicom_data, (0x0010, 0x0020)),
            "PatientSex": get_tag_value(dicom_data, (0x0010, 0x0040)),
            "StudyDate": get_tag_value(dicom_data, (0x0008, 0x0020)),
            "AcquisitionDate": get_tag_value(dicom_data, (0x0008, 0x0022)),
            "SequenceName": get_tag_value(dicom_data, (0x0018, 0x0024)),
            "SeriesDescription": get_tag_value(dicom_data, (0x0008, 0x103E)),
            "StudyInstanceUID": get_tag_value(dicom_data, (0x0020, 0x000D)),
            "ScanningSequence": get_tag_value(dicom_data, (0x0018, 0x0020)),
            "Manufacturer": get_tag_value(dicom_data, (0x0008, 0x0070)),
            "Path": filepath
        }
    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
        return {}

def collect_dicom_data(root_dir):
    dcm_folders = []
    for subdir, dirs, files in tqdm(os.walk(root_dir), desc='Scanning directories'):
        if not dirs:  # If this directory has no subdirectories, we consider it a lowest-level folder
            dcm_files = [f for f in files if f.endswith('.dcm')]
            if not dcm_files:
                continue  # Skip folders with no DICOM files
            dcm_folders.append(subdir)

    data = {
        "PatientID": [],
        "PatientSex": [],
        "StudyDate": [],
        "AcquisitionDate": [],
        "SequenceName": [],
        "SeriesDescription": [],
        "StudyInstanceUID": [],
        "ScanningSequence": [],
        "Manufacturer": [],
        "Path": [],
    }

    def process_folder(folder):
        filepaths = [os.path.join(folder, filename) for filename in os.listdir(folder) if filename.endswith('.dcm')]
        with concurrent.futures.ThreadPoolExecutor(max_workers=128) as executor:
            results = list(executor.map(process_file, filepaths))
        return results

    for folder in tqdm(dcm_folders, desc="Processing folders"):
        folder_results = process_folder(folder)
        for result in folder_results:
            if result:
                for key in data:
                    data[key].append(result[key])

    df = pl.DataFrame(data)
    return df

def main():
    parser = argparse.ArgumentParser(description="Process DICOM files and save data to CSV.")
    parser.add_argument("path", help="Path to the root directory containing DICOM files")
    parser.add_argument("-o", "--output", help="Output CSV file path", default="dicom_data.csv")

    args = parser.parse_args()
    root_dir = args.path
    output_path = args.output

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
