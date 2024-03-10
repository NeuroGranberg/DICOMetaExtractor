import os
import sys
import argparse
import pydicom
import polars as pl
from tqdm.auto import tqdm


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
    }

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

    for folder in tqdm(dcm_folders, desc="Processing folders"):
        for filename in os.listdir(folder):
            if filename.endswith('.dcm'):
                filepath = os.path.join(folder, filename)
                dicom_data = pydicom.dcmread(filepath)
                data["PatientID"].append(get_tag_value(dicom_data, (0x0010, 0x0020)))
                data["PatientSex"].append(get_tag_value(dicom_data, (0x0010, 0x0040)))
                data["StudyDate"].append(get_tag_value(dicom_data, (0x0008, 0x0020)))
                data["AcquisitionDate"].append(get_tag_value(dicom_data, (0x0008, 0x0022)))
                data["SequenceName"].append(get_tag_value(dicom_data, (0x0018, 0x0024)))
                data["SeriesDescription"].append(get_tag_value(dicom_data, (0x0008, 0x103E)))
                data["StudyInstanceUID"].append(get_tag_value(dicom_data, (0x0020, 0x000D)))
                data["ScanningSequence"].append(get_tag_value(dicom_data, (0x0018, 0x0020)))
                data["Manufacturer"].append(get_tag_value(dicom_data, (0x0008, 0x0070)))
                break  # Assuming we collect data from the first file that meets criteria per folder

    df = pl.DataFrame(data)
    return df

def main():
    """
    should be use like tihs:
    python3 get_metadata.py /path/to/dicom/root/directory -o output.csv
    """
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
