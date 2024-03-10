import os
import sys
import argparse
import pydicom
import polars as pl
from tqdm.auto import tqdm

def verify_dicom_structure_enhanced(root_dir):
    verified_folders = []
    multi_subject_folders = []
    
    for subdir, dirs, files in os.walk(root_dir):
        if not dirs:  # If this directory has no subdirectories, we consider it a lowest-level folder
            dcm_files = [f for f in files if f.endswith('.dcm')]
            if not dcm_files:
                continue  # Skip folders with no DICOM files
            
            patient_ids = set()
            for dcm_file in dcm_files:
                try:
                    filepath = os.path.join(subdir, dcm_file)
                    dicom_data = pydicom.dcmread(filepath)
                    patient_ids.add(dicom_data.PatientID)
                except Exception:
                    continue  # Skip files that can't be read or don't have a PatientID
                
            if len(patient_ids) == 1:
                verified_folders.append((subdir, True))
            else:
                multi_subject_folders.append(subdir)
    
    return {
        "verified_folders": verified_folders,
        "multi_subject_folders": multi_subject_folders
    }

def collect_dicom_data(root_dir):
    verification_results = verify_dicom_structure_enhanced(root_dir)
    verified_folders = verification_results['verified_folders']
    valid_folders = [folder for folder, is_valid in verified_folders if is_valid]
    multi_subject_folders = verification_results['multi_subject_folders']

    data = {
        "PatientID": [],
        "PatientSex": [],
        "StudyDate": [],
        "AcquisitionDate": [],
        "SequenceName": [],
        "SeriesDescription": [],
        "StudyInstanceUID": [],
        "ScanningSequence": []
    }

    def get_tag_value(dicom_data, tag, default="unavailable"):
        if tag in dicom_data:
            return dicom_data[tag].value
        return default

    for folder in tqdm(valid_folders, desc="Processing folders"):
        for filename in os.listdir(folder):
            if filename.endswith('.dcm'):
                filepath = os.path.join(folder, filename)
                dicom_data = pydicom.dcmread(filepath)
                
                data["PatientID"].append(get_tag_value(dicom_data, "PatientID"))
                data["PatientSex"].append(get_tag_value(dicom_data, "PatientSex"))
                data["StudyDate"].append(get_tag_value(dicom_data, "StudyDate"))
                data["AcquisitionDate"].append(get_tag_value(dicom_data, "AcquisitionDate"))
                data["SequenceName"].append(get_tag_value(dicom_data, (0x0018, 0x0024)))
                data["SeriesDescription"].append(get_tag_value(dicom_data, (0x0008, 0x103E)))
                data["StudyInstanceUID"].append(get_tag_value(dicom_data, "StudyInstanceUID"))
                data["ScanningSequence"].append(get_tag_value(dicom_data, "ScanningSequence"))
                break  # Assuming we collect data from the first file that meets criteria per folder
    if multi_subject_folders:
        print("Folders with multiple subjects (skipped):", multi_subject_folders)

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
