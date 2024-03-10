import os
import sys
import argparse
import pydicom
import polars as pl
from tqdm.auto import tqdm
import concurrent.futures

def get_tag_value(dicom_data, tag, default=None):
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
            "Modality": get_tag_value(dicom_data, (0x0008, 0x0060)),
            "ImageType": get_tag_value(dicom_data, (0x0008, 0x0008)),
            "ImageModality": get_tag_value(dicom_data, (0x0008, 0x0061)),
            "PatientID": get_tag_value(dicom_data, (0x0010, 0x0020)),
            "PatientsName": get_tag_value(dicom_data, (0x0010, 0x0010)),
            "PatientsBirthDate": get_tag_value(dicom_data, (0x0010, 0x0030)),
            "PatientSex": get_tag_value(dicom_data, (0x0010, 0x0040)),
            "StudyDate": get_tag_value(dicom_data, (0x0008, 0x0020)),
            "SeriesDate": get_tag_value(dicom_data, (0x0008, 0x0021)),
            "AcquisitionDate": get_tag_value(dicom_data, (0x0008, 0x0022)),
            "SeriesTime": get_tag_value(dicom_data, (0x0008, 0x0031)),
            "AcquisitionTime": get_tag_value(dicom_data, (0x0008, 0x0032)),
            "InstanceCreationDate": get_tag_value(dicom_data, (0x0008, 0x0012)),
            "InstanceCreationTime": get_tag_value(dicom_data, (0x0008, 0x0013)),
            "SequenceName": get_tag_value(dicom_data, (0x0018, 0x0024)),
            "ScanningSequence": get_tag_value(dicom_data, (0x0018, 0x0020)),
            "MRAcquisitionType": get_tag_value(dicom_data, (0x0018, 0x0023)),
            "AcquisitionType": get_tag_value(dicom_data, (0x0018, 0x9302)),
            "SeriesDescription": get_tag_value(dicom_data, (0x0008, 0x103E)),
            "StudyInstanceUID": get_tag_value(dicom_data, (0x0020, 0x000D)),
            "Manufacturer": get_tag_value(dicom_data, (0x0008, 0x0070)),
            "ManufacturersModelName": get_tag_value(dicom_data, (0x0008, 0x1090)),
            "SOPClassUID": get_tag_value(dicom_data, (0x0008, 0x0016)),
            "SOPInstanceUID": get_tag_value(dicom_data, (0x0008, 0x0018)),
            "SeriesInstanceUID": get_tag_value(dicom_data, (0x0020, 0x000E)),
            "StudyID": get_tag_value(dicom_data, (0x0020, 0x0010)),
            "SeriesNumber": get_tag_value(dicom_data, (0x0020, 0x0011)),
            "AcquisitionNumber": get_tag_value(dicom_data, (0x0020, 0x0012)),
            "InstanceNumber": get_tag_value(dicom_data, (0x0020, 0x0013)),
            "ContrastBolusAgent": get_tag_value(dicom_data, (0x0018, 0x0010)),
            "BodyPartExamined": get_tag_value(dicom_data, (0x0018, 0x0015)),
            "SequenceVariant": get_tag_value(dicom_data, (0x0018, 0x0021)),
            "ScanOptions": get_tag_value(dicom_data, (0x0018, 0x0022)),
            "AngioFlag": get_tag_value(dicom_data, (0x0018, 0x0025)),
            "SliceThickness": get_tag_value(dicom_data, (0x0018, 0x0050)),
            "RepetitionTime": get_tag_value(dicom_data, (0x0018, 0x0080)),
            "EchoTime": get_tag_value(dicom_data, (0x0018, 0x0081)),
            "InversionTime": get_tag_value(dicom_data, (0x0018, 0x0082)),
            "NumberofAverages": get_tag_value(dicom_data, (0x0018, 0x0083)),
            "ImagedNucleus": get_tag_value(dicom_data, (0x0018, 0x0085)),
            "EchoNumbers": get_tag_value(dicom_data, (0x0018, 0x0086)),
            "MagneticFieldStrength": get_tag_value(dicom_data, (0x0018, 0x0087)),
            "SpacingBetweenSlices": get_tag_value(dicom_data, (0x0018, 0x0088)),
            "NumberofPhaseEncodingSteps": get_tag_value(dicom_data, (0x0018, 0x0089)),
            "EchoTrainLength": get_tag_value(dicom_data, (0x0018, 0x0091)),
            "PercentSampling": get_tag_value(dicom_data, (0x0018, 0x0093)),
            "PercentPhaseFieldofView": get_tag_value(dicom_data, (0x0018, 0x0094)),
            "PixelBandwidth": get_tag_value(dicom_data, (0x0018, 0x0095)),
            "SoftwareVersions": get_tag_value(dicom_data, (0x0018, 0x1020)),
            "TemporalResolution": get_tag_value(dicom_data, (0x0018, 0x1063)),
            "ProtocolName": get_tag_value(dicom_data, (0x0018, 0x1030)),
            "ContrastBolusVolume": get_tag_value(dicom_data, (0x0018, 0x1041)),
            "ContrastBolusTotalDose": get_tag_value(dicom_data, (0x0018, 0x1044)),
            "ContrastBolusIngredient": get_tag_value(dicom_data, (0x0018, 0x1048)),
            "ContrastBolusIngredientConcentration": get_tag_value(dicom_data, (0x0018, 0x1049)),
            "Diffusion b-value": get_tag_value(dicom_data, (0x0018, 0x9087)),
            "TransmitCoilName": get_tag_value(dicom_data, (0x0018, 0x1251)),
            "AcquisitionMatrix": get_tag_value(dicom_data, (0x0018, 0x1310)),
            "In-planePhaseEncodingDirection": get_tag_value(dicom_data, (0x0018, 0x1312)),
            "FlipAngle": get_tag_value(dicom_data, (0x0018, 0x1314)),
            "VariableFlipAngleFlag": get_tag_value(dicom_data, (0x0018, 0x1315)),
            "SAR": get_tag_value(dicom_data, (0x0018, 0x1316)),
            "PatientPosition": get_tag_value(dicom_data, (0x0018, 0x5100)),
            "Rows": get_tag_value(dicom_data, (0x0028, 0x0010)),
            "Columns": get_tag_value(dicom_data, (0x0028, 0x0011)),
            "DicomPath": filepath,
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
            "Modality": [],
            "ImageType": [],
            "ImageModality": [],
            "PatientID": [],
            "PatientsName": [],
            "PatientsBirthDate": [],
            "PatientSex": [],
            "StudyDate": [],
            "SeriesDate": [],
            "AcquisitionDate": [],
            "SeriesTime": [],
            "AcquisitionTime": [],
            "InstanceCreationDate": [],
            "InstanceCreationTime": [],
            "SequenceName": [],
            "ScanningSequence": [],
            "MRAcquisitionType": [],
            "AcquisitionType": [],
            "SeriesDescription": [],
            "StudyInstanceUID": [],
            "Manufacturer": [],
            "ManufacturersModelName": [],
            "SOPClassUID": [],
            "SOPInstanceUID": [],
            "SeriesInstanceUID": [],
            "StudyID": [],
            "SeriesNumber": [],
            "AcquisitionNumber": [],
            "InstanceNumber": [],
            "ContrastBolusAgent": [],
            "BodyPartExamined": [],
            "SequenceVariant": [],
            "ScanOptions": [],
            "AngioFlag": [],
            "SliceThickness": [],
            "RepetitionTime": [],
            "EchoTime": [],
            "InversionTime": [],
            "NumberofAverages": [],
            "ImagedNucleus": [],
            "EchoNumbers": [],
            "MagneticFieldStrength": [],
            "SpacingBetweenSlices": [],
            "NumberofPhaseEncodingSteps": [],
            "EchoTrainLength": [],
            "PercentSampling": [],
            "PercentPhaseFieldofView": [],
            "PixelBandwidth": [],
            "SoftwareVersions": [],
            "TemporalResolution": [],
            "ProtocolName": [],
            "ContrastBolusVolume": [],
            "ContrastBolusTotalDose": [],
            "ContrastBolusIngredient": [],
            "ContrastBolusIngredientConcentration": [],
            "Diffusion b-value": [],
            "TransmitCoilName": [],
            "AcquisitionMatrix": [],
            "In-planePhaseEncodingDirection": [],
            "FlipAngle": [],
            "VariableFlipAngleFlag": [],
            "SAR": [],
            "PatientPosition": [],
            "Rows": [],
            "Columns": [],
            "DicomPath": [],
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

