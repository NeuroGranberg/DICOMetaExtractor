from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm
import polars as pl
import pandas as pd
import uuid
import os
import sys
import argparse
import pydicom
import json
import shutil
import warnings
import portalocker

warnings.filterwarnings("ignore", module="pydicom")


def dicom_tag_to_dict(dicom_file_path):
    dcm_dict = pydicom._dicom_dict.DicomDictionary
    dicom_file = pydicom.dcmread(
        dicom_file_path, specific_tags=list(dcm_dict.keys())[:-14]
    )
    results_tag_dict = {}

    def process_element(element, prefix=""):
        if isinstance(element, pydicom.DataElement) and not isinstance(
            element.value, pydicom.sequence.Sequence
        ):
            if not (
                element.tag.group >> 8 == 0x60 and (element.tag.group & 0xFF) % 2 == 0
            ) and not element.tag.group in (0x7FE0, 0xFFFA, 0xFFFC, 0xFFFE):
                tag_key = f"{prefix}{element.name} {element.tag}"
                results_tag_dict[tag_key] = element.value
        elif isinstance(element, pydicom.Dataset) or isinstance(
            element.value, pydicom.sequence.Sequence
        ):
            prefix = (
                f"{prefix}{element.name} {element.tag} - "
                if isinstance(element, pydicom.DataElement)
                else prefix
            )
            for sub_element in (
                element if isinstance(element, pydicom.Dataset) else element.value
            ):
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
            return (
                json.dumps(value)
                if isinstance(value, (list, dict, set))
                else str(value)
            )
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
    filepaths = [
        os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".dcm")
    ]
    chunk_size = 6
    chunks = [
        filepaths[i : i + chunk_size] for i in range(0, len(filepaths), chunk_size)
    ]
    all_results = []
    with ProcessPoolExecutor(max_workers=6) as executor:
        future_to_chunk = {
            executor.submit(process_files_chunk, chunk): chunk for chunk in chunks
        }
        for future in as_completed(future_to_chunk):
            all_results.extend(future.result())

    return all_results


def process_files_chunk(filepaths):
    return [process_file(filepath) for filepath in filepaths]


def create_temp_dir(output_path):
    base_dir = os.path.dirname(output_path)
    temp_dir_name = "temp_processing"
    temp_dir_path = os.path.join(base_dir, temp_dir_name)
    os.makedirs(temp_dir_path, exist_ok=True)
    return temp_dir_path


def save_partial_results(folder_results, temp_dir, index):
    unique_id = uuid.uuid4()
    partial_output = os.path.join(temp_dir, f"part_{index}_{unique_id}.json")
    with open(partial_output, "w", encoding="utf-8") as file:
        json.dump(folder_results, file, ensure_ascii=False, indent=4)

def replace_with_none(value):
    if value in ["", "N/A", "None", "NONE", None]:
        return None
    else:
        return value

def merge_partial_results_and_cleanup(temp_dir, output_path):
    print(
        f"\nMergging all partial results into {output_path} and cleaning up temporary files..."
    )
    partial_files = [
        os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".json")
    ]
    if not partial_files:
        print("No DICOM files were processed. No CSV file will be generated.")
        shutil.rmtree(temp_dir)
        return

    all_data = []
    for json_file in partial_files:
        try:
            with open(json_file, "r", encoding="utf-8") as file:
                data = json.load(file)
                all_data.extend(data)
        except Exception as e:
            print(f"Failed to load {json_file}: {e}")

    final_df = pl.from_pandas(pd.DataFrame(all_data))
    final_df = final_df.with_columns(pl.all().map_elements(replace_with_none))
    final_df = final_df[
        [s.name for s in final_df if not (s.null_count() == final_df.height)]
    ]
    final_df.write_csv(output_path)
    shutil.rmtree(temp_dir)


def load_processed_folders(base_dir):
    try:
        with open(f"{base_dir}/processed_folders.json", "r") as file:
            portalocker.lock(file, portalocker.LOCK_SH)
            data = json.load(file)
            portalocker.unlock(file)
            return set(data)
    except FileNotFoundError:
        return set()
    except json.decoder.JSONDecodeError:
        return set()


def mark_folder_as_processed(folder, output_path):
    base_dir = os.path.dirname(output_path)
    processed = load_processed_folders(base_dir)
    processed.add(folder)
    with open(f"{base_dir}/processed_folders.json", "w") as file:
        portalocker.lock(file, portalocker.LOCK_EX)
        json.dump(list(processed), file)
        portalocker.unlock(file)

def check_dcm_files(directory):
    """Check if a directory contains any .dcm files."""
    for subdir, _, files in os.walk(directory):
        if any(fname.endswith(".dcm") for fname in files):
            return subdir
    return None

def find_dcm_folders(root_dir):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(check_dcm_files, os.path.join(root_dir, subdir)): subdir
            for subdir, _, _ in os.walk(root_dir)
        }
        dcm_folders = []
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Checking directories"
        ):
            result = future.result()
            if result:
                dcm_folders.append(result)
    dcm_folders = list(set(dcm_folders))
    dcm_folders.sort()
    return dcm_folders


def process_folder_and_save_results(folder, temp_dir, processed_folders, output_path):
    if folder not in processed_folders:
        folder_results = process_folder(folder)
        save_partial_results(folder_results, temp_dir, len(os.listdir(temp_dir)))
        mark_folder_as_processed(folder, output_path)


def collect_and_process_dicom_data(root_dir, output_path):
    base_dir = os.path.dirname(output_path)
    temp_dir = create_temp_dir(output_path)
    dcm_folders = find_dcm_folders(root_dir)
    processed_folders = load_processed_folders(base_dir)
    with ProcessPoolExecutor(max_workers=12) as executor:
        futures = [
            executor.submit(
                process_folder_and_save_results,
                folder,
                temp_dir,
                processed_folders,
                output_path,
            )
            for folder in dcm_folders
        ]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing folders"
        ):
            folder_processed = future.result()
    merge_partial_results_and_cleanup(temp_dir, output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Process DICOM files and save data to CSV."
    )
    parser.add_argument(
        "path", help="Path to the root directory containing DICOM files"
    )
    parser.add_argument(
        "-o", "--output", help="Output CSV file path", default="dicom_data.csv"
    )

    args = parser.parse_args()
    if not os.path.isdir(args.path):
        print(
            f"The provided path '{args.path}' is not a directory or does not exist.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Collecting DICOM metadata...")
    collect_and_process_dicom_data(args.path, args.output)

    base_dir = os.path.dirname(args.output)
    os.remove(f"{base_dir}/processed_folders.json")
    print("Done.")


if __name__ == "__main__":
    main()
