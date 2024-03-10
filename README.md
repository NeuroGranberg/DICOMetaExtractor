# DICOM Metadata Extraction Tool

This Python script, `DICOMetaExtractor_v32.py`, is designed to efficiently extract metadata from DICOM files across a directory structure, leveraging advanced data processing libraries to handle large datasets effectively. The tool outputs the extracted metadata into a CSV file, providing a comprehensive overview of the DICOM files processed.

## Prerequisites

- Python 3.7 or newer
- pip (Python package installer)

## Installation

Before running the script, ensure you have the required libraries installed. The primary libraries used in this version are `pydicom`, `polars`, `tqdm`, `pandas`, and `portalocker`. You can install these libraries using pip:

```sh
pip install pydicom polars tqdm pandas portalocker
```

Ensure all dependencies are installed successfully before proceeding to use the script.

## Usage

The `get_dicom_metadata_v10.py` script is designed to be used from the command line interface (CLI). The basic usage pattern is outlined below:

```sh
python DICOMetaExtractor_v32.py <path_to_dicom_directory> -o <output_csv_file_path>
```

### Arguments

- `<path_to_dicom_directory>`: This is the path to the root directory containing your DICOM files. The script will recursively search this directory and its subdirectories for DICOM files to process.
- `-o <output_csv_file_path>`: (Optional) Path where the extracted metadata CSV file will be saved. If not specified, the script defaults to `dicom_data.csv` in the current directory.

### Example

To extract metadata from DICOM files located in `/path/to/dicom/files` and save the output to `extracted_metadata.csv` in the current working directory, run:

```sh
python DICOMetaExtractor_v32.py /path/to/dicom/files -o extracted_metadata.csv
```

### Notes

- The script handles large datasets by processing files in parallel and managing memory usage efficiently. It creates temporary files during processing, which are automatically cleaned up upon completion.
- An internet connection is required for the initial installation of dependencies but not for running the script on local DICOM files.

## Customizing Number of Workers

The `get_dicom_metadata_v10.py` script uses parallel processing to improve efficiency, especially when working with large datasets. By default, the script dynamically allocates a certain number of worker processes to optimize performance based on your system's capabilities. However, you might find it necessary to adjust the number of workers manually to better match your system's resources or to optimize the script's performance for your specific dataset.

### Adjusting Worker Processes

To customize the number of worker processes used by the script, you will need to modify the source code slightly. This involves changing the `max_workers` parameter in the `ProcessPoolExecutor` and potentially the `ThreadPoolExecutor`, depending on where you want to adjust the parallelism.

1. **Open the script** in your preferred text editor or Integrated Development Environment (IDE).

2. **Find the `ProcessPoolExecutor` instantiation**. Look for the following line in the `collect_and_process_dicom_data` function:

   ```python
   with ProcessPoolExecutor(max_workers=12) as executor:
   ```

3. **Modify the `max_workers` parameter** to reflect the number of worker processes you wish to use. For example, to use 8 workers, change the line to:

   ```python
   with ProcessPoolExecutor(max_workers=8) as executor:
   ```

4. **(Optional) Adjust ThreadPoolExecutor**: If you also wish to change the number of threads used for directory scanning, find the `ThreadPoolExecutor` instantiation in the `find_dcm_folders` function and adjust the `max_workers` parameter similarly.

   ```python
   with ThreadPoolExecutor(max_workers=4) as executor:
   ```

5. **Save your changes** and close the file.

### Guidelines for Choosing the Number of Workers

- **CPU Resources**: The optimal number of worker processes usually correlates with the number of CPU cores available on your system. Setting `max_workers` to the number of cores or logical processors can maximize your CPU usage.
  
- **Memory Constraints**: Be mindful of your system's memory (RAM). Increasing the number of workers increases memory usage. Monitor your system's memory usage and adjust the number of workers to prevent exhausting system resources.

- **Disk I/O**: For disk-bound tasks, such as reading DICOM files from a slow disk, increasing the number of workers might not lead to performance improvements. In such cases, disk speed is the limiting factor.

- **Trial and Error**: Finding the optimal setting may require some experimentation. Start with a number close to your system's CPU core count and adjust based on observed performance and system resource usage.

After adjusting the number of workers, run the script as usual to process your DICOM files with the new configuration. This customization allows you to tailor the script's performance to your specific system and dataset characteristics, optimizing efficiency and resource utilization.

## Contributing

Contributions to enhance the script or address issues are welcome.

## License

This script is released under the MIT License. Please refer to the `LICENSE` file for more details.

## Acknowledgments

This tool leverages several open-source libraries, and we are grateful to the maintainers and contributors of these projects:

- [Pydicom](https://github.com/pydicom/pydicom) for DICOM file handling.
- [Polars](https://github.com/pola-rs/polars) for efficient data processing.
- [Pandas](https://github.com/pandas-dev/pandas) for data manipulation.
- [Tqdm](https://github.com/tqdm/tqdm) for progress bar functionality.
- [Portalocker](https://github.com/WoLpH/portalocker) for file locking.

For any questions or issues, please open an issue on the GitHub repository page.
