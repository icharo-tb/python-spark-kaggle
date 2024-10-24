import os
import re
import shutil

def find_csv_files(directory):
    # Regex pattern to match CSV files
    pattern = re.compile(r'.*\.csv$', re.IGNORECASE)

    # List to store found CSV files
    csv_files = []

    # Walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if pattern.match(file):
                csv_files.append(os.path.join(root, file))

    return csv_files

def move_csv_files(source_directory, destination_directory):
    # Ensure the destination directory exists
    os.makedirs(destination_directory, exist_ok=True)

    # Regex pattern to match CSV files
    pattern = re.compile(r'.*\.csv$', re.IGNORECASE)

    # List to store moved CSV files
    moved_files = []

    # Walk through the source directory
    for root, _, files in os.walk(source_directory):
        for file in files:
            if pattern.match(file):
                # Construct full file paths
                source_file_path = os.path.join(root, file)
                destination_file_path = os.path.join(destination_directory, file)

                # Move the file
                shutil.move(source_file_path, destination_file_path)
                moved_files.append(destination_file_path)

    return moved_files