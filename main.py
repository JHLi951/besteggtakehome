import csv
import sys
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
src_path = os.path.join(dir_path, 'src')
sys.path.append(src_path)

from some_storage_library import SomeStorageLibrary

def read_source_columns(file_path):
    # Attempt to take in the column data information and order them
    columns = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                order, name = line.strip().split('|')
                columns[int(order)] = name
    except Exception as e:
       print(f"An error occurred while reading {file_path}: {e}")
       sys.exit(1)
    return [name for _, name in sorted(columns.items())]

def read_source_data(file_path):
    # Attempt to read in and store the raw data
    data = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                data.append(line.strip().split('|'))
    except Exception as e:
        print(f"An error occurred while reading {file_path}: {e}")
        sys.exit(1)
    return data

def write_csv(header, data, file_path):
    # Write our combined data into a new csv file
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(data)

def main():
    source_columns_path = 'data/source/SOURCECOLUMNS.txt'
    source_data_path = 'data/source/SOURCEDATA.txt'
    destination_csv = 'combined_data.csv'

    # Read the order and names of columns
    header = read_source_columns(source_columns_path)

    # Read the raw data
    data = read_source_data(source_data_path)

    # Write to a CSV file
    write_csv(header, data, destination_csv)

    # Move the generated CSV file to the destination directory
    storage = SomeStorageLibrary()
    storage.load_csv(destination_csv)

if __name__ == '__main__':
    print('Beginning the ETL process...')
    main()
