

import os
import pandas as pd
import datetime

def split_csv_file(input_file, output_folder, max_rows=1000):
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Read the CSV file
    reader = pd.read_csv(input_file)

    chunk_count = 0
    rows_count = 0
    chunk_rows = []

    for _, row in reader.iterrows():
        chunk_rows.append(row)
        rows_count += 1

        if rows_count == max_rows:
            # Create the output file path
            #output_file = os.path.join(output_folder, f"chunk_{chunk_count}.csv")

            # Create a DataFrame from the accumulated rows
            chunk_df = pd.DataFrame(chunk_rows, columns=reader.columns)

            # Write the chunk data to the output file
            #chunk_df.to_csv(output_file, index=False)
            #print(f"Chunk {chunk_count}: {output_file} created")

            chunk_count += 1
            rows_count = 0
            chunk_rows = []

    # Write the remaining chunk to a file
    if rows_count > 0:
        #output_file = os.path.join(output_folder, f"chunk_{chunk_count}.csv")
        chunk_df = pd.DataFrame(chunk_rows, columns=reader.columns)
        #chunk_df.to_csv(output_file, index=False)
        #print(f"Chunk {chunk_count}: {output_file} created")

    print("File splitting completed!")


# Example usage
input_file = 'data/input/row_validation_5GB.csv'
output_folder = 'data/output/'

current_time = datetime.datetime.now()

print ("Time job started:", current_time)

print ("Chunk file for ", input_file)
split_csv_file(input_file, output_folder)

current_time = datetime.datetime.now()

print ("Time job Stopped:", current_time)

