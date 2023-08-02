
import csv
import os
import datetime

def chunk_large_csv(input_file_path, output_folder, chunk_size):
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    with open(input_file_path, newline='') as infile:
        reader = csv.reader(infile)

        header = next(reader)  # Read the header

        chunk_count = 1
        current_chunk = []
        for i, row in enumerate(reader, 1):
            current_chunk.append(row)

            if i % chunk_size == 0:
                output_file_path = os.path.join(output_folder, f"output_chunk_{chunk_count}.csv")
                with open(output_file_path, 'w', newline='') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(header)
                    writer.writerows(current_chunk)
                print(f"Chunk {chunk_count} saved to {output_file_path}")

                current_chunk = []
                chunk_count += 1

        # Save the remaining rows (if any) in the last chunk
        if current_chunk:
            output_file_path = os.path.join(output_folder, f"output_chunk_{chunk_count}.csv")
            with open(output_file_path, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)
                writer.writerows(current_chunk)
            print(f"Chunk {chunk_count} saved to {output_file_path}")

if __name__ == "__main__":
    current_time = datetime.datetime.now()
    print ("Time job started:", current_time)
    input_file_path = 'data/input/row_validation_10MB.csv'  # Replace with the path to your large CSV file
    output_folder = "data/output/"  # Replace with the desired output folder path
    chunk_size = 2000000  # Number of rows per chunk
    print("Chunking for", input_file_path)
    chunk_large_csv(input_file_path, output_folder, chunk_size)
    current_time = datetime.datetime.now()
    print ("Time job started:", current_time)