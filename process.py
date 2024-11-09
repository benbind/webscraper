import os
import pandas as pd
import dask.dataframe as dd
def clean_csv_files(input_folder):
    # Loop through each file in the specified folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            
            # Read the CSV file
            try:
                df = pd.read_csv(file_path)
                
                # Drop columns where all values are empty strings
                df = df.dropna(axis=1, how='all')
                df.fillna('N/A', inplace=True)
                # Save the cleaned DataFrame back to the original file
                df.to_csv(f"processed_data/cleaned_{filename}", index=False)
                print(f"Cleaned and saved: {filename}")
            
            except Exception as e:
                print(f"Error processing {filename}: {e}")


def csv_to_parquet():
    # Specify the input and output directories
    input_directory = f'{os.getcwd()}/processed_csv' 
    output_directory = f'{os.getcwd()}/parquet_data'  
    # Create the output directory if it doesn't exist
    # Get a list of all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]

    # Process each CSV file
    for csv_file in csv_files:
        csv_file_path = os.path.join(input_directory, csv_file)
        # Base name for the output Parquet files
        # parquet_file_base = os.path.join(output_directory, csv_file.replace('.csv', ''))

        # Read the CSV file using Dask
        # Adjust blocksize parameter based on your system's memory capacity
        ddf = dd.read_csv(csv_file_path, blocksize='512MB')  # Reads the CSV in 512MB chunks


        filename_prefix = os.path.splitext(csv_file)[0]

        # Write the Dask DataFrame to Parquet files
        # Dask will partition the data and write multiple Parquet files
        ddf.to_parquet(
            output_directory, 
            write_index=False,
            name_function=lambda i: filename_prefix + f'-part-{i}.parquet',
        )

    print("Conversion completed. Parquet files are saved in:", output_directory)

if __name__ == "__main__":
    # Define the folder containing the unprocessed data
    input_folder = "unprocessed_data"
    
    # Run the cleaning function
    # clean_csv_files(input_folder)
    csv_to_parquet()
