import os
import pandas as pd
import dask.dataframe as dd
import json

def clean_csv_files(input_folder, output_folder):
    """
    Clean CSV files and save them in the output folder.
    """
    os.makedirs(output_folder, exist_ok=True)
    
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Drop columns where all values are empty strings
                df = df.dropna(axis=1, how='all')
                
                # Replace NaN values with 'N/A'
                df.fillna('N/A', inplace=True)
                
                # Save the cleaned DataFrame
                cleaned_path = os.path.join(output_folder, f"cleaned_{filename}")
                df.to_csv(cleaned_path, index=False)
                print(f"Cleaned and saved: {cleaned_path}")
            
            except Exception as e:
                print(f"Error processing {filename}: {e}")


def csv_to_jsonl(input_folder, jsonl_folder):
    """
    Convert cleaned CSV files to JSONL format.
    """
    os.makedirs(jsonl_folder, exist_ok=True)
    
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            csv_file_path = os.path.join(input_folder, filename)
            jsonl_file_path = os.path.join(jsonl_folder, filename.replace('.csv', '.jsonl'))
            
            try:
                # Read the CSV file
                df = pd.read_csv(csv_file_path)
                
                # Replace NaN values with 'N/A'
                df.fillna('N/A', inplace=True)
                
                # Convert DataFrame to JSONL format
                with open(jsonl_file_path, 'w') as jsonl_file:
                    for record in df.to_dict(orient='records'):
                        json.dump(record, jsonl_file)
                        jsonl_file.write('\n')
                
                print(f"Converted to JSONL: {jsonl_file_path}")
                
                # Print the head of the JSONL file
                print("JSONL head preview:")
                with open(jsonl_file_path, 'r') as f:
                    for _ in range(5):  # Print first 5 lines for preview
                        print(f.readline().strip())
            
            except Exception as e:
                print(f"Error converting {filename} to JSONL: {e}")


def jsonl_to_parquet(jsonl_folder, parquet_folder):
    """
    Convert JSONL files to Parquet format.
    """
    os.makedirs(parquet_folder, exist_ok=True)
    
    for filename in os.listdir(jsonl_folder):
        if filename.endswith(".jsonl"):
            jsonl_file_path = os.path.join(jsonl_folder, filename)
            
            try:
                # Load the JSONL file using Dask
                ddf = dd.read_json(jsonl_file_path, lines=True, blocksize='512MB')
                
                # Define the output Parquet file path
                parquet_file_path = f"parquet_data/"#os.path.join(parquet_folder, filename.replace('.jsonl', '.parquet'))
                
                # Write to Parquet format
                dd.to_parquet(ddf, parquet_file_path, name_function=(lambda x: f"{filename.replace('.jsonl', '.parquet')}"), write_index=False)
                
                print(f"Converted to Parquet: {parquet_file_path}")
            
            except Exception as e:
                print(f"Error converting {filename} to Parquet: {e}")


if __name__ == "__main__":
    # Define input and output directories
    input_folder = "unprocessed_data"
    processed_folder = "processed_csv"
    jsonl_folder = "jsonl_data"
    parquet_folder = "parquet_data"
    
    # Step 1: Clean CSV files
    clean_csv_files(input_folder, processed_folder)
    
    # Step 2: Convert cleaned CSV files to JSONL
    csv_to_jsonl(processed_folder, jsonl_folder)
    
    # Step 3: Convert JSONL files to Parquet
    jsonl_to_parquet(jsonl_folder, parquet_folder)
