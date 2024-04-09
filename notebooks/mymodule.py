import os
import pandas as pd
from janitor import clean_names

def trim_to_three_cd10(df, column_names):
    for col_name in column_names:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype(str).str.slice(0,3)
        else:
            print(f"Column '{col_name}' not found in the dataframe")

def remove_pattern(x):
    if isinstance(x, str) and x.startswith("b'") and x.endswith("'"):
        return x[2:-1]
    return x

def create_csv_file(path_to_directory, file_name, sas_file_path, chunksize=50000):
    """
    Creates a CSV file from a SAS file.

    Args:
    path_to_directory (str): Path to the directory where the CSV file will be created.
    file_name (str): Name of the CSV file to be created.
    sas_file_path (str): Path to the input SAS file.
    chunksize (int, optional): Number of rows per chunk to read at a time. Defaults to 50000.
    """

    # Construct the full file path
    file_path = os.path.join(path_to_directory, file_name)

    #clean the data from b''
    chunk = chunk.applymap(remove_pattern)

    # Read SAS file in chunks and write to CSV
    chunks = pd.read_sas(sas_file_path, chunksize=chunksize)
    for i, chunk in enumerate(chunks):
        chunk = clean_names(chunk)

        # Trim strings in specified columns
        column_names = ['dx1','dx2','dx3','dx4','dx5','dx6','dx7','dx8','dx9','dx10','dx11','dx12','dx13','dx14','dx15']
        if column_names:
            trim_to_three_cd10(chunk, column_names)
        # Write each chunk to CSV. Use mode='a' to append and include header only for the first chunk
        chunk.to_csv(file_path, mode='a', index=False, header=i == 0)





def convert_columns_to_integers(df, columns, nan_placeholder=0):
    for column in columns:
        if col in df.columns:
            df[col].fillna(nan_placeholder).astype(int)
        else:
            print(f"Column '{col}' not found in dataframe")
    return df
