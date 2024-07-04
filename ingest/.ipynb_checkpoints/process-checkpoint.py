import pandas as pd
from datetime import datetime, timedelta
import subprocess
import io
import os


# Function to read and preprocess data
def get_last_directory(base_path):
    """
    Get the last directory in the given base path.
    """
    directories = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    if not directories:
        raise Exception("No directories found in the base path.")
    directories.sort()
    return directories[-1]

def generate_file_name(base_name, directory_name):
    """
    Generate file name based on the last character of the directory name.
    """
    number = directory_name[-1]
    parts = base_name.split('_')
    parts[-1] = f"{number}.csv"
    new_file_name = '_'.join(parts)
    return new_file_name


def read_and_preprocess_data(file_path):
    # Read CSV file into DataFrame
    df = pd.read_csv(file_path)
    
    # Add load date and load source columns
    df["load_date"] = datetime.now().date()
    df["load_source"] = "source1"
    
    return df

# Function to create dynamic HDFS path based on current time
def create_hdfs_path(base_path, file):
    current_time = datetime.now()
    day = current_time.strftime('%j')  # Day of the year (1-365)
    hour = current_time.hour + 1  # Hour of the day (1-24)
    return f'{base_path}/day{day}/hour{hour}/{file}'

# Function to write DataFrame to HDFS
def write_dataframe_to_hdfs(df, hdfs_path):
    # Convert DataFrame to CSV format in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Move to the beginning of the StringIO buffer

    # Create the directory in HDFS
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_path.rsplit('/', 1)[0]], check=True)

    # Use subprocess to pipe the content of the StringIO directly to HDFS
    proc = subprocess.Popen(['hdfs', 'dfs', '-put', '-', hdfs_path], stdin=subprocess.PIPE, universal_newlines=True)
    proc.communicate(csv_buffer.getvalue())

    print(f'DataFrame has been written to HDFS path: {hdfs_path}')