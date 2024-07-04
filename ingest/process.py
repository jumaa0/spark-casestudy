import pandas as pd
from datetime import datetime, timedelta
import subprocess
import io

# Function to read and preprocess data
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