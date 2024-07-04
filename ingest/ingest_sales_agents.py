import os
from process import read_and_preprocess_data, create_hdfs_path, write_dataframe_to_hdfs, get_last_directory, generate_file_name

def main():
    # Define base path and HDFS path
    base_path = "/data/casestudy"
    base_hdfs_path = '/casestudy'
    
    # Get the last directory
    last_directory = get_last_directory(base_path)
    
    # Define the file name dynamically
    base_file_name = "sales_agents_SS_raw_2.csv"
    dynamic_file_name = generate_file_name(base_file_name, last_directory)
    file_path = os.path.join(base_path, last_directory, dynamic_file_name)
    
    # Read and preprocess data
    sales_agents_df = read_and_preprocess_data(file_path)
    
    # Create dynamic HDFS path
    hdfs_path = create_hdfs_path(base_hdfs_path, "sales_agents.csv")
    
    # Write DataFrame to HDFS
    write_dataframe_to_hdfs(sales_agents_df, hdfs_path)

# Execute the main function
if __name__ == "__main__":
    main()
