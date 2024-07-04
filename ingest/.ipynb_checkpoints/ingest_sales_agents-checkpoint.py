# Import necessary functions from process module
from process import read_and_preprocess_data, create_hdfs_path, write_dataframe_to_hdfs

# Define the main function to execute the workflow
def main():
    # Define file path and base HDFS path
    file_path = "/data/casestudy/group2/sales_agents_SS_raw_2.csv"
    base_hdfs_path = '/casestudy'
    
    # Read and preprocess data
    sales_agents_df = read_and_preprocess_data(file_path)
    
    # Create dynamic HDFS path
    hdfs_path = create_hdfs_path(base_hdfs_path, "sales_agents.csv")
    
    # Write DataFrame to HDFS
    write_dataframe_to_hdfs(sales_agents_df, hdfs_path)

# Execute the main function
if __name__ == "__main__":
    main()
