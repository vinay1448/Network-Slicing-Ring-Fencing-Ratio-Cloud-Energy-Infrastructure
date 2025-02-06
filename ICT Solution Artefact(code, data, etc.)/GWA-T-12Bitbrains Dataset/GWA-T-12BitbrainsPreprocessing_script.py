import pandas as pd
import numpy as np
import sys
import os
import glob

# Function to preprocess a CSV file with configurable handling of 'inf' values
def preprocess_csv(file_path, handle_inf_method='median'):
    # Read the CSV file with semicolon delimiter, skipping the first row with column names
    df = pd.read_csv(file_path, delimiter=';', header=None)
    
    # Rename columns for clarity
    df.columns = ['Timestamp', 'CPU_cores', 'CPU_provisioned_MHz', 'CPU_usage_MHz', 
                  'CPU_usage_percent', 'Memory_provisioned_KB', 'Memory_usage_KB', 
                  'Disk_read_KBs', 'Disk_write_KBs', 'Net_received_KBs', 'Net_transmitted_KBs']
    
    # Strip any leading/trailing whitespace from the entire DataFrame
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Convert numeric columns to their proper data types
    cols_to_numeric = ['CPU_cores', 'CPU_provisioned_MHz', 'CPU_usage_MHz', 
                       'CPU_usage_percent', 'Memory_provisioned_KB', 'Memory_usage_KB', 
                       'Disk_read_KBs', 'Disk_write_KBs', 'Net_received_KBs', 'Net_transmitted_KBs']
    
    for col in cols_to_numeric:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert 'Timestamp' column to numeric first, then to datetime
    df['Timestamp'] = pd.to_numeric(df['Timestamp'], errors='coerce')
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s', errors='coerce')

    # Ensure that CPU_cores are non-zero
    df['CPU_cores'] = df['CPU_cores'].apply(lambda x: max(x, 1))  # Replace zero cores with 1

    # Ensure no invalid values in CPU_usage_MHz and other critical fields
    df['CPU_usage_MHz'] = df['CPU_usage_MHz'].apply(lambda x: max(x, 0))  # Replace negative values or NaNs with 0

    # Normalize CPU usage percentage: handle cases where CPU_provisioned_MHz is zero to avoid division by zero
    df['CPU_usage_percent'] = np.where(df['CPU_provisioned_MHz'] != 0, 
                                       df['CPU_usage_MHz'] / df['CPU_provisioned_MHz'], 
                                       np.inf)  # Use inf for rows with 0 provisioned MHz

    # Handle infinite values in CPU_usage_percent based on the selected method
    if handle_inf_method == 'median':
        # Replace inf values with the median of valid values
        median_cpu_usage_percent = df['CPU_usage_percent'].replace([np.inf, -np.inf], np.nan).median()
        df['CPU_usage_percent'] = df['CPU_usage_percent'].replace([np.inf, -np.inf], median_cpu_usage_percent)
    elif handle_inf_method == 'max':
        # Replace inf values with the maximum non-inf value
        max_cpu_usage_percent = df['CPU_usage_percent'].replace([np.inf, -np.inf], np.nan).max()
        df['CPU_usage_percent'] = df['CPU_usage_percent'].replace([np.inf, -np.inf], max_cpu_usage_percent)
    elif handle_inf_method == 'large_number':
        # Replace inf with a large finite number (sys.float_info.max)
        df['CPU_usage_percent'] = df['CPU_usage_percent'].replace([np.inf, -np.inf], sys.float_info.max)

    # Handle missing data by forward filling the entire DataFrame
    df = df.ffill()  # Forward fill remaining missing data
    
    return df

def load_csv_files(directory, handle_inf_method='median'):
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return pd.DataFrame()
    
    # Check if the files are present in the directory ending with .csv
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    print(f"Found {len(csv_files)} CSV files in {directory}")
    
    # Load and preprocess each CSV file
    if len(csv_files) == 0:
        return pd.DataFrame()
    
    dataframes = []
    for file in csv_files:
        print(f"Processing file: {file}")
        df = preprocess_csv(file, handle_inf_method=handle_inf_method)
        dataframes.append(df)
        
    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

# Function to aggregate data, keeping CPU_cores as integers
def aggregate_data(df):
    # Ensure 'Timestamp' exists
    if 'Timestamp' not in df.columns:
        print("Error: 'Timestamp' column not found!")
        print(f"Columns found: {df.columns}")
        return None
    
    # Set the 'Timestamp' column as the index
    df.set_index('Timestamp', inplace=True)
    
    # Perform aggregation on all columns except 'CPU_cores'
    aggregation_funcs = {
        'CPU_cores': 'first',  # Keep the first CPU_cores value for each time window
        'CPU_provisioned_MHz': 'mean',
        'CPU_usage_MHz': 'mean',
        'CPU_usage_percent': 'mean',
        'Memory_provisioned_KB': 'mean',
        'Memory_usage_KB': 'mean',
        'Disk_read_KBs': 'mean',
        'Disk_write_KBs': 'mean',
        'Net_received_KBs': 'mean',
        'Net_transmitted_KBs': 'mean'
    }

    # Resample data by hour and apply the aggregation functions
    aggregated_data = df.resample('1H').agg(aggregation_funcs)
    
    return aggregated_data

# Function to save preprocessed data to CSV
def save_preprocessed_data(df, output_path):
    df.to_csv(output_path, index=True)

# Main function to execute the preprocessing
def main():
    # Define paths for fastStorage and rnd directories
    fastStorage_dir = r"C:\Users\vinay\Research Project\fastStorage\2013-8"
    rnd_dirs = [
        r"C:\Users\vinay\Research Project\rnd\2013-7",
        r"C:\Users\vinay\Research Project\rnd\2013-8",
        r"C:\Users\vinay\Research Project\rnd\2013-9"
    ]
    
    # Process fastStorage folder
    print("Processing fastStorage folder...")
    fastStorage_data = load_csv_files(fastStorage_dir, handle_inf_method='median')  # You can change method here
    
    if fastStorage_data.empty:
        print("No data found in fastStorage directory.")

    # Process rnd folders
    print("Processing rnd folders...")
    rnd_data = pd.DataFrame()  # Initialize an empty DataFrame to hold all rnd data
    for rnd_dir in rnd_dirs:
        print(f"Processing {rnd_dir}...")
        rnd_data = pd.concat([rnd_data, load_csv_files(rnd_dir, handle_inf_method='max')], ignore_index=True)  # Example: different method for rnd
    
    if rnd_data.empty:
        print("No data found in rnd directories.")
    
    # Aggregate data
    if not fastStorage_data.empty:
        print("Aggregating fastStorage data...")
        aggregated_fastStorage = aggregate_data(fastStorage_data)
        save_preprocessed_data(aggregated_fastStorage, 'preprocessed_fastStorage.csv')
    
    if not rnd_data.empty:
        print("Aggregating rnd data...")
        aggregated_rnd = aggregate_data(rnd_data)
        save_preprocessed_data(aggregated_rnd, 'preprocessed_rnd.csv')

    print("Preprocessing complete.")

if __name__ == "__main__":
    main()