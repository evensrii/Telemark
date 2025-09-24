import pandas as pd
import os
import sys
from pathlib import Path
import glob
from datetime import datetime

# Add the Helper_scripts directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'Helper_scripts'))

from Helper_scripts.github_functions import handle_output_data

def convert_comma_decimals_to_float(df, exclude_columns=None):
    """
    Convert columns with comma decimal separators to float, excluding specified columns.
    
    Args:
        df: pandas DataFrame
        exclude_columns: list of column names to exclude from conversion
    
    Returns:
        pandas DataFrame with converted numeric columns
    """
    if exclude_columns is None:
        exclude_columns = []
    
    df_converted = df.copy()
    
    for col in df.columns:
        if col not in exclude_columns:
            # Check if column contains measurement data (PM10 or NO2)
            if any(keyword in col for keyword in ['PM10', 'NO2']) and 'µg/m³' in col:
                if df[col].dtype == 'object':  # String column
                    # Replace comma with dot for decimal conversion
                    df_converted[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                    df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce')
                    # Use float64 for compatibility with github_functions
                    df_converted[col] = df_converted[col].astype('float64')
    
    return df_converted

def convert_float_to_comma_format(df, numeric_columns):
    """
    Convert float columns back to string format with comma decimal separators.
    
    Args:
        df: pandas DataFrame
        numeric_columns: list of column names that contain numeric data
    
    Returns:
        pandas DataFrame with comma-formatted numeric columns
    """
    df_formatted = df.copy()
    
    for col in numeric_columns:
        if col in df.columns and df[col].dtype in ['float64', 'float32']:
            # Format to match original precision and use comma as decimal separator
            df_formatted[col] = df[col].round(8).astype(str).str.replace('.', ',', regex=False)
    
    return df_formatted

def load_and_combine_luftforurensing_files():
    """
    Load all CSV files from the luftforurensing folder on GitHub repository,
    combine them into a single DataFrame, and sort by Fra-tid.
    
    Returns:
        pandas DataFrame with combined and sorted data
    """
    
    print("=== Loading and combining luftforurensing CSV files ===")
    
    # Define the data folder path
    data_folder = os.path.join(
        os.path.dirname(__file__), 
        '..', '..', '..', '..', 
        'Data', 'Bystrategi_Grenland', 'Klima', 'Luftforurensing'
    )
    data_folder = os.path.abspath(data_folder)
    
    print(f"Looking for CSV files in: {data_folder}")
    
    # Find all CSV files in the folder (excluding the combined output file)
    csv_files = []
    if os.path.exists(data_folder):
        for file in os.listdir(data_folder):
            if (file.endswith('.csv') and 
                file != 'luftforurensing_grenland.csv' and  # Exclude the output file
                not file.endswith('_randomized.csv')):  # Exclude randomized test files
                csv_files.append(os.path.join(data_folder, file))
    
    if not csv_files:
        print("No CSV files found to process.")
        return pd.DataFrame()
    
    print(f"Found {len(csv_files)} CSV files to process:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")
    
    # Load and combine all CSV files
    combined_data = []
    
    for file_path in csv_files:
        try:
            print(f"\nProcessing: {os.path.basename(file_path)}")
            
            # Read CSV with semicolon separator and UTF-8 encoding
            df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
            
            print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
            print(f"  Date range: {df['Fra-tid'].iloc[0]} to {df['Fra-tid'].iloc[-1]}")
            
            # Convert comma decimals to float for processing
            preserve_columns = ['Fra-tid', 'Til-tid']
            preserve_columns.extend([col for col in df.columns if 'Datadekning (%)' in col])
            
            df_converted = convert_comma_decimals_to_float(df, exclude_columns=preserve_columns)
            
            combined_data.append(df_converted)
            
        except Exception as e:
            print(f"  Error processing {file_path}: {str(e)}")
            continue
    
    if not combined_data:
        print("No data could be loaded from CSV files.")
        return pd.DataFrame()
    
    # Combine all DataFrames
    print(f"\nCombining {len(combined_data)} DataFrames...")
    df_combined = pd.concat(combined_data, ignore_index=True)
    
    print(f"Combined data shape: {df_combined.shape}")
    
    # Convert Fra-tid to datetime for proper sorting
    df_combined['Fra-tid_datetime'] = pd.to_datetime(df_combined['Fra-tid'], format='%d.%m.%Y %H:%M')
    
    # Sort by Fra-tid
    df_combined = df_combined.sort_values('Fra-tid_datetime')
    
    # Remove the temporary datetime column
    df_combined = df_combined.drop('Fra-tid_datetime', axis=1)
    
    # Reset index after sorting
    df_combined = df_combined.reset_index(drop=True)
    
    print(f"Final sorted data shape: {df_combined.shape}")
    print(f"Date range: {df_combined['Fra-tid'].iloc[0]} to {df_combined['Fra-tid'].iloc[-1]}")
    
    # Convert numeric columns back to comma format for output
    numeric_columns = []
    for col in df_combined.columns:
        if any(keyword in col for keyword in ['PM10', 'NO2']) and 'µg/m³' in col:
            numeric_columns.append(col)
    
    df_final = convert_float_to_comma_format(df_combined, numeric_columns)
    
    return df_final

def main():
    """
    Main function to process luftforurensing data and handle GitHub operations.
    """
    
    print("=== Luftforurensing Grenland Data Processing ===")
    print(f"Script started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Load and combine all CSV files
        df_combined = load_and_combine_luftforurensing_files()
        
        if df_combined.empty:
            print("No data to process. Exiting.")
            return
        
        print(f"\nProcessed {len(df_combined)} total rows")
        print("Sample of combined data:")
        print(df_combined.head(3))
        
        ##################### Save to CSV, compare and upload to GitHub #####################
        
        file_name = "luftforurensing_grenland.csv"
        task_name = "Bystrategi Grenland - Luftforurensing"
        github_folder = "Data/Bystrategi_Grenland/Klima/Luftforurensing"
        temp_folder = os.environ.get("TEMP_FOLDER")
        
        # Process all files and track their status
        is_new_data = handle_output_data(df_combined, file_name, github_folder, temp_folder, keepcsv=True)
        
        # Write a single status file that indicates if any file has new data
        log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
        task_name_safe = task_name.replace(".", "_").replace(" ", "_")
        new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")
        
        # Write the result in a detailed format - set to "Yes" if any file has new data
        with open(new_data_status_file, "w", encoding="utf-8") as log_file:
            log_file.write(f"{task_name_safe},multiple_files,{'Yes' if is_new_data else 'No'}\n")
        
        # Output results for debugging/testing
        if is_new_data:
            print(f"New data detected in {file_name} and pushed to GitHub.")
        else:
            print(f"No new data detected in {file_name}.")
        
        print(f"New data status log written to {new_data_status_file}")
        
    except Exception as e:
        print(f"Error in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()