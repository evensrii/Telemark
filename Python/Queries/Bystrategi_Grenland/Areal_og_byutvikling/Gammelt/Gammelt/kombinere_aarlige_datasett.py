import pandas as pd
import os
import re
from pathlib import Path
import sys

# Add helper scripts to path
sys.path.append(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Helper_scripts')
from github_functions import handle_output_data

def combine_annual_datasets():
    """
    Combine annual population datasets from the Befolkning_rutenett_250 folder.
    Extracts year from filename and adds as 'År' column.
    """
    
    # Define paths
    input_folder = Path(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\Bystrategi_Grenland\Areal_og_byutvikling\Befolkning_rutenett_250')
    output_folder = Path(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\Bystrategi_Grenland\Areal_og_byutvikling')
    output_filename = 'befolkning_i_rutenett.csv'
    
    print(f"Looking for CSV files in: {input_folder}")
    
    # Find all CSV files matching the pattern
    csv_files = []
    pattern = r'(\d{4}-\d{2}-\d{2})-befolkning_250m_(\d{4})\.csv'
    
    for file in input_folder.glob('*.csv'):
        if file.name != 'readme.txt':  # Skip readme file
            match = re.match(pattern, file.name)
            if match:
                year = match.group(2)
                csv_files.append((file, year))
                print(f"Found file: {file.name} -> Year: {year}")
    
    if not csv_files:
        print("No matching CSV files found!")
        return
    
    # Sort by year
    csv_files.sort(key=lambda x: x[1])
    
    print(f"\nProcessing {len(csv_files)} files...")
    
    # Read and combine all files
    combined_data = []
    
    for file_path, year in csv_files:
        print(f"Processing {file_path.name}...")
        
        # Read CSV with semicolon separator
        df = pd.read_csv(file_path, sep=';')
        
        # Add year column
        df['År'] = int(year)
        
        # Ensure numeric columns are float64 for compatibility with github_functions
        if 'pop_tot' in df.columns:
            df['pop_tot'] = df['pop_tot'].astype('float64')
        
        combined_data.append(df)
        print(f"  - Added {len(df)} rows for year {year}")
    
    # Concatenate all dataframes
    final_df = pd.concat(combined_data, ignore_index=True)
    
    print(f"\nCombined dataset summary:")
    print(f"Total rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print(f"Years covered: {sorted(final_df['År'].unique())}")
    print(f"Data types:\n{final_df.dtypes}")
    
    # Save the combined dataset
    output_path = output_folder / output_filename
    
    print(f"\nSaving combined dataset to: {output_path}")
    
    # Save directly to local file
    final_df.to_csv(output_path, sep=';', index=False)
    
    print("Dataset combination completed successfully!")
    
    return final_df

if __name__ == "__main__":
    df = combine_annual_datasets()