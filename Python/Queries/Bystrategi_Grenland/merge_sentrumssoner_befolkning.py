"""
Script for merging sentrumssoner data with befolkning data based on ssbid.

This script:
1. Reads sentrumssoner_grenland.csv (list of sentrumssoner)
2. Reads befolkning_i_rutenett.csv (population data for grid cells)
3. Merges them based on the common 'ssbid' column
4. Saves the result as sentrumssoner_med_befolkning.csv

Author: Even Sæther Røed
Date: 2025-09-18
"""

import pandas as pd
import os
import sys

# Add helper scripts to path
sys.path.append(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Helper_scripts')
from github_functions import handle_output_data

def merge_sentrumssoner_befolkning():
    """
    Merge sentrumssoner data with befolkning data based on ssbid column.
    """
    
    # Define file paths
    base_path = r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\Bystrategi_Grenland\Areal_og_byutvikling\Sentrumssoner'
    sentrumssoner_file = os.path.join(base_path, 'sentrumssoner_grenland.csv')
    befolkning_file = os.path.join(base_path, 'befolkning_i_rutenett.csv')
    output_file = 'sentrumssoner_med_befolkning.csv'
    output_path = os.path.join(base_path, output_file)
    
    print("Starting merge process...")
    print(f"Reading sentrumssoner from: {sentrumssoner_file}")
    print(f"Reading befolkning data from: {befolkning_file}")
    
    try:
        # Read sentrumssoner file (semicolon separated)
        print("Loading sentrumssoner data...")
        sentrumssoner_df = pd.read_csv(sentrumssoner_file, sep=';', dtype={'ssbid': str})
        print(f"Loaded {len(sentrumssoner_df)} sentrumssoner records")
        print(f"Sentrumssoner columns: {list(sentrumssoner_df.columns)}")
        
        # Read befolkning file (assuming comma separated based on standard format)
        print("Loading befolkning data...")
        befolkning_df = pd.read_csv(befolkning_file, dtype={'ssbid': str})
        print(f"Loaded {len(befolkning_df)} befolkning records")
        print(f"Befolkning columns: {list(befolkning_df.columns)}")
        
        # Check if ssbid column exists in both files
        if 'ssbid' not in sentrumssoner_df.columns:
            raise ValueError("Column 'ssbid' not found in sentrumssoner file")
        if 'ssbid' not in befolkning_df.columns:
            raise ValueError("Column 'ssbid' not found in befolkning file")
        
        # Perform inner join to get only matching records
        print("Merging data based on ssbid...")
        merged_df = pd.merge(
            sentrumssoner_df, 
            befolkning_df, 
            on='ssbid', 
            how='inner',
            suffixes=('_sentrumssoner', '_befolkning')
        )
        
        print(f"Merged dataset contains {len(merged_df)} records")
        print(f"Final columns: {list(merged_df.columns)}")
        
        # Convert numeric columns to float64 for github_functions compatibility
        numeric_columns = merged_df.select_dtypes(include=['int32', 'int64', 'float32']).columns
        for col in numeric_columns:
            merged_df[col] = merged_df[col].astype('float64')
        
        # Display sample of merged data
        print("\nSample of merged data:")
        print(merged_df.head())
        
        # Use standard data handling workflow
        print(f"\nProcessing output data using handle_output_data...")
        handle_output_data(
            df=merged_df,
            file_name=output_file,
            output_folder=base_path,
            github_folder='Data/Bystrategi_Grenland/Areal_og_byutvikling/Sentrumssoner'
        )
        
        print(f"Process completed successfully!")
        print(f"Output saved to: {output_path}")
        
        return merged_df
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        return None
    except Exception as e:
        print(f"Error during merge process: {e}")
        return None

if __name__ == "__main__":
    result = merge_sentrumssoner_befolkning()
    if result is not None:
        print(f"\nMerge completed successfully with {len(result)} records")
    else:
        print("\nMerge failed - please check error messages above")
