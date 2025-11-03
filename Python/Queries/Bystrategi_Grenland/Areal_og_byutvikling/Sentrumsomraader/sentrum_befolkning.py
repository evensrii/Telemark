"""
Script to combine annual population grid datasets from GitHub.
Downloads CSV files ending with YYYY.csv, combines them into long format,
and uploads the result back to GitHub.
"""

import pandas as pd
import requests
import sys
import os
import re
from io import BytesIO

# Import GitHub functions
from Helper_scripts.github_functions import get_github_token, upload_github_file

# Script configuration
GITHUB_FOLDER = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Sentrumssoner/250x250/Befolkning_250"
OUTPUT_FILENAME = "befolkning_rutenett_250.csv"
SCRIPT_NAME = os.path.basename(__file__)

def list_github_folder_contents(folder_path, token):
    """
    List all files in a GitHub folder using the GitHub API.
    
    Args:
        folder_path (str): Path to the folder in the GitHub repository
        token (str): GitHub authentication token
        
    Returns:
        list: List of file information dictionaries
    """
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{folder_path}?ref=main"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to list folder contents: {response.status_code}")
        return []


def download_csv_from_github(file_path, token):
    """
    Download a CSV file from GitHub.
    
    Args:
        file_path (str): Path to the file in the GitHub repository
        token (str): GitHub authentication token
        
    Returns:
        pd.DataFrame: DataFrame containing the CSV data, or None if download fails
    """
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{file_path}?ref=main"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3.raw"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # Read CSV with semicolon separator
        df = pd.read_csv(BytesIO(response.content), sep=';')
        return df
    else:
        print(f"Failed to download {file_path}: {response.status_code}")
        return None


def combine_annual_datasets():
    """
    Main function to combine annual population grid datasets.
    Downloads files from GitHub, combines them, and uploads the result.
    """
    print(f"Starting script: {SCRIPT_NAME}")
    print(f"Looking for CSV files in GitHub folder: {GITHUB_FOLDER}\n")
    
    # Get GitHub token
    github_token = get_github_token()
    
    # List all files in the GitHub folder
    files = list_github_folder_contents(GITHUB_FOLDER, github_token)
    
    if not files:
        print("No files found in the GitHub folder!")
        return
    
    # Filter for CSV files matching the pattern *YYYY.csv
    pattern = r'(\d{4})\.csv$'
    csv_files = []
    
    for file in files:
        if file['type'] == 'file':
            match = re.search(pattern, file['name'])
            if match:
                year = match.group(1)
                csv_files.append({
                    'name': file['name'],
                    'path': file['path'],
                    'year': year
                })
                print(f"Found file: {file['name']} -> Year: {year}")
    
    if not csv_files:
        print("No matching CSV files found!")
        return
    
    # Sort by year
    csv_files.sort(key=lambda x: x['year'])
    
    print(f"\nProcessing {len(csv_files)} files...\n")
    
    # Download and combine all files
    combined_data = []
    
    for file_info in csv_files:
        print(f"Downloading and processing: {file_info['name']}")
        
        # Download CSV from GitHub
        df = download_csv_from_github(file_info['path'], github_token)
        
        if df is None:
            print(f"  - Skipping {file_info['name']} due to download error")
            continue
        
        # Debug: Print actual columns
        print(f"  - Columns found: {list(df.columns)}")
        
        # Rename columns to match output format
        # Expected input columns: ogc_fid, SSBID0250M, pop_tot
        df = df.rename(columns={
            'SSBID0250M': 'grid_id',
            'pop_tot': 'population'
        })
        
        # Check if rename worked, otherwise try alternative column names
        if 'grid_id' not in df.columns:
            # Try finding the grid ID column by looking for common patterns
            for col in df.columns:
                if 'ssbid' in col.lower() or 'grid' in col.lower():
                    df = df.rename(columns={col: 'grid_id'})
                    break
        
        if 'population' not in df.columns:
            # Try finding the population column
            for col in df.columns:
                if 'pop' in col.lower() or 'befolkning' in col.lower():
                    df = df.rename(columns={col: 'population'})
                    break
        
        # Add year column
        df['year'] = file_info['year']
        
        # Select and reorder columns: grid_id, year, population
        df = df[['grid_id', 'year', 'population']]
        
        # Ensure correct data types
        df['grid_id'] = df['grid_id'].astype(str)
        df['year'] = df['year'].astype(str)
        df['population'] = df['population'].astype('float64')
        
        combined_data.append(df)
        print(f"  - Added {len(df)} rows for year {file_info['year']}")
    
    if not combined_data:
        print("\nNo data was successfully processed!")
        return
    
    # Concatenate all dataframes
    final_df = pd.concat(combined_data, ignore_index=True)
    
    print(f"\nCombined dataset summary:")
    print(f"Total rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print(f"Years covered: {sorted(final_df['year'].unique())}")
    print(f"Data types:\n{final_df.dtypes}")
    print(f"\nFirst few rows:")
    print(final_df.head(10))
    
    # Save to temporary file
    temp_folder = os.path.join(os.path.dirname(__file__), 'temp')
    os.makedirs(temp_folder, exist_ok=True)
    temp_file_path = os.path.join(temp_folder, OUTPUT_FILENAME)
    
    final_df.to_csv(temp_file_path, index=False, encoding='utf-8')
    print(f"\nSaved temporary file: {temp_file_path}")
    
    # Upload to GitHub
    github_file_path = f"{GITHUB_FOLDER}/{OUTPUT_FILENAME}"
    print(f"\nUploading to GitHub: {github_file_path}")
    
    upload_github_file(
        local_file_path=temp_file_path,
        github_file_path=github_file_path,
        message=f"Updated {OUTPUT_FILENAME} - Combined {len(csv_files)} annual datasets"
    )
    
    # Clean up temporary file
    try:
        os.remove(temp_file_path)
        print(f"Deleted temporary file: {temp_file_path}")
    except Exception as e:
        print(f"Error deleting temporary file: {e}")
    
    print("\nScript completed successfully!")
    return final_df


if __name__ == "__main__":
    df = combine_annual_datasets()
