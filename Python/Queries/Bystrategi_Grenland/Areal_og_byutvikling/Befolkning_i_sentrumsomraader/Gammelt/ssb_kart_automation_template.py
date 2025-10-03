"""
Template for automating SSB kart.ssb.no data downloads
Replace the URL and parameters based on your network analysis
"""

import requests
import pandas as pd
import os
import time
from pathlib import Path

# Add the Helper_scripts directory to the path for imports
import sys
sys.path.append(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Python\Helper_scripts')

from github_functions import handle_output_data

def download_ssb_kart_data(attributes=None, format_type="Csv", language="nb", srid="32633"):
    """
    Download 2024 data from SSB kart using the discovered API endpoint
    
    Parameters:
    - attributes: List of attributes to include (e.g., ["ogc_fid", "ssbid250m", "pop_tot"])
    - format_type: Export format ("Csv", "Json", etc.)
    - language: Language code ("nb", "en")
    - srid: Spatial reference system ID
    """
    
    # API endpoint discovered from network analysis
    export_url = "https://kart.ssb.no/api/core/v1/export/file"
    
    # Default attributes if none provided
    if attributes is None:
        attributes = ["ogc_fid", "ssbid250m", "pop_tot"]
    
    # Dataset ID for 2024 data
    dataset_id = "067d56b0-f529-7f1a-8000-432a2077faee"
    
    # Request payload based on network analysis
    payload = {
        "dataset": dataset_id,
        "format": format_type,
        "attributes": attributes,
        "language": language,
        "srid": srid
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'no,en;q=0.9',
        'Content-Type': 'application/json',
        'Referer': 'https://kart.ssb.no/',
        'Origin': 'https://kart.ssb.no'
    }
    
    try:
        # Step 1: Request export (POST to export/file)
        print("Requesting data export...")
        response = requests.post(export_url, json=payload, headers=headers)
        response.raise_for_status()
        
        export_info = response.json()
        print(f"Export requested. ID: {export_info.get('id')}")
        
        # Step 2: Wait for file to be ready and download
        download_url = export_info.get('url')
        if not download_url:
            print("No download URL received")
            return None
            
        print(f"Waiting for file to be ready...")
        
        # Poll the download URL until file is ready (max 60 seconds)
        max_attempts = 12
        wait_time = 5
        
        for attempt in range(max_attempts):
            try:
                print(f"Attempt {attempt + 1}/{max_attempts}: Checking file availability...")
                download_response = requests.get(download_url, headers=headers)
                
                if download_response.status_code == 200:
                    print("File is ready for download!")
                    break
                elif download_response.status_code == 404:
                    if attempt < max_attempts - 1:
                        print(f"File not ready yet, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        print("File did not become available within timeout period")
                        return None
                else:
                    download_response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_attempts - 1:
                    print(f"Request failed, retrying in {wait_time} seconds... ({e})")
                    time.sleep(wait_time)
                else:
                    print(f"Failed to download after {max_attempts} attempts: {e}")
                    return None
        
        # Step 3: Parse the CSV data
        from io import StringIO
        df = pd.read_csv(StringIO(download_response.text))
        
        # Ensure numeric columns are float64 for github_functions compatibility
        for col in df.select_dtypes(include=['number']).columns:
            df[col] = df[col].astype('float64')
        
        print(f"Downloaded data with shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def main():
    """
    Main function to download and process SSB kart data
    """
    
    # Download 2024 data
    attributes = ["ogc_fid", "ssbid250m", "pop_tot"]
    
    df = download_ssb_kart_data(
        attributes=attributes,
        format_type="Csv",
        language="nb",
        srid="32633"
    )
    
    if df is not None:
        # Define output path - same folder as script
        script_folder = Path(__file__).parent
        filename = 'ssb_kart_data.csv'
        output_path = script_folder / filename
        
        # Save CSV directly to script folder
        df.to_csv(output_path, index=False)
        print(f"Data saved to: {output_path}")
        
        # Optional: Also use standard workflow if you want GitHub integration
        # handle_output_data(
        #     df=df,
        #     folder_path=script_folder,
        #     filename=filename,
        #     github_folder='Python/Andre kilder/SSB'
        # )
    else:
        print("Failed to download data")

if __name__ == "__main__":
    main()
