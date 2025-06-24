import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import os
from base64 import b64encode
from dateutil.relativedelta import relativedelta

# List of Telemark municipalities
TELEMARK_MUNICIPALITIES = [
    '4001', '4003', '4005', '4010', '4012', '4014', '4016', '4018',
    '4020', '4022', '4024', '4026', '4028', '4030', '4032', '4034', '4036'
]

from Helper_scripts.github_functions import handle_output_data

# Get the GITHUB_TOKEN from the token.env file
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    raise ValueError("PYTHONPATH environment variable is not set.")

# Construct the full path to the token.env
env_file_path = os.path.join(pythonpath, "token.env")
if not os.path.exists(env_file_path):
    raise ValueError(f"token.env file not found in: {env_file_path}")

# Load the .env file
load_dotenv(env_file_path)
print(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} Loaded .env file from: {env_file_path}")

# Get the GITHUB_TOKEN from the environment
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

print(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} GITHUB_TOKEN loaded successfully.")

# Add PYTHONPATH to sys.path
PYTHON_PATH = os.environ.get("PYTHONPATH")
if PYTHON_PATH not in sys.path:
    sys.path.append(PYTHON_PATH)

# Script parameters
task_name = "Klima og energi - Solkraftproduksjon (Elhub)"
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub/Solkraft"
temp_folder = os.environ.get("TEMP_FOLDER")

# GitHub Repository information
REPO = "evensrii/Telemark"
BRANCH = "main"
GITHUB_API_URL = "https://api.github.com"
DATA_PATH = github_folder + "/"

# Function to get current timestamp
def get_timestamp():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

# Function to download a file from GitHub
def download_github_file(file_path):
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{file_path}?ref={BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.text
    elif response.status_code == 404:
        print(f"{get_timestamp()} File not found: {file_path}")
        return None
    else:
        print(
            f"{get_timestamp()} Failed to download file: {file_path}, Status Code: {response.status_code}"
        )
        return None

# Function to upload a file to GitHub
def upload_github_file(file_path, content, message="Updating data"):
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.get(url, headers=headers)
    sha = response.json().get("sha") if response.status_code == 200 else None

    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha

    response = requests.put(url, json=payload, headers=headers)

    if response.status_code in [201, 200]:
        print(f"{get_timestamp()} File uploaded successfully: {file_path}")
    else:
        print(f"{get_timestamp()} Failed to upload file: {response.json()}")

def query_elhub_municipality(municipality_id, start_date, end_date):
    """Query Elhub API for a specific municipality and date range"""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    url = f"https://api.elhub.no/energy-data/v0/municipalities/{municipality_id}?dataset=INSTALLED_CAPACITY_PER_METERING_POINT_TYPE_GROUP_MUNICIPALITY_DAILY&startDate={start_str}&endDate={end_str}"
    
    print(f"{get_timestamp()} Querying data for municipality {municipality_id} from {start_str} to {end_str}")
    
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"{get_timestamp()} Error querying municipality {municipality_id}: {response.text}")
        return pd.DataFrame()
    
    try:
        data = response.json()
        if not data or 'data' not in data:
            return pd.DataFrame()
            
        muni_data = data['data']
        if not isinstance(muni_data, dict) or 'attributes' not in muni_data:
            return pd.DataFrame()
            
        daily_data = muni_data['attributes'].get('installedCapacityPerMeteringPointTypeGroupMunicipalityDaily', [])
        if not daily_data:
            return pd.DataFrame()
            
        records = []
        for daily in daily_data:
            record = {
                'Kommunenummer': muni_data['attributes'].get('municipalityNumber'),
                'Kommune': muni_data['attributes'].get('name'),
                'Kilde': daily.get('productionGroup'),
                'InstallertKapasitet': daily.get('installedCapacity'),
                'Dato': daily.get('usageDateId'),
                'SistOppdatert': data.get('meta', {}).get('lastUpdated')
            }
            records.append(record)
            
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"{get_timestamp()} Error processing data for municipality {municipality_id}: {str(e)}")
        return pd.DataFrame()
    
    # Print the URL being used
    print(f"{get_timestamp()} Using URL: {url}")
    
    # Add headers for API request
    headers = {
        "Accept": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    print(f"{get_timestamp()} API Response status code: {response.status_code}")
    
    # Print the actual response content for debugging
    print(f"{get_timestamp()} API Response content: {response.text[:500]}...")
    
    if response.status_code != 200:
        print(f"{get_timestamp()} Error from API: {response.text}")
        return pd.DataFrame()
    
    try:
        data = response.json()
    except Exception as e:
        print(f"{get_timestamp()} Error parsing JSON response: {str(e)}")
        return pd.DataFrame()
    
    if not data:
        print(f"{get_timestamp()} No data available for {date}")
        return pd.DataFrame()
    
    try:
        # Print raw data structure for debugging
        print(f"{get_timestamp()} First municipality data sample:")
        if data.get('data') and len(data['data']) > 0:
            first_muni = data['data'][0]
            print(f"Municipality: {first_muni}")
            if 'attributes' in first_muni:
                daily_data = first_muni['attributes'].get('installedCapacityPerMeteringPointTypeGroupMunicipalityDaily', [])
                if daily_data:
                    print(f"First daily data: {daily_data[0]}")

        # Extract municipality data from nested structure
        municipalities_data = []
        for municipality in data.get('data', []):
            muni_attrs = municipality.get('attributes', {})
            daily_data = muni_attrs.get('installedCapacityPerMeteringPointTypeGroupMunicipalityDaily', [])
            
            for daily in daily_data:
                record = {
                    'Kommunenummer': muni_attrs.get('municipalityNumber'),
                    'Kommune': muni_attrs.get('name'),
                    'Kilde': daily.get('productionGroup'),  # Fixed field name
                    'InstallertKapasitet': daily.get('installedCapacity'),
                    'Dato': daily.get('usageDateId'),
                    'SistOppdatert': data.get('meta', {}).get('lastUpdated')
                }
                municipalities_data.append(record)
        
        # Convert to DataFrame
        df = pd.DataFrame(municipalities_data)
        
        if df.empty:
            print(f"{get_timestamp()} No data extracted from the response")
            return df
            
        # Print DataFrame info for debugging
        print(f"{get_timestamp()} DataFrame columns: {df.columns.tolist()}")
        print(f"{get_timestamp()} Total row count: {len(df)}")
        
        # Show sample of the data
        print(f"{get_timestamp()} First few rows of data:")
        pd.set_option('display.max_columns', None)
        print(df.head())
        
        # Show detailed analysis of energy sources
        print(f"{get_timestamp()} Analyse av energikilder:")
        kilde_counts = df['Kilde'].value_counts()
        print("\nAntall per energikilde:")
        print(kilde_counts.to_string())
        
        print("\nUnike energikilder:")
        for kilde in sorted(df['Kilde'].unique()):
            print(f"- {kilde}")
        
        # Filter for municipalities in Vestfold og Telemark (40XX)
        print(f"{get_timestamp()} Filtrerer for kommuner i Vestfold og Telemark...")
        df = df[df['Kommunenummer'].astype(str).str.match('^40\d{2}$')]
        
        if df.empty:
            print(f"{get_timestamp()} Ingen data funnet for Vestfold og Telemark")
            return df
        
        # Convert dates
        df['Dato'] = pd.to_datetime(df['Dato'].astype(str), format='%Y%m%d')
        
        # Convert date fields
        df["SistOppdatert"] = pd.to_datetime(df["SistOppdatert"])
        
        # Ensure numeric columns are properly typed
        df["InstallertKapasitet"] = pd.to_numeric(df["InstallertKapasitet"], errors="coerce")
        
        # Show summary of the data
        print(f"{get_timestamp()} Datasammendrag:")
        print(f"Antall kommuner i Vestfold og Telemark: {df['Kommunenummer'].nunique()}")
        print(f"Datoperiode: {df['Dato'].min().strftime('%Y-%m-%d')} til {df['Dato'].max().strftime('%Y-%m-%d')}")
        
        # Calculate and show capacity by energy source for the latest date
        latest_date = df['Dato'].max()
        latest_df = df[df['Dato'] == latest_date]
        
        print(f"\nInstallert kapasitet per energikilde (per {latest_date.strftime('%Y-%m-%d')}):")
        capacity_by_source = latest_df.groupby('Kilde')['InstallertKapasitet'].sum().sort_values(ascending=False)
        for kilde, kapasitet in capacity_by_source.items():
            print(f"{kilde:<10}: {kapasitet:>10,.1f} kW")
        
        print(f"\nTotal installert kapasitet: {latest_df['InstallertKapasitet'].sum():,.1f} kW")
        
        return df
        
    except Exception as e:
        print(f"{get_timestamp()} Error processing data: {str(e)}")
        return pd.DataFrame()

# Function to process the data
def get_latest_date():
    """Get the latest date from the existing CSV file on GitHub"""
    try:
        url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{BRANCH}/{github_folder}/installed_capacity.csv"
        df = pd.read_csv(url)
        if not df.empty:
            df['Dato'] = pd.to_datetime(df['Dato'])
            return df['Dato'].max()
    except Exception as e:
        print(f"{get_timestamp()} Error reading existing data: {str(e)}")
    return None

def process_data(df):
    if df.empty:
        return df
    
    # Convert energy source names to Norwegian
    kilde_map = {
        'hydro': 'Vann',
        'solar': 'Sol',
        'wind': 'Vind',
        'other': 'Annet',
        'thermal': 'Termisk'
    }
    df['Kilde'] = df['Kilde'].map(kilde_map)
    
    # Convert date fields
    df['Dato'] = pd.to_datetime(df['Dato'].astype(str), format='%Y%m%d')
    df['SistOppdatert'] = pd.to_datetime(df['SistOppdatert']).dt.strftime('%Y-%m-%d')
    df['Dato'] = df['Dato'].dt.strftime('%Y-%m-%d')

    # Sort the data
    df = df.sort_values(["Dato", "Kommunenummer"])
    return df

# Function to determine the latest date from GitHub files
def get_latest_date_from_github():
    latest_date = datetime(2023, 1, 1)  # Start from 2023 as requested
    
    # List files in the GitHub directory
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{github_folder}?ref={BRANCH}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        files = [f["name"] for f in response.json() if f["name"].endswith(".csv")]
        for file in files:
            year = int(file.split("_")[1].split(".")[0])
            content = download_github_file(f"{github_folder}/{file}")
            if content:
                df = pd.read_csv(io.StringIO(content))
                if not df.empty and "Tid" in df.columns:
                    df["Tid"] = pd.to_datetime(df["Tid"])
                    file_latest = df["Tid"].max()
                    if file_latest > latest_date:
                        latest_date = file_latest
    
    return latest_date

# Function to save data to GitHub
def save_data_to_github(df):
    if df.empty:
        return
    
    # Sort data by date and municipality
    df = df.sort_values(["Dato", "Kommunenummer"])
    
    # Convert to CSV without extra newlines
    csv_content = df.to_csv(index=False, line_terminator='\n')
    
    filename = "installed_capacity.csv"
    
    # Save to GitHub
    github_path = f"{github_folder}/{filename}"
    upload_github_file(github_path, csv_content, f"Update solar installed capacity data")
    
    # Save to Temp folder
    temp_folder = os.environ.get("TEMP_FOLDER")
    temp_path = os.path.join(temp_folder, filename)
    with open(temp_path, 'w', encoding='utf-8', newline='') as f:
        f.write(csv_content)
    print(f"{get_timestamp()} Saved copy to {temp_path}")

def main():
    print(f"{get_timestamp()} Starting solar installed capacity data update")
    
    # For testing, start from January 2025
    start_date = datetime(2025, 1, 1)
    end_date = start_date + relativedelta(months=1) - timedelta(days=1)
    
    all_data = []
    
    try:
        # Query data for each municipality
        for municipality_id in TELEMARK_MUNICIPALITIES:
            # Query one month of data
            df = query_elhub_municipality(municipality_id, start_date, end_date)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            # Combine all data
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Process the data
            combined_df = process_data(combined_df)
            
            # Sort by date and municipality
            combined_df = combined_df.sort_values(['Dato', 'Kommunenummer'])
            
            # Save processed data
            save_data_to_github(combined_df)
            
            print(f"{get_timestamp()} Successfully updated solar installed capacity data")
        else:
            print(f"{get_timestamp()} No data available")
            
    except Exception as e:
        print(f"{get_timestamp()} Error updating solar installed capacity data: {str(e)}")
        raise
    
    print(f"{get_timestamp()} Completed solar installed capacity data update")

if __name__ == "__main__":
    main()