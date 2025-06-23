import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from dotenv import load_dotenv
import io
import sys

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

# Function to query the Elhub API for solar production data
def query_elhub(date):
    url = f"https://api.elhub.no/energy-data/v0/municipalities?dataset=PRODUCTION_PER_GROUP_MBA_HOUR&startDate={date}"
    
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
        # Convert the data to a DataFrame
        df = pd.DataFrame(data)
        
        # Print DataFrame columns for debugging
        print(f"{get_timestamp()} DataFrame columns: {df.columns.tolist()}")
        
        # Filter for solar production data
        df = df[df["productionGroup"].str.lower() == "solar"]
        
        # Convert timestamps to datetime
        df["startTime"] = pd.to_datetime(df["startTime"])
        df["endTime"] = pd.to_datetime(df["endTime"])
        df["lastUpdatedTime"] = pd.to_datetime(df["lastUpdatedTime"])
        
        return df
        
    except Exception as e:
        print(f"{get_timestamp()} Error processing data: {str(e)}")
        return pd.DataFrame()

# Function to process the data
def process_data(df):
    if df.empty:
        return df

    # Rename columns for clarity
    df = df.rename(columns={
        "startTime": "Tid",
        "endTime": "Tid_slutt",
        "priceArea": "Prisomrade",
        "productionGroup": "Gruppe",
        "quantityKwh": "Produksjon_kWh",
        "lastUpdatedTime": "Sist_oppdatert"
    })

    # Sort the data
    df = df.sort_values(["Tid", "Prisomrade"])
    
    # Add year and month columns for easier analysis
    df["År"] = df["Tid"].dt.year
    df["Måned"] = df["Tid"].dt.month
    
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

# Function to save data by year to GitHub
def save_data_by_year_to_github(df):
    if df.empty:
        return

    # Group data by year
    df["Year"] = df["Tid"].dt.year
    years = df["Year"].unique()

    for year in years:
        year_data = df[df["Year"] == year].copy()
        del year_data["Year"]

        # Sort data
        year_data = year_data.sort_values(["Tid", "Kommune"])

        # Convert to CSV
        csv_content = year_data.to_csv(index=False)

        # Upload to GitHub
        file_path = f"{github_folder}/solkraft_{year}.csv"
        upload_github_file(file_path, csv_content, f"Update solar production data for {year}")

def main():
    print(f"{get_timestamp()} Starting solar production data update")
    
    # Set specific date for June 2025
    start_date = datetime(2025, 6, 1)
    print(f"{get_timestamp()} Fetching data for {start_date.strftime('%Y-%m')}")
    
    # Query data for June 2025
    df = query_elhub(start_date.strftime("%Y-%m-%d"))
    
    if not df.empty:
        # Process the data
        df = process_data(df)
        
        # Convert to CSV
        csv_content = df.to_csv(index=False)
        
        # Upload to GitHub
        file_path = f"{github_folder}/solkraft_{start_date.strftime('%Y-%m')}.csv"
        upload_github_file(file_path, csv_content, f"Update solar production data for {start_date.strftime('%Y-%m')}")
        
        print(f"{get_timestamp()} Successfully updated solar production data for {start_date.strftime('%Y-%m')}")
    else:
        print(f"{get_timestamp()} No data available for {start_date.strftime('%Y-%m')}")
    print(f"{get_timestamp()} Completed solar production data update")

if __name__ == "__main__":
    main()