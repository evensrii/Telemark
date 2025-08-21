import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from dotenv import load_dotenv
import io
import sys

from Helper_scripts.github_functions import handle_output_data

## Get the GITHUB_TOKEN from the token.env file

# Retrieve PYTHONPATH environment variable
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
task_name = "Klima og energi - Stromforbruk (Elhub)"
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub"
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


# Function to query the Elhub API for a specific date
def query_elhub(date):
    url = f"https://api.elhub.no/energy-data/v0/municipalities?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR&startDate={date}"
    response = requests.get(url)
    data = response.json()
    
    # Debug prints
    print(f"{get_timestamp()} API Response status code: {response.status_code}")
    
    if not data.get("data"):
        print(f"{get_timestamp()} No data available for {date}")
        return pd.DataFrame()  # Return empty DataFrame if no data

    df_data = pd.json_normalize(data["data"])
    
    # Check if consumption data is empty
    if df_data.empty or all(df_data['attributes.consumptionPerGroupMunicipalityHour'].apply(lambda x: not x)):
        print(f"{get_timestamp()} No consumption data available for {date}")
        return pd.DataFrame()
    
    df_data_expanded = df_data.explode("attributes.consumptionPerGroupMunicipalityHour")
    df_consumption = pd.json_normalize(
        df_data_expanded["attributes.consumptionPerGroupMunicipalityHour"]
    )
    
    if df_consumption.empty:
        print(f"{get_timestamp()} No consumption records found for {date}")
        return pd.DataFrame()

    df_data_expanded = df_data_expanded.reset_index(drop=True)
    df_consumption = df_consumption.reset_index(drop=True)

    df_combined = pd.concat(
        [
            df_data_expanded[["attributes.municipalityNumber", "attributes.name"]],
            df_consumption,
        ],
        axis=1,
    )

    # Keep only rows where 'attributes.municipalityNumber' is numeric (first line may contain an asterisk)
    df_combined = df_combined[
        df_combined["attributes.municipalityNumber"].str.isnumeric()
    ]

    df_telemark = df_combined[
        (
            df_combined["attributes.municipalityNumber"].astype(int) > 4000
        )
        & (df_combined["attributes.municipalityNumber"].astype(int) < 4200)
    ]
    
    if df_telemark.empty:
        print(f"{get_timestamp()} No Telemark data found for {date}")
        return pd.DataFrame()
        
    return df_telemark


# Function to remove rows where 'Tid' and 'Gruppe' columns are empty
def clean_existing_data(df):
    return df.dropna(subset=["Tid", "Gruppe"])


# Function to rename columns and process the data
def process_data(df):
    # Remove unnecessary columns
    df = df.drop(columns=["endTime", "lastUpdatedTime"], errors="ignore")

    # Rename columns for clarity
    df = df.rename(
        columns={
            "attributes.municipalityNumber": "Knr",
            "attributes.name": "Kommunenavn",
            "startTime": "Tid",
            "consumptionGroup": "Gruppe",
            "quantityKwh": "Forbruk (kWh)",
            "meteringPointCount": "Antall målere",
        }
    )

    # Ensure proper conversion of 'Tid' to datetime format and drop invalid dates
    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")
    df = df.dropna(subset=["Tid"])

    ## NB: Fjerner tidssoneinfo (i.e. +01:00:00) i dette scriptet. Sørger for å rette opp dette i PowerQuery, slik at 31.12.2020 23:00:00 blir 01.01.2021 00:00:00 osv.

    # Standardize the 'Gruppe' values
    df["Gruppe"] = df["Gruppe"].replace(
        {
            "business": "Næring (unntatt industri)",
            "industry": "Industri",
            "private": "Husholdninger",
        }
    )

    # Sort by 'Tid' (oldest dates first), then by 'Kommunenavn' and 'Gruppe'
    df = df.sort_values(by=["Tid", "Kommunenavn", "Gruppe"]).reset_index(drop=True)

    return df


# Function to determine the latest date across all yearly CSV files on GitHub
def get_latest_date_from_github():
    latest_date_found = None
    current_year = datetime.now().year
    earliest_start_date = datetime(current_year - 3, 1, 1).date()  # Three years ago

    for year in range(earliest_start_date.year, current_year + 1):
        file_path = f"{DATA_PATH}{year}.csv"
        file_content = download_github_file(file_path)

        if file_content:
            df_year = pd.read_csv(io.StringIO(file_content))
            df_year["Tid"] = pd.to_datetime(df_year["Tid"], utc=True, errors="coerce")
            df_year = clean_existing_data(df_year)

            if not df_year.empty:
                latest_date_in_year = df_year["Tid"].max().date()
                if latest_date_found is None or latest_date_in_year > latest_date_found:
                    latest_date_found = latest_date_in_year

    return latest_date_found if latest_date_found else earliest_start_date


# Function to save the data into yearly files on GitHub
def save_data_by_year_to_github(df):
    df["Tid"] = pd.to_datetime(df["Tid"], utc=True, errors="coerce")
    df = df.dropna(subset=["Tid"])

    # Extract the year from the 'Tid' column for grouping
    df["Year"] = df["Tid"].dt.year

    # Track if any files were updated
    files_updated = []
    commit_times = []

    for year, year_data in df.groupby("Year"):
        file_name = f"{year}.csv"
        github_path = f"{github_folder}/{file_name}".replace("\\", "/")
        
        # Remove the temporary Year column
        year_data = year_data.drop(columns=["Year"])
        
        # Try to get existing data from GitHub
        file_content = download_github_file(github_path)
        if file_content:
            # If file exists, read it and combine with new data
            existing_data = pd.read_csv(io.StringIO(file_content))
            existing_data["Tid"] = pd.to_datetime(existing_data["Tid"], utc=True)
            
            # Combine existing and new data
            combined_data = pd.concat([existing_data, year_data])
            # Remove duplicates based on all columns except floating point columns
            combined_data = combined_data.drop_duplicates(subset=["Knr", "Kommunenavn", "Tid", "Gruppe"])
            # Sort by time
            combined_data = combined_data.sort_values("Tid")
        else:
            combined_data = year_data
        
        # Use handle_output_data to manage the file
        is_updated = handle_output_data(
            combined_data,
            file_name,
            github_folder,
            temp_folder,
            keepcsv=True
        )
        
        if is_updated:
            files_updated.append(file_name)
            commit_times.append(datetime.now())
            print(f"{get_timestamp()} New data detected in {file_name} and pushed to GitHub.")
        else:
            print(f"{get_timestamp()} No new data detected in {file_name}.")

    # Return the latest commit time if available
    latest_commit_time = max(commit_times) if commit_times else None
    return files_updated, latest_commit_time

def query_and_append_new_data():
    # Get the latest date from GitHub files
    latest_date = get_latest_date_from_github()
    print(f"{get_timestamp()} Latest date in GitHub files: {latest_date}")

    if latest_date:
        # Convert latest_date to datetime if it's a string
        if isinstance(latest_date, str):
            latest_date = pd.to_datetime(latest_date)

        # Query data for each day from the latest date until 2 days ago
        current_date = datetime.now() - timedelta(days=2)  # Only query up to 2 days ago
        dates_to_query = pd.date_range(start=latest_date + timedelta(days=1), end=current_date)

        if not dates_to_query.empty:
            all_data = []
            for date in dates_to_query:
                print(f"{get_timestamp()} Querying data for {date.date()}")
                df = query_elhub(date.date())  # Pass date.date() to get YYYY-MM-DD format
                if not df.empty:  # Only append if we got data
                    all_data.append(df)

            if all_data:  # Only process if we have any data
                # Combine all the new data
                combined_df = pd.concat(all_data, ignore_index=True)
                # Process the data
                processed_df = process_data(combined_df)
                # Save to GitHub by year
                files_updated, latest_commit_time = save_data_by_year_to_github(processed_df)
                
                # Write status log
                log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
                task_name_safe = task_name.replace(".", "_").replace(" ", "_")
                new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")
                
                with open(new_data_status_file, "w", encoding="utf-8") as log_file:
                    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if files_updated else 'No'},{latest_commit_time or ''}\n")
                
                print(f"{get_timestamp()} New data status log written to {new_data_status_file}")
                return bool(files_updated)
            else:
                print(f"\n{get_timestamp()} No new data retrieved from API.")
                print(f"{get_timestamp()} Latest data in GitHub: {latest_date.strftime('%Y-%m-%d')}")

                # Create status log for no new data
                log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
                task_name_safe = task_name.replace(".", "_").replace(" ", "_")
                new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")
                
                with open(new_data_status_file, "w", encoding="utf-8") as log_file:
                    log_file.write(f"{task_name_safe},multiple_files,No\n")
                
                print(f"{get_timestamp()} New data status log written to {new_data_status_file}")
                return False
        else:
            print(f"\n{get_timestamp()} No new dates to query.")
            return False
    else:
        print(f"{get_timestamp()} No existing data found in GitHub")
        return False

def main():
    try:
        query_and_append_new_data()
    except Exception as e:
        print(f"{get_timestamp()} Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
