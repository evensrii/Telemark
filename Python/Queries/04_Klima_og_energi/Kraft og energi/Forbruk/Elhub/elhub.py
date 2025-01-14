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
print(f"Loaded .env file from: {env_file_path}")

# Get the GITHUB_TOKEN from the environment
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

print("GITHUB_TOKEN loaded successfully.")

# Add PYTHONPATH to sys.path
PYTHON_PATH = os.environ.get("PYTHONPATH")
if PYTHON_PATH not in sys.path:
    sys.path.append(PYTHON_PATH)



# Script parameters
task_name = "Klima og energi - Elhub"
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub"
temp_folder = os.environ.get("TEMP_FOLDER")

# GitHub Repository information
REPO = "evensrii/Telemark"
BRANCH = "main"
GITHUB_API_URL = "https://api.github.com"
DATA_PATH = github_folder + "/"

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
        print(f"File not found: {file_path}")
        return None
    else:
        print(
            f"Failed to download file: {file_path}, Status Code: {response.status_code}"
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
        print(f"File uploaded successfully: {file_path}")
    else:
        print(f"Failed to upload file: {response.json()}")


# Function to query the Elhub API for a specific date
def query_elhub(date):
    url = f"https://api.elhub.no/energy-data/v0/municipalities?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR&startDate={date}"
    response = requests.get(url)
    data = response.json()

    df_data = pd.json_normalize(data["data"])
    df_data_expanded = df_data.explode("attributes.consumptionPerGroupMunicipalityHour")
    df_consumption = pd.json_normalize(
        df_data_expanded["attributes.consumptionPerGroupMunicipalityHour"]
    )

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
    any_files_updated = False

    for year, year_data in df.groupby("Year"):
        file_name = f"{year}.csv"
        
        # Remove the temporary Year column
        year_data = year_data.drop(columns=["Year"])
        
        # Try to get existing data from GitHub
        file_content = download_github_file(os.path.join(github_folder, file_name))
        if file_content:
            # If file exists, read it and combine with new data
            existing_data = pd.read_csv(io.StringIO(file_content))
            existing_data["Tid"] = pd.to_datetime(existing_data["Tid"], utc=True)
            
            # Combine existing and new data
            combined_data = pd.concat([existing_data, year_data])
            # Remove duplicates based on all columns
            combined_data = combined_data.drop_duplicates()
            # Sort by time
            combined_data = combined_data.sort_values("Tid")
        else:
            combined_data = year_data
        
        # Use handle_output_data to manage the file
        was_updated = handle_output_data(
            combined_data,
            file_name,
            github_folder,
            temp_folder
        )
        
        if was_updated:
            any_files_updated = True
            print(f"Updated data for year {year}")

    return any_files_updated


# Function to query and append new data based on the latest date in GitHub files
def query_and_append_new_data():
    # Get the latest date from GitHub files
    latest_date = get_latest_date_from_github()
    print(f"Latest date in GitHub files: {latest_date}")

    # Set the current date to two days ago to avoid empty data
    end_date = (datetime.now() - timedelta(days=2)).date()

    # Query new data starting from one day after the latest date
    current_date = latest_date + timedelta(days=1)

    # Initialize an empty DataFrame to store all new data
    all_new_data = pd.DataFrame()

    # Loop through each day from current_date to end_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Querying data for {date_str}")

        # Query the API for the current date
        daily_data = query_elhub(date_str)
        
        if not daily_data.empty:
            # Process the data
            daily_data = process_data(daily_data)
            all_new_data = pd.concat([all_new_data, daily_data], ignore_index=True)

        current_date += timedelta(days=1)

    if not all_new_data.empty:
        # Save data by year and check if any files were updated
        files_updated = save_data_by_year_to_github(all_new_data)
        
        if not files_updated:
            print("No new data to upload")
            
        # Create status log
        with open(os.path.join(PYTHON_PATH, "Log", f"new_data_status_{task_name.replace(' ', '_')}.log"), "w") as f:
            f.write("New data: No" if not files_updated else "New data: Yes")
    else:
        print("No new data retrieved from API")
        # Create status log for no new data
        with open(os.path.join(PYTHON_PATH, "Log", f"new_data_status_{task_name.replace(' ', '_')}.log"), "w") as f:
            f.write("New data: No")


def main():
    """Main function to execute the script."""
    try:
        query_and_append_new_data()
        print("Script completed successfully!")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()
