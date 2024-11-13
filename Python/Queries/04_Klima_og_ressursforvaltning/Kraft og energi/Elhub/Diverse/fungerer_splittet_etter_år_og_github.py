import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from dotenv import load_dotenv
import io

# Load GitHub token from the .env file
load_dotenv("../../../token.env", override=True)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    raise ValueError(
        "GitHub token not found. Please ensure that the token is set in '../../../token.env'"
    )

# GitHub Repository information
REPO = "evensrii/Telemark"
BRANCH = "main"
GITHUB_API_URL = "https://api.github.com"
DATA_PATH = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub/"


def download_github_file(file_path):
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{file_path}?ref={BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",  # Get the raw content directly
    }

    # Print the URL and headers for debugging
    print(f"Downloading file from URL: {url}")

    response = requests.get(url, headers=headers)

    # Print the response status for debugging
    print(f"Response Status Code: {response.status_code}")

    if response.status_code == 200:
        # Return the raw file content (already decoded)
        return response.text  # Raw CSV content
    elif response.status_code == 404:
        print(f"File not found: {file_path}")
        return None
    else:
        print(
            f"Failed to download file: {file_path}, Status Code: {response.status_code}"
        )
        print(f"Response: {response.text}")  # Log the response for debugging
        return None


# Function to upload a file to GitHub
def upload_github_file(file_path, content, message="Updating data"):
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # First, check if the file exists (to get the SHA for updates)
    response = requests.get(url, headers=headers)
    sha = response.json().get("sha") if response.status_code == 200 else None

    # Prepare the payload
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha

    # Make the request
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

    # Step 1: Normalize the 'data' field
    df_data = pd.json_normalize(data["data"])

    # Step 2: Expand the 'attributes.consumptionPerGroupMunicipalityHour' field while keeping index intact
    df_data_expanded = df_data.explode("attributes.consumptionPerGroupMunicipalityHour")

    # Step 3: Normalize the 'consumptionPerGroupMunicipalityHour' field to flatten it
    df_consumption = pd.json_normalize(
        df_data_expanded["attributes.consumptionPerGroupMunicipalityHour"]
    )

    # Reset the index to ensure both dataframes are aligned properly
    df_data_expanded = df_data_expanded.reset_index(drop=True)
    df_consumption = df_consumption.reset_index(drop=True)

    # Step 4: Combine the 'municipalityNumber' and 'name' with the expanded consumption data
    df_combined = pd.concat(
        [
            df_data_expanded[["attributes.municipalityNumber", "attributes.name"]],
            df_consumption,
        ],
        axis=1,
    )

    # Filter for municipalities in Telemark (Knr 4000 to 4200)
    df_telemark = df_combined[
        (df_combined["attributes.municipalityNumber"].astype(int) > 4000)
        & (df_combined["attributes.municipalityNumber"].astype(int) < 4200)
    ]

    return df_telemark


# Function to remove rows where 'Tid' and 'Gruppe' columns are empty
def clean_existing_data(df):
    return df.dropna(subset=["Tid", "Gruppe"])


# Function to rename columns and process the data
def process_data(df):
    # Remove unnecessary columns "endTime" and "lastUpdatedTime"
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

    # Ensure proper conversion of the "Tid" column to datetime format
    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")

    # Drop rows where "Tid" could not be converted to a valid datetime
    df = df.dropna(subset=["Tid"])

    # Rename values in the "Gruppe" column
    df["Gruppe"] = df["Gruppe"].replace(
        {
            "business": "Næring (unntatt industri)",
            "industry": "Industri",
            "private": "Husholdninger",
        }
    )

    return df


# Function to determine the latest date across all yearly CSV files on GitHub
def get_latest_date_from_github():
    latest_date_found = None
    for year in range(2021, datetime.now().year + 1):
        file_path = f"{DATA_PATH}{year}.csv"

        # Download the CSV content from GitHub
        file_content = download_github_file(file_path)

        if file_content:  # If we successfully get the file content
            # Use io.StringIO to read the CSV content
            df_year = pd.read_csv(io.StringIO(file_content))
            df_year["Tid"] = pd.to_datetime(df_year["Tid"], errors="coerce")
            df_year = clean_existing_data(df_year)

            if not df_year.empty:
                latest_date_in_year = df_year["Tid"].max().date()
                if latest_date_found is None or latest_date_in_year > latest_date_found:
                    latest_date_found = latest_date_in_year
        else:
            print(f"No content or file not found for {file_path}")

    return latest_date_found


# Function to save the data into yearly files on GitHub
def save_data_by_year_to_github(df):
    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")
    df = df.dropna(subset=["Tid"])

    df["Year"] = df["Tid"].dt.year

    for year, year_data in df.groupby("Year"):
        file_path = f"{DATA_PATH}{year}.csv"

        # Download existing data from GitHub
        file_content = download_github_file(file_path)

        if file_content:  # If file exists, load the content and append to it
            # Use io.StringIO to read the existing CSV content
            existing_data = pd.read_csv(io.StringIO(file_content))
            year_data = pd.concat([existing_data, year_data], ignore_index=True)

        # Save the updated CSV content back to GitHub
        csv_content = year_data.to_csv(index=False)
        upload_github_file(file_path, csv_content, message=f"Updating data for {year}")


# Function to query and append new data based on the latest date in GitHub files
def query_and_append_new_data():
    latest_date_found = get_latest_date_from_github()

    if latest_date_found is None:
        latest_date_found = datetime(2021, 1, 1).date()
        print(f"No data found, starting from {latest_date_found}")
    else:
        print(f"Latest date found in GitHub files: {latest_date_found}")

    end_date = (datetime.now() - timedelta(days=2)).date()
    current_date = latest_date_found + timedelta(days=1)

    new_data = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Querying data for {date_str}")
        daily_data = query_elhub(date_str)

        if not daily_data.empty:
            new_data.append(daily_data)
        current_date += timedelta(days=1)

    if new_data:
        new_data = pd.concat(new_data, ignore_index=True)
        new_data = process_data(new_data)
        save_data_by_year_to_github(new_data)
        print("New data appended and GitHub files updated.")
    else:
        print("No new data to update.")


def main():
    query_and_append_new_data()


if __name__ == "__main__":
    main()
