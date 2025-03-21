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
    df = df.drop(columns=["endTime", "lastUpdatedTime"], errors="ignore")

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

    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")
    df = df.dropna(subset=["Tid"])

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
        file_content = download_github_file(file_path)

        if file_content:
            df_year = pd.read_csv(io.StringIO(file_content))
            df_year["Tid"] = pd.to_datetime(df_year["Tid"], errors="coerce")
            df_year = clean_existing_data(df_year)

            if not df_year.empty:
                latest_date_in_year = df_year["Tid"].max().date()
                if latest_date_found is None or latest_date_in_year > latest_date_found:
                    latest_date_found = latest_date_in_year

    return latest_date_found


# Function to save the data into yearly files on GitHub
def save_data_by_year_to_github(df):
    # Convert 'Tid' column to datetime and drop invalid dates
    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")
    df = df.dropna(subset=["Tid"])

    # Extract the year from the 'Tid' column for grouping
    df["Year"] = df["Tid"].dt.year

    for year, year_data in df.groupby("Year"):
        file_path = f"{DATA_PATH}{year}.csv"
        file_content = download_github_file(file_path)

        if file_content:
            existing_data = pd.read_csv(io.StringIO(file_content))
            existing_data = existing_data.drop(
                columns=["Year"], errors="ignore"
            )  # Ensure existing data does not have 'Year'

            # Merge existing and new data
            year_data = pd.concat([existing_data, year_data], ignore_index=True)

        # Drop the "Year" column before saving
        year_data = year_data.drop(columns=["Year"], errors="ignore")
        csv_content = year_data.to_csv(index=False)
        upload_github_file(file_path, csv_content, message=f"Updating data for {year}")


# Function to query and append new data based on the latest date in GitHub files
def query_and_append_new_data():
    latest_date_found = get_latest_date_from_github()

    if latest_date_found is None:
        # Start from 2021-01-01 if no data exists
        latest_date_found = datetime(2021, 1, 1).date()
        print(f"No data found, starting from {latest_date_found}")
    else:
        print(f"Latest date found in GitHub files: {latest_date_found}")

    end_date = (datetime.now() - timedelta(days=2)).date()

    # Initialize an empty list to store new data
    new_data = []

    # Loop through each year, starting from the latest year found
    start_year = latest_date_found.year
    for year in range(start_year, datetime.now().year + 1):
        # If it's the first year and starting from the middle, use the `latest_date_found`
        if year == start_year and latest_date_found > datetime(year, 1, 1).date():
            current_date = latest_date_found + timedelta(days=1)
        else:
            current_date = datetime(year, 1, 1).date()

        # Collect only the first week of data for the year
        while current_date <= end_date and current_date < datetime(year, 1, 8).date():
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
        print("First week data for each year appended and GitHub files updated.")
    else:
        print("No new data to update.")


def main():
    query_and_append_new_data()


if __name__ == "__main__":
    main()
