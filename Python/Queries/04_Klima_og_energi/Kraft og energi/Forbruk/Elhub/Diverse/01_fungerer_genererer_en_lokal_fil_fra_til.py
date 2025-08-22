import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Define the path for the existing CSV file
temp_folder = os.environ.get("TEMP_FOLDER")
csv_file = os.path.join(temp_folder, "elhub_telemark.csv")


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

    # Convert 'attributes.municipalityNumber' to numeric, coercing errors to NaN
    df_combined["attributes.municipalityNumber"] = pd.to_numeric(
        df_combined["attributes.municipalityNumber"], errors="coerce"
    )

    # Drop rows where 'attributes.municipalityNumber' is NaN
    df_combined.dropna(subset=["attributes.municipalityNumber"], inplace=True)

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

    # Convert the "Tid" column to datetime format
    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")

    # Rename values in the "Gruppe" column
    df["Gruppe"] = df["Gruppe"].replace(
        {
            "business": "Næring (unntatt industri)",
            "industry": "Industri",
            "private": "Husholdninger",
        }
    )

    return df


# Function to query and append new data
def query_and_append_new_data(df_existing, start_date_str=None, end_date_str=None):
    # Determine the start date
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        print(f"Using provided start date: {start_date}")
    elif not df_existing.empty and "Tid" in df_existing.columns:
        latest_date_in_csv = pd.to_datetime(df_existing["Tid"]).max().date()
        start_date = latest_date_in_csv + timedelta(days=1)
        print(f"Latest date in CSV: {latest_date_in_csv}, starting from next day.")
    else:
        start_date = datetime(2022, 1, 1).date()
        print(f"No existing data or 'Tid' column, starting from {start_date}")

    # Determine the end date
    if end_date_str:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        print(f"Using provided end date: {end_date}")
    else:
        end_date = (datetime.now() - timedelta(days=2)).date()
        print(f"Using default end date (2 days ago): {end_date}")

    # Initialize a list to store all daily data
    new_data = []
    current_date = start_date

    # Loop through each day from current_date to end_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Querying data for {date_str}")

        # Query the API for the current date and collect the data
        daily_data = query_elhub(date_str)

        if not daily_data.empty:
            # Append the daily data to the list
            new_data.append(daily_data)

        # Move to the next day
        current_date += timedelta(days=1)

    # If there's new data, process and append it to the existing data
    if new_data:
        new_data_df = pd.concat(new_data, ignore_index=True)
        new_data_df = process_data(new_data_df)

        # Combine the cleaned new data with the cleaned existing data
        combined_data = pd.concat([df_existing, new_data_df], ignore_index=True)

        # Save the updated data to the CSV
        combined_data.to_csv(csv_file, index=False)
        print("New data appended and CSV updated.")
    else:
        print("No new data to update.")


def main(start_date=None, end_date=None):
    # Step 1: Load the existing CSV file (if it exists)
    try:
        df_existing = pd.read_csv(csv_file)
        df_existing["Tid"] = pd.to_datetime(df_existing["Tid"], errors="coerce")

        # Clean the existing data by removing rows where 'Tid' and 'Gruppe' are empty
        df_existing = clean_existing_data(df_existing)
    except FileNotFoundError:
        # If the CSV doesn't exist, initialize an empty DataFrame
        df_existing = pd.DataFrame()

    # Step 2: Query and append new data to the existing CSV
    query_and_append_new_data(df_existing, start_date, end_date)


# Run the main function
if __name__ == "__main__":
    # To run with a specific date range, uncomment the following line and set the dates:
    main(start_date="2022-01-01", end_date="2022-12-31")

    # To run with default date logic (from last date in file to two days ago), use this:
    # main()

# Explanation:
# 1) Initial Data Collection:

# If this is the first run (i.e., the CSV file does not exist), the script will query the Elhub API starting from 2021-01-01 and store the data in the CSV after performing necessary transformations (renaming columns, filtering, etc.).

# 2) Subsequent Data Appending:

# On subsequent runs, the script will load the existing CSV, clean any rows where Tid and Gruppe are empty, and determine the latest date in the CSV file.
# It will then query new data starting from one day after the latest date in the CSV, perform the same transformations, and append the new data to the existing CSV.

# 3) Data Cleaning:

# Before appending new data, the script will ensure that rows with empty values in Tid and Gruppe columns are removed from both the new and existing data.

# 4) End Date Handling:

# The script will stop querying two days before today to avoid querying empty data (since Elhub's data may not be updated immediately for today or yesterday).

# Key Functions:
# query_elhub(date): Queries the Elhub API for a given date.
# clean_existing_data(df): Removes rows where Tid or Gruppe is empty from the existing data.
# process_data(df): Renames columns and processes the queried data to prepare it for saving.
# query_and_append_new_data(df_existing): Handles the logic for appending new data from Elhub to the existing CSV file.

# Output:
# The script will either create a new CSV file or update the existing one with the latest available data.
