import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# Directory for storing yearly CSV files
output_dir = "../../../Temp/elhub_yearly/"


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


# Function to determine the latest date across all yearly CSV files
def get_latest_date_from_year_files():
    latest_date_found = None
    # Check files from the earliest year (2021) up to the current year
    for year in range(2021, datetime.now().year + 1):
        year_file = os.path.join(output_dir, f"{year}.csv")
        if os.path.exists(year_file):
            df_year = pd.read_csv(year_file)
            df_year["Tid"] = pd.to_datetime(df_year["Tid"], errors="coerce")
            df_year = clean_existing_data(df_year)
            if not df_year.empty:
                latest_date_in_year = df_year["Tid"].max().date()
                if latest_date_found is None or latest_date_in_year > latest_date_found:
                    latest_date_found = latest_date_in_year
    return latest_date_found


# Function to save the data into yearly files
def save_data_by_year(df):
    # Ensure "Tid" column is properly converted to datetime
    df["Tid"] = pd.to_datetime(df["Tid"], errors="coerce")

    # Drop rows where "Tid" is invalid
    df = df.dropna(subset=["Tid"])

    # Extract the year from the 'Tid' column
    df["Year"] = df["Tid"].dt.year

    # Save data by year
    for year, year_data in df.groupby("Year"):
        year_file = os.path.join(output_dir, f"{year}.csv")
        # Append to the file if it exists, otherwise create a new one
        if os.path.exists(year_file):
            existing_data = pd.read_csv(year_file)
            year_data = pd.concat([existing_data, year_data], ignore_index=True)
        year_data.to_csv(year_file, index=False)


# Function to query and append new data based on the latest date across all files
def query_and_append_new_data():
    # Determine the latest date from existing CSV files
    latest_date_found = get_latest_date_from_year_files()

    if latest_date_found is None:
        # If no date was found, start from 2021-01-01
        latest_date_found = datetime(2021, 1, 1).date()
        print(f"No data found, starting from {latest_date_found}")
    else:
        print(f"Latest date found in files: {latest_date_found}")

    # Set the end date as two days ago to avoid querying incomplete data
    end_date = (datetime.now() - timedelta(days=2)).date()

    # Query new data starting from one day after the latest date found
    current_date = latest_date_found + timedelta(days=1)

    # Initialize a list to store all daily data
    new_data = []

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

    # If there's new data, process and split it by year
    if new_data:
        new_data = pd.concat(new_data, ignore_index=True)
        new_data = process_data(new_data)
        save_data_by_year(new_data)
        print("New data appended and CSVs updated.")
    else:
        print("No new data to update.")


def main():
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Query and append new data to the appropriate yearly files
    query_and_append_new_data()


# Run the main function
if __name__ == "__main__":
    main()
