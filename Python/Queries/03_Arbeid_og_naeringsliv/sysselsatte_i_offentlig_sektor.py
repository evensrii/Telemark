import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/13472/"

################## KOMMUNER

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommSummer",
        "values": [
          "K-4001",
          "K-4003",
          "K-4005",
          "K-4010",
          "K-4012",
          "K-4014",
          "K-4016",
          "K-4018",
          "K-4020",
          "K-4022",
          "K-4024",
          "K-4026",
          "K-4028",
          "K-4030",
          "K-4032",
          "K-4034",
          "K-4036"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "6100",
          "6500.6",
          "6500.7",
          "A+B+D+E.5-7",
          "A+B+D+E.0-1+9"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselEtterArbste"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "8"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df = fetch_data(
        url=POST_URL,
        payload=payload,
        error_messages=error_messages,
        query_name="Arbeidsmarkedstilknytning, kommuner",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print("\nOriginal data:")
print(df.head())

# Create a mask for private sector
private_sector_mask = df['sektor'] == 'Privat sektor'

# Split the data into private and public sectors
private_sector = df[private_sector_mask].copy()
public_sector = df[~private_sector_mask].copy()

# Sum up all public sector values for each region
public_sector_sum = public_sector.groupby(['region', 'statistikkvariabel', 'år'])['value'].sum().reset_index()
public_sector_sum['sektor'] = 'Offentlig sektor'

# Combine private and public sector data
df_processed = pd.concat([public_sector_sum, private_sector], ignore_index=True)

# Clean up the region names and add Label column
df_processed["region"] = df_processed["region"].str.replace(r'\s*\(\d{4}-\d{4}\)', '', regex=True)
df_processed = df_processed.rename(columns={
    "region": "Kommune", 
    "value": "Antall sysselsatte"
})
df_processed["Label"] = df_processed["Kommune"]

# Sort by Kommune and sektor
df_processed = df_processed.sort_values(['Kommune', 'sektor'])

print("\nProcessed data:")
print(df_processed)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "sysselsatte_i_offentlig_sektor.csv"
task_name = "Arbeid og naeringsliv - Sysselsatte i offentlig sektor"
github_folder = "Data/03_Arbeid og næringsliv/Sysselsetting"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_processed, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
