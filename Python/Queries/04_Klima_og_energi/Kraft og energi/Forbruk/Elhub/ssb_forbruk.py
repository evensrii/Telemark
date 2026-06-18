import requests
import sys
import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# SSB API v2 (GET request) - Tabell 14490: Elektrisitetsforbruk etter forbrukargruppe
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/14490/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Forbruk"
    "&valueCodes[Tid]=*"
    "&valueCodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&valueCodes[Forbrukargruppe]=*"
    "&codelist[Region]=agg_KommSummer"
    "&outputValues[Region]=aggregated"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="SSB 14490 - Elektrisitetsforbruk",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


### DATA CLEANING

# Inspect the dataframe
print(df.head())
print(df.columns.tolist())
print(df.dtypes)

# Rename columns to more readable names
df = df.rename(columns={
    "region": "Kommune",
    "forbrukergruppe": "Forbrukergruppe",
    "statistikkvariabel": "Statistikkvariabel",
    "år": "År",
    "value": "Verdi",
})

# Drop the "Statistikkvariabel" column (only contains "Forbruk")
df = df.drop(columns=["Statistikkvariabel"])

# Convert "Verdi" to float64 for compatibility with github_functions
df["Verdi"] = pd.to_numeric(df["Verdi"], errors="coerce")

# Sort by year, region, and consumer group
df = df.sort_values(by=["År", "Kommune", "Forbrukergruppe"]).reset_index(drop=True)

print(f"\nFinal dataframe shape: {df.shape}")
print(df.head(20))


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "ssb_forbruk.csv"
task_name = "Klima og energi - Stromforbruk (SSB)"
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/SSB"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

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
