import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import compare_to_github

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

----------------------------------

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    # Fetch data using the fetch_data function, with separate calls for each request
    df_vtfk = fetch_data(POST_URL, payload_vtfk, error_messages, query_name="VTFK")
    df_tfk = fetch_data(POST_URL, payload_tfk, error_messages, query_name="TFK")

except Exception as e:
    # If any query fails, send the error notification and stop execution
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

----------------------------------

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

# Lagre som .csv i Temp folder
csv_file_name = "andel_sysselsatte_innvandrere.csv"
df_filtered.to_csv(os.path.join(temp_folder, csv_file_name), index=False)

# GitHub configuration (Repo etc. is defined in the function)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Ensure this is set in your environment

destination_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"

github_file_path = f"{destination_folder}/{csv_file_name}"
local_file_path = os.path.join(temp_folder, csv_file_name)

# Download the existing file from GitHub
existing_data = download_github_file(github_file_path)

# Check if new data compared to Github
if existing_data is not None:
    # Compare the existing data with the new data
    existing_df = existing_data.astype(str).sort_values(by=list(existing_data.columns))
    new_df = (
        pd.read_csv(local_file_path)
        .astype(str)
        .sort_values(by=list(existing_data.columns))
    )

    if existing_df.equals(new_df):
        print("No new data to upload. Skipping GitHub update.")
    else:
        print("New data detected. Uploading to GitHub.")
        upload_github_file(
            local_file_path, github_file_path, message=f"{csv_file_name} updated"
        )
else:
    # If the file does not exist on GitHub, upload the new file
    print("File not found on GitHub. Uploading new file.")
    upload_github_file(
        local_file_path, github_file_path, message=f"{csv_file_name} added"
    )

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
