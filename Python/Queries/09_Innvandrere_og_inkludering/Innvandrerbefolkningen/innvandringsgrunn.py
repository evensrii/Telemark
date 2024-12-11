import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
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

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

# Finner URL vha. "Inspiser side" og fane "Network"
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/befolkning_innvandringsgrunn"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Innvandringsgrunn",
        response_type="csv",  # The expected response type, either 'json' or 'csv'.
        delimiter=";",  # The delimiter for CSV data (default: ';').
        encoding="ISO-8859-1",  # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Print the unique values in the column "Kommunenummer"
# print(df["Kommunenummer"].unique())
# df.dtypes

df.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "innvandringsgrunn_telemark.csv"
github_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"
temp_folder = os.environ.get("TEMP_FOLDER")

""" compare_to_github(
    df, file_name, github_folder, temp_folder
)  # <--- Endre navn på dataframe her!
 """
# Call the function and get the "New Data" status
is_new_data = compare_to_github(df, file_name, github_folder, temp_folder)

# Write the "New Data" status to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    log_file.write(f"{file_name},New Data,{is_new_data}\n")

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
