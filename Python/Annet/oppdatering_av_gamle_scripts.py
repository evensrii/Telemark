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
----------------------------------

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

url = "..."

try:
    df = fetch_data(
        url=url,
        payload=None, #The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Query name",
        response_type="csv", # The expected response type, either 'json' or 'csv'.
        delimiter=";", # The delimiter for CSV data (default: ';'). Comment if json.
        encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1'). Comment if json.
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

----------------------------------

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_sysselsatte_innvandrere.csv"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
temp_folder = os.environ.get("TEMP_FOLDER")

compare_to_github(
    df_filtered, file_name, github_folder, temp_folder
)  # <--- Endre navn på dataframe her!

##################### Remove temporary local files #####################

delete_files_in_temp_folder()





# ------------ Etter overgang til Python master script ------------

from Helper_scripts.github_functions import handle_output_data




