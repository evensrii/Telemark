import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
import numpy as np

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import compare_to_github
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

################# Felte elg #################

### Fylker 2024, sammenslåtte tidsserier

# Endepunkt for SSB API
POST_URL_elg = "https://data.ssb.no/api/v0/no/table/03432/"

# Spørring for å hente ut data fra SSB
payload_elg = {
    "query": [
        {
            "code": "Region",
            "selection": {"filter": "agg:KommFylker", "values": ["F-40"]},
        },
        {"code": "Dyr", "selection": {"filter": "item", "values": ["00a"]}},
    ],
    "response": {"format": "json-stat2"},
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_elg = fetch_data(
        url=POST_URL_elg,
        payload=payload_elg,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Elg",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df_elg.head()
# df_elg.info()


################# Felte hjort #################

### Fylker 2024, sammenslåtte tidsserier

# Endepunkt for SSB API
POST_URL_hjort = "https://data.ssb.no/api/v0/no/table/03434/"

# Spørring for å hente ut data fra SSB
payload_hjort = {
    "query": [
        {
            "code": "Region",
            "selection": {"filter": "agg:KommFylker", "values": ["F-40"]},
        },
        {"code": "Dyr", "selection": {"filter": "item", "values": ["00a"]}},
    ],
    "response": {"format": "json-stat2"},
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_hjort = fetch_data(
        url=POST_URL_hjort,
        payload=payload_hjort,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Hjort",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df_hjort.head()
# df_hjort.info()


df_elg = df_elg.drop(columns=["region", "alder", "statistikkvariabel"])
df_elg.columns = ["Jaktår", "Antall felte elg"]

df_hjort = df_hjort.drop(columns=["region", "alder", "statistikkvariabel"])
df_hjort.columns = ["Jaktår", "Antall felte hjort"]

df_hjort.info()

## Tall for felte hjort mangler for 2008/2009. Bruker lineær interpolering (fra numpy) for å estimere verdien.
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].replace(0, np.nan)
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].interpolate(
    method="linear"
)
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].round()
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].astype(int)

# Merge dataframes
df = pd.merge(df_elg, df_hjort, on="Jaktår")

# Remove everything after "-" in "Jaktår" (including "-")
df["Jaktår"] = df["Jaktår"].str.split("-").str[0]


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "antall_felte_elg_og_hjort.csv"
task_name = "Klima og energi - Felte hjortedyr"
github_folder = "Data/04_Klima og ressursforvaltning/Ressursforvaltning"
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