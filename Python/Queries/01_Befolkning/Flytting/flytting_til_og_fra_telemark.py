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
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

################# Spørring - Tilflytting #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/13864/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "TilflyttRegion",
      "selection": {
        "filter": "agg_single:FylkerFastland01",
        "values": [
          "40"
        ]
      }
    },
    {
      "code": "Fraflyttingsregion",
      "selection": {
        "filter": "agg_single:FylkerFastland01",
        "values": [
          "31",
          "32",
          "03",
          "33",
          "34",
          "39",
          "42",
          "11",
          "46",
          "15",
          "50",
          "18",
          "55",
          "56"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_flytting_til = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Flytting til Telemark",
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

### DATA CLEANING

df_flytting_til = df_flytting_til.drop(columns=["tilflyttingsregion", "statistikkvariabel"])
df_flytting_til = df_flytting_til.rename(columns={"fraflyttingsregion": "Fra fylke", "år": "År", "value": "Antall"})
first_year = df_flytting_til["År"].iloc[0]
df_flytting_til = df_flytting_til.rename(columns={"Antall": f"Antall ({first_year})"})
df_flytting_til = df_flytting_til.drop(columns=["År"])
df_flytting_til["Fra fylke"] = df_flytting_til["Fra fylke"].replace({"Trøndelag - Trööndelage": "Trøndelag", "Nordland - Nordlánnda": "Nordland", "Troms - Romsa - Tromssa": "Troms", "Finnmark - Finnmárku - Finmarkku": "Finnmark"})
df_flytting_til.insert(0, "series", "Series 1")

################# Spørring - Fraflytting #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/13864/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "TilflyttRegion",
      "selection": {
        "filter": "agg_single:FylkerFastland01",
        "values": [
          "31",
          "32",
          "03",
          "33",
          "34",
          "39",
          "42",
          "11",
          "46",
          "15",
          "50",
          "18",
          "55",
          "56"
        ]
      }
    },
    {
      "code": "Fraflyttingsregion",
      "selection": {
        "filter": "agg_single:FylkerFastland01",
        "values": [
          "40"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_flytting_fra = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Flytting fra Telemark",
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

### DATA CLEANING

df_flytting_fra = df_flytting_fra.drop(columns=["fraflyttingsregion", "statistikkvariabel"])
df_flytting_fra = df_flytting_fra.rename(columns={"tilflyttingsregion": "Til fylke", "år": "År", "value": "Antall"})
first_year = df_flytting_fra["År"].iloc[0]
df_flytting_fra = df_flytting_fra.rename(columns={"Antall": f"Antall ({first_year})"})
df_flytting_fra = df_flytting_fra.drop(columns=["År"])
df_flytting_fra["Til fylke"] = df_flytting_fra["Til fylke"].replace({"Trøndelag - Trööndelage": "Trøndelag", "Nordland - Nordlánnda": "Nordland", "Troms - Romsa - Tromssa": "Troms", "Finnmark - Finnmárku - Finmarkku": "Finnmark"})
df_flytting_fra.insert(0, "series", "Series 1")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name1 = "tilflytting.csv"
file_name2 = "fraflytting.csv"
task_name = "Flytting - Til og fra Telemark"
github_folder = "Data/01_Befolkning/Flytting"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data1 = handle_output_data(df_flytting_til, file_name1, github_folder, temp_folder, keepcsv=True)
is_new_data2 = handle_output_data(df_flytting_fra, file_name2, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if (is_new_data1 or is_new_data2) else 'No'}\n")

# Output results for debugging/testing
if is_new_data1:
    print(f"New data detected in {file_name1} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name1}.")

if is_new_data2:
    print(f"New data detected in {file_name2} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name2}.")

print(f"New data status log written to {new_data_status_file}")