import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors

from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

################# Spørring #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:KommGjeldende",
        "values": [
          "4001",
          "4003",
          "4005",
          "4010",
          "4012",
          "4014",
          "4016",
          "4018",
          "4020",
          "4022",
          "4024",
          "4026",
          "4028",
          "4030",
          "4032",
          "4034",
          "4036"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",                              
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Personer"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2024"
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
    df = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Tittel spørring",
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

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "xxx.csv"
task_name = "Tema - Tittel"
github_folder = "Data/07_Idrett_friluftsliv_og_frivillighet/Friluftsliv"
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

print(f"New data status log written to {new_data_status_file}")   Q2