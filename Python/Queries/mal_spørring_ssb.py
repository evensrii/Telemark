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

""" ######### Kjøre spørring første gang, for å finne siste år (top 1)

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/XXXXX/"

# Spørring for å hente ut data fra SSB
payload_most_recent_year = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommSummer",
        "values": [
          "K-4001",
        ]
      }
    },
    {
      "code": "NACE2007",
      "selection": {
        "filter": "agg_single:NACE2007arb11",
        "values": [
          "01-03",
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselsatteArb"
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
    df_last_year = fetch_data(
        url=POST_URL,
        payload=payload_most_recent_year,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Tittel spørring - første år",
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

# Get most recent year
most_recent_year = df_last_year['år'].iloc[0]

# Create a list of years from XXXX until most_recent_year, years as strings enclosed in ""
years = [str(year) for year in range(2016, int(most_recent_year) + 1)]

# Kjøre spørring på nytt, fra første til siste år

## Bruke "years" (uten anførselstegn) sammen med "item" for år!!!!!!!!
#      "code": "Tid",
#      "selection": {
#        "filter": "item",
#        "values": years
#      }
#  """

################# Spørring #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/XXXXX/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {"code": "Region", "selection": {"filter": "vs:FylkerJakt", "values": ["40"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["1", "2"]}},
        {
            "code": "Alder",
            "selection": {
                "filter": "item",
                "values": [
                    "00-19a",
                    "20-29",
                    "30-39",
                    "40-49",
                    "50-59",
                    "60-69",
                    "070+",
                ],
            },
        },
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["BetaltJegeravg"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
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

print(f"New data status log written to {new_data_status_file}")