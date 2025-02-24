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
POST_URL = "https://data.ssb.no/api/v0/no/table/13470/"

################## TELEMARK

# Spørring for å hente ut data fra SSB
payload_telemark = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommFylker",
        "values": [
          "F-40"
        ]
      }
    },
    {
      "code": "NACE2007",
      "selection": {
        "filter": "agg_single:NACE2007arb11",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68-75",
          "77-82",
          "84",
          "85",
          "86-88",
          "90-99"
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
    df_telemark = fetch_data(
        url=POST_URL,
        payload=payload_telemark,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Sysselsatte i næringer, Telemark",
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



################## LANDET

# Spørring for å hente ut data fra SSB
payload_landet = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "NACE2007",
      "selection": {
        "filter": "agg_single:NACE2007arb11",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68-75",
          "77-82",
          "84",
          "85",
          "86-88",
          "90-99"
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
    df_landet = fetch_data(
        url=POST_URL,
        payload=payload_landet,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Sysselsatte i næringer, landet",
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
most_recent_year = df_telemark['år'].iloc[0]

# Process Telemark data
telemark_sum = df_telemark['value'].sum()
df_telemark[f'Telemark ({most_recent_year})'] = (df_telemark['value'] / telemark_sum * 100).round(1)
df_telemark = df_telemark.drop(['value', 'statistikkvariabel', 'år', 'region'], axis=1)

# Process national data
landet_sum = df_landet['value'].sum()
df_landet[f'Landet ({most_recent_year})'] = (df_landet['value'] / landet_sum * 100).round(1)
df_landet = df_landet.drop(['value', 'statistikkvariabel', 'år', 'region'], axis=1)

print("\nProcessed Telemark data:")
print(df_telemark)
print("\nProcessed national data:")
print(df_landet)

# Combine the two dataframes
df_combined = pd.merge(df_telemark, df_landet, on='næring (SN2007)', how='outer')
df_combined = df_combined[['næring (SN2007)', f'Telemark ({most_recent_year})', f'Landet ({most_recent_year})']]

# Sort df_combined by the Telemark column, descending
df_combined = df_combined.sort_values(f'Telemark ({most_recent_year})', ascending=False)

#Filter the top 5 rows
df_combined = df_combined.head(5)

print("\nCombined data:")
print(df_combined)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "ansatte_i_storste_naringer.csv"
task_name = "Arbeid og naeringsliv - Sysselsatte i naeringer"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Næringsstruktur og arbeidsplasser"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_combined, file_name, github_folder, temp_folder, keepcsv=True)

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
