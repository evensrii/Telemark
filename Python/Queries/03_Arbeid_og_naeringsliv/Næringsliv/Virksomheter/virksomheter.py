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
POST_URL = "https://data.ssb.no/api/v0/no/table/10309/"

################## VIRKSOMHETER I KOMMUNENE

# Spørring for å hente ut data fra SSB
payload_virksomheter = {
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
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35",
          "36-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68",
          "69-75",
          "77-82",
          "84.100-84.399",
          "85",
          "86-88",
          "90-93",
          "94-96",
          "97",
          "99",
          "00"
        ]
      }
    },
    {
      "code": "AntAnsatte",
      "selection": {
        "filter": "item",
        "values": [
          "00",
          "01",
          "02",
          "03",
          "04",
          "05",
          "15",
          "16"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_virksomheter = fetch_data(
        url=POST_URL,
        payload=payload_virksomheter,
        error_messages=error_messages,
        query_name="Virksomheter i kommunene",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


################## VIRKSOMHETER I LANDET

# Spørring for å hente ut data fra SSB
payload_virksomheter_landet = {
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
        "filter": "item",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35",
          "36-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68",
          "69-75",
          "77-82",
          "84.100-84.399",
          "85",
          "86-88",
          "90-93",
          "94-96",
          "97",
          "99",
          "00"
        ]
      }
    },
    {
      "code": "AntAnsatte",
      "selection": {
        "filter": "item",
        "values": [
          "00",
          "01",
          "02",
          "03",
          "04",
          "05",
          "15",
          "16"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_virksomheter_landet = fetch_data(
        url=POST_URL,
        payload=payload_virksomheter_landet,
        error_messages=error_messages,
        query_name="Virksomheter i landet",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


##################### Analyse av totalt antall virksomheter per år - Telemark #####################

# Filtrer ut "Alle næringer" (NACE2007 kode "00-99") og beregn totalt antall virksomheter per år
yearly_totals = df_virksomheter.groupby('år')['value'].sum()
print("\nTotalt antall virksomheter i Telemark per år :")
print(yearly_totals)

# Beregn prosentvis endring fra 2009 til siste år
first_year = 2009
latest_year = yearly_totals.index.max()
first_year_value = yearly_totals[str(first_year)]
latest_year_value = yearly_totals[str(latest_year)]
percent_change = ((latest_year_value - first_year_value) / first_year_value) * 100

print(f"\nProsentvis endring i Telemark fra {first_year} til {latest_year}:")
print(f"{percent_change:.1f}%")


##################### Analyse av totalt antall virksomheter per år - Landet #####################

# Filtrer ut "Alle næringer" (NACE2007 kode "00-99") og beregn totalt antall virksomheter per år
yearly_totals_landet = df_virksomheter_landet.groupby('år')['value'].sum()
print("\nTotalt antall virksomheter i Norge per år:")
print(yearly_totals_landet)

# Beregn prosentvis endring fra 2009 til siste år
first_year_landet = 2009
latest_year_landet = yearly_totals_landet.index.max()
first_year_value_landet = yearly_totals_landet[str(first_year_landet)]
latest_year_value_landet = yearly_totals_landet[str(latest_year_landet)]
percent_change_landet = ((latest_year_value_landet - first_year_value_landet) / first_year_value_landet) * 100

print(f"\nProsentvis endring i Norge fra {first_year_landet} til {latest_year_landet}:")
print(f"{percent_change_landet:.1f}%")


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "virksomheter.csv"
task_name = "Arbeid og naeringsliv - Virksomheter"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_virksomheter, file_name, github_folder, temp_folder, keepcsv=True)

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
