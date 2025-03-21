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


#### SPØRRING FOR Å FINNE UT HVILKET ÅR SOM ER NYEST I DATASETTET

# Spørring for å hente ut data fra SSB
payload_sysselsatte_siste_aar = {
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
          "01-03"
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

try:
    df_sysselsatte_siste_aar = fetch_data(
        url=POST_URL,
        payload=payload_sysselsatte_siste_aar,
        error_messages=error_messages,
        query_name="Sysselsatte, siste år",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Get most recent year
most_recent_year = df_sysselsatte_siste_aar['år'].iloc[0]

# Create a list of years from 2016 until most_recent_year, years as strings enclosed in ""
years = [str(year) for year in range(2016, int(most_recent_year) + 1)]



################## ARBEIDSPLASSER I KOMMUNER (Dvs. sysselsatte etter arbeidssted)

# Spørring for å hente ut data fra SSB
payload_sysselsatte = {
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
          "90-99",
          "00"
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
        "filter": "item",
        "values": years
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_sysselsatte = fetch_data(
        url=POST_URL,
        payload=payload_sysselsatte,
        error_messages=error_messages,
        query_name="Sysselsatte, kommuner",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Sum values for year
sysselsatte_per_year = df_sysselsatte.groupby(['region', 'næring (SN2007)', 'år'])['value'].sum().reset_index()

# Group by year and sum value
sysselsatte_per_year_sum = sysselsatte_per_year.groupby(['år'])['value'].sum().reset_index()


print("\nSysselsatte per år:\n")
print(sysselsatte_per_year_sum)

# Extract latest year
latest_year = sysselsatte_per_year_sum.iloc[-1]['år']

# Calculate growth in employment from 2016 until latest year in table
growth = (sysselsatte_per_year_sum.iloc[-1]['value'] - sysselsatte_per_year_sum.iloc[0]['value'])

print(f"Økning i antall arbeidsplasser (sysselsatte med arbeidssted i Telemark) fra 2016 til {latest_year}: {growth}")



##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "endring_i_arbeidsplasser_over_tid.csv"
task_name = "Arbeid og naeringsliv - Endring arbeidsplasser over tid"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Næringsstruktur og arbeidsplasser"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_sysselsatte, file_name, github_folder, temp_folder, keepcsv=True)

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
