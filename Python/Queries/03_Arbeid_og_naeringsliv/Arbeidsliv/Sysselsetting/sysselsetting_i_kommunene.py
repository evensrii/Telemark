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
POST_URL = "https://data.ssb.no/api/v0/no/table/06445/"

# First payload with new municipality codes
payload_new = {
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
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-74"
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

# Second payload with old municipality codes
payload_old = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:Komm2020",
        "values": [
          "3806",
          "3807",
          "3808",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-74"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2023"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

# Try the first payload
try:
    df = fetch_data(
        url=POST_URL,
        payload=payload_new,
        error_messages=error_messages,
        query_name="Sysselsetting i kommunene (new codes)",
        response_type="json"
    )
    
    # Check if all values are 0
    if df['value'].sum() == 0:
        print("No data found with new municipality codes, trying old codes...")
        df = fetch_data(
            url=POST_URL,
            payload=payload_old,
            error_messages=error_messages,
            query_name="Sysselsetting i kommunene (old codes)",
            response_type="json"
        )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print("\nFetched data:")
#print(df)

# Remove any "(YYYY-YYYY)" in column "region"
df["region"] = df["region"].str.replace(r'\s*\(\d{4}-\d{4}\)', '', regex=True)
df = df.rename(columns={"region": "Kommune"})

# Rename column "value" to "Andel sysselsatte ({year})", with year being the the top value in the "år" column.
most_recent_year = df["år"].max()
df = df.rename(columns={"value": f"Andel sysselsatte ({most_recent_year})"})

# Remove columns "alder", "statistikkvariabel" and "år".
df = df.drop(columns=["alder", "statistikkvariabel", "år"])

# Add a copy of the "Kommune" column, with the name "Label".
df["Label"] = df["Kommune"]
print(df)
print("\nData types:")
print(df.dtypes)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "sysselsetting_i_kommunene.csv"
task_name = "Arbeid og naeringsliv - Sysselsatte i kommunene"
github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/Sysselsetting"
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