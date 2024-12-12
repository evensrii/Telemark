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

################# Spørring #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/03440/"

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
        query_name="Andel jegere",
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


# Drop kolonner "region" og "statistikkvariabel"
df = df.drop(columns=["region", "statistikkvariabel"])

# Rename og inkludere "siste_år" i colonnenavnet "Antall aktive jegere"
siste_år = df["intervall (år)"].max()
df.columns = ["Kjønn", "Alder", "Jaktår", f"Antall aktive jegere {siste_år}"]

# Drop kolonne "Jaktår"
df = df.drop(columns=["Jaktår"])

# Print the first value in the column "år"
print(f"Her hentes tallene for jaktåret {siste_år}")

# Pivoter på kolonne "Kjønn"
df_pivot = df.pivot(
    index="Alder", columns="Kjønn", values=f"Antall aktive jegere {siste_år}"
).reset_index()

df_pivot.columns.name = None  # Remove the multi-index name for columns

## Skal ha aldersfordeling i prosent for hvert kjønn

# Calculate the total sum of both 'Kvinner' and 'Menn' across all rows
total_kvinner = df_pivot["Kvinner"].sum()
total_menn = df_pivot["Menn"].sum()

# Create the 'Andel kvinner' and 'Andel menn' columns as percentages of the total within each gender
df_pivot["Andel kvinner"] = (df_pivot["Kvinner"] / total_kvinner) * 100
df_pivot["Andel menn"] = (df_pivot["Menn"] / total_menn) * 100

# Set the row order as "Under 20 år", "20-29 år", "30-39 år", "40-49 år", "50-59 år", "60-69 år" and "70 år eller eldre"
df_pivot = df_pivot.reindex([6, 0, 1, 2, 3, 4, 5]).reset_index(drop=True)

# Drop columns Kvinner og Menn
df_pivot = df_pivot.drop(columns=["Kvinner", "Menn"])

# Round values in andel to 1 decimals
df_pivot["Andel kvinner"] = df_pivot["Andel kvinner"].round(1)
df_pivot["Andel menn"] = df_pivot["Andel menn"].round(1)

# Set column names as "Alder", "Kvinner" and "Menn"
df_pivot.columns = [f"Alder ({siste_år})", "Kvinner", "Menn"]

# Round values in "Kvinner" and "Menn" to 0 decimals
df_pivot["Kvinner"] = df_pivot["Kvinner"].astype(int)
df_pivot["Menn"] = df_pivot["Menn"].astype(int)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_jegere.csv"
github_folder = "Data/07_Idrett_friluftsliv_og_frivillighet/Friluftsliv"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = compare_to_github(df_pivot, file_name, github_folder, temp_folder)

# Write the "New Data" status to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    if is_new_data:
        log_file.write(f"{file_name},New Data,Yes\n")
    else:
        log_file.write(f"{file_name},New Data,No\n")

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
