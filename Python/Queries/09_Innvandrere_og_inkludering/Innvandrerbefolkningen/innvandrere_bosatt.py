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
POST_URL = "https://data.ssb.no/api/v0/no/table/09817/"

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
                    "4036",
                ],
            },
        },
        {"code": "InnvandrKat", "selection": {"filter": "item", "values": ["B"]}},
        {"code": "Landbakgrunn", "selection": {"filter": "item", "values": ["999"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["AndelBefolkning"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
}

try:
    df = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Bosatt",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df.head()
# df.info()

################# Bearbeiding datasett #################

# Select year from the column "år"
most_recent_year = df["år"].max()

# Keep only the columns "region" and "value"
df = df[["region", "value"]]

# Rename headers to "Kommune" and "Andel"
df.columns = ["Kommune", "Andel"]

# Round the values in the column to 0 decimals, and convert to integer
df["Andel"] = df["Andel"].astype(int)

# Create a third column identical to "Kommune", but name the column "Labels"
df["Label"] = df["Kommune"]

# Add latest year from dataset
df.columns = [
    "Kommune",
    f"Andel bosatt {most_recent_year}",
    "Label",
]

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_innvandrere_bosatt.csv"
github_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"
temp_folder = os.environ.get("TEMP_FOLDER")

""" compare_to_github(
    df, file_name, github_folder, temp_folder
)  # <--- Endre navn på dataframe her! """

# Call the function and get the "New Data" status
is_new_data = compare_to_github(df, file_name, github_folder, temp_folder)

# Write the "New Data" status to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    if is_new_data:
        log_file.write(f"{file_name},New Data,Yes\n")
    else:
        log_file.write(f"{file_name},New Data,No\n")

##################### Remove temporary local files #####################

#delete_files_in_temp_folder()
