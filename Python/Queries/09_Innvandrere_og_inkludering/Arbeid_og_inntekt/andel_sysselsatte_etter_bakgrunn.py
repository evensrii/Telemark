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

# Alle kommuner, siste ti år (dvs. "top 10")


## MANGLER TALL FOR KUN TELEMARK I ÅRENE 2020-2023!
## SJEKKER PÅ NYTT NÅR 2024-TALLENE ER KLARE


""" # Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/11607/"

payload = {
    "query": [
        {
            "code": "Region",
            "selection": {"filter": "vs:FylkerAlle", "values": ["40", "38", "08"]},
        },
        {"code": "Alder", "selection": {"filter": "item", "values": ["15-74"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {
            "code": "Landbakgrunn",
            "selection": {"filter": "item", "values": ["abc", "ddd", "eee"]},
        },
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Sysselsatte2"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["10"]}},
    ],
    "response": {"format": "json-stat2"},
}

try:
    df = fetch_data(
        url=POST_URL,
        payload=payload, #The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Query name",
        response_type="csv", # The expected response type, either 'json' or 'csv'.
        delimiter=";", # The delimiter for CSV data (default: ';').
        encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df.head()
df.info()

# Convert "år" to datetime
df["år"] = pd.to_datetime(df["år"])

df["kjønn"].unique()
df["landbakgrunn"].unique()

# Rename values in column "landbakgrunn"
df["landbakgrunn"] = df["landbakgrunn"].replace(
    {
        "Norden utenom Norge, EU/EFTA,  Storbritannia, USA, Canada, Australia, New Zealand": "Gruppe 1-land (EU, Storbritannia, USA ++)",
        "Europa utenom EU/EFTA og Storbritannia, Afrika, Asia, Amerika utenom USA og Canada, Oseania utenom Australia og NZ, polare områder": "Gruppe 2-land (Asia, Afrika, Latin-Amerika ++)",
    }
)

# Remove columns "alder", "kjønn" and "statistikkvariabel"
df = df.drop(columns=["alder", "kjønn", "statistikkvariabel"])

# Remove rows with NaN values
df = df.dropna()


# Rename columns
df_fylker = df_fylker.rename(columns={"Antall": "Andel"})

# Reset index
df_fylker = df_fylker.reset_index(drop=True)
 """

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_sysselsatte_innvandrere.csv"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = compare_to_github(df, file_name, github_folder, temp_folder)

# Write the "New Data" status to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    if is_new_data:
        log_file.write(f"{file_name},New Data,Yes\n")
    else:
        log_file.write(f"{file_name},New Data,No\n")

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
