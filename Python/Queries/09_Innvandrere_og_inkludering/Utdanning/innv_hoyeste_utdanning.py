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


######## Per september 2024 vises VTFK-tall for 2022, men sørger for at scriptet også fanger opp Telemarkstallene (2024) når de kommer. ######################################################

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = "https://app-simapi-prod.azurewebsites.net/download_csv/f/utdanningsniva"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_fylker = fetch_data(
        url=url_fylker,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Høyeste utdanning fylker",
        response_type="csv",  # The expected response type, either 'json' or 'csv'.
        delimiter=";",  # The delimiter for CSV data (default: ';').
        encoding="ISO-8859-1",  # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


# df_fylker.info()
df_fylker.head()

# Set data types
df_fylker["Fylkesnummer"] = df_fylker["Fylkesnummer"].astype(str).str.zfill(2)
df_fylker["År"] = pd.to_datetime(df_fylker["År"], format="%Y")

# Fange opp VTFK, og etterhvert også TFK-tallene.
df_fylker = df_fylker[df_fylker["Fylkesnummer"].isin(["38", "40"])]
# dtale.show(df_fylker, open_browser=True)

# Get the most recent year in the dataset
most_recent_year = df_fylker["År"].max()

# Filter rows based on the most recent year
df_fylker = df_fylker[df_fylker["År"] == most_recent_year]

# Filter based on other criteria
df_fylker = df_fylker[df_fylker["Enhet"] == "Prosent"]
df_fylker = df_fylker[df_fylker["Bakgrunn"] != "Hele befolkningen"]
df_fylker = df_fylker[df_fylker["Kjønn"] == "Alle"]
df_fylker = df_fylker[df_fylker["Utdanningsnivå"] != "Alle"]

# Remove columns
df_fylker = df_fylker.drop(columns=["Enhet", "Fylke", "Kjønn"])

# Rename columns
df_fylker = df_fylker.rename(columns={"Antall": "Andel"})

# Reset index
df_fylker = df_fylker.reset_index(drop=True)


# Pivotere til rett format
df_fylker_pivot = df_fylker.pivot_table(
    index=["Bakgrunn", "År", "Fylkesnummer"], columns="Utdanningsnivå", values="Andel"
).reset_index()

# Flatten the MultiIndex columns
df_fylker_pivot.columns = [col for col in df_fylker_pivot.columns]

# Move columns "År" and "Fylkesnummer" to the end
df_fylker_pivot = df_fylker_pivot[
    [
        "Bakgrunn",
        "Grunnskole",
        "Videregående skole",
        "Universitet og høgskole",
        "Ingen utdanning",
        "Uoppgitt",
    ]
]

df_fylker_pivot.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "høyeste_utdanning_innv.csv"
github_folder = "Data/09_Innvandrere og inkludering/Utdanningsnivå Telemark"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = compare_to_github(df_fylker_pivot, file_name, github_folder, temp_folder)

# Write the "New Data" status to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    if is_new_data:
        log_file.write(f"{file_name},New Data,Yes\n")
    else:
        log_file.write(f"{file_name},New Data,No\n")

##################### Remove temporary local files #####################

delete_files_in_temp_folder()
