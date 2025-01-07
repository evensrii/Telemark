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
from Helper_scripts.github_functions import handle_output_data

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

file_name = "andel_sysselsatte_innvandrere_etter_bakgrunn.csv"
task_name = "Innvandrerbefolkningen - Sysselsatte etter bakgrunn"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
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