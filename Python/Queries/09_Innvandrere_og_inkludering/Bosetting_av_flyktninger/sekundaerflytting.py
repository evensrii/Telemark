import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data, delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/k/flyktning_botid_flytting"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Sekundærflytting",
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

# df.info()
df.head()

# Format "År" as datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Filter the most recent year
df = df[df["År"] == df["År"].max()]
print(f"Most recent year in table is {df['År'].max()}")

# Convert the column "Kommunenummer" to a string with 4 digits
df["Kommunenummer"] = (
    df["Kommunenummer"].astype(str).str.pad(width=4, side="left", fillchar="0")
)

# Filter rows where "Enhet" is "Personer"
df = df[df["Enhet"] == "Personer"]

# Filter rows where "Sekundærflytting" is "Bosatt her", "Flyttet hit" or "Uoppgitt".
df = df[df["Sekundærflytting"].isin(["Bosatt her", "Flyttet hit", "Uoppgitt"])]

# Filter rows where "Botid i Norge" is "Alle"
df = df[df["Botid i Norge"] == "Alle"]

# Remove colums "Enhet"
df = df.drop(columns=["Enhet"])

# Dictionary for innfylling av manglende kommunenavn, samt filtrering av datasettet

kommuner_telemark = {
    "3806": "Porsgrunn",
    "3807": "Skien",
    "3808": "Notodden",
    "3812": "Siljan",
    "3813": "Bamble",
    "3814": "Kragerø",
    "3815": "Drangedal",
    "3816": "Nome",
    "3817": "Midt-Telemark",
    "3818": "Tinn",
    "3819": "Hjartdal",
    "3820": "Seljord",
    "3821": "Kviteseid",
    "3822": "Nissedal",
    "3823": "Fyresdal",
    "3824": "Tokke",
    "3825": "Vinje",
    "4001": "Porsgrunn",
    "4003": "Skien",
    "4005": "Notodden",
    "4010": "Siljan",
    "4012": "Bamble",
    "4014": "Kragerø",
    "4016": "Drangedal",
    "4018": "Nome",
    "4020": "Midt-Telemark",
    "4022": "Seljord",
    "4024": "Hjartdal",
    "4026": "Tinn",
    "4028": "Kviteseid",
    "4030": "Nissedal",
    "4032": "Fyresdal",
    "4034": "Tokke",
    "4036": "Vinje",
}

## Filtrering av rader hvor "Kommunenummer" er i kommuner_telemark.keys()
df_kommuner = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn

# Map the dictionary to the DataFrame using the 'Kommunenummer' column
# This creates a new Series that can be used to fill missing values in 'Kommune'
df_kommuner["Kommune"] = df_kommuner["Kommune"].fillna(
    df_kommuner["Kommunenummer"].map(kommuner_telemark)
)

# dtale.show(df_kommuner, open_browser=True)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "sekundærflytting.csv"
task_name = "Innvandrere - Sekundaerflytting"
github_folder = "Data/09_Innvandrere og inkludering/Bosetting av flyktninger"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_kommuner, file_name, github_folder, temp_folder, keepcsv=True)

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