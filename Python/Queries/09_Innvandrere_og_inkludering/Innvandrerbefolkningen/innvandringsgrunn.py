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

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

# Finner URL vha. "Inspiser side" og fane "Network"
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/befolkning_innvandringsgrunn"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Innvandringsgrunn",
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

# Print the unique values in the column "Kommunenummer"
# print(df["Kommunenummer"].unique())
# df.dtypes

# Convert the column "Kommunenummer" to a string with 4 digits
df["Kommunenummer"] = df["Kommunenummer"].astype(str).str.pad(width=4, fillchar="0")

# Dictionary for innfylling av manglende kommunenavn, samt filtrering av datasettet

kommuner_telemark = {
    "0805": "Porsgrunn",
    "0806": "Skien",
    "0807": "Notodden",
    "0811": "Siljan",
    "0814": "Bamble",
    "0815": "Kragerø",
    "0817": "Drangedal",
    "0819": "Nome",
    "0821": "Bø (Telemark)",
    "0822": "Sauherad",
    "0826": "Tinn",
    "0827": "Hjartdal",
    "0828": "Seljord",
    "0829": "Kviteseid",
    "0830": "Nissedal",
    "0831": "Fyresdal",
    "0833": "Tokke",
    "0834": "Vinje",
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
df = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn
df["Kommune"] = df["Kommune"].fillna(df["Kommunenummer"].map(kommuner_telemark))

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "innvandringsgrunn_telemark.csv"
task_name = "Innvandrere - Innvandringsgrunn"
github_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"
temp_folder = os.environ.get("TEMP_FOLDER")

value_columns = ["Antall"]
ignore_patterns = [col for col in df.columns if col != 'Antall']

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns, ignore_patterns=ignore_patterns)

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