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

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

########### Fylke Telemark 2014 - 2019 + 2024 (datasett "Alle fylker")

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/f/enslige_mindrearige"
)


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_fylker = fetch_data(
        url=url_fylker,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Fylker",
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

# print(df_fylker["Fylkesnummer"].unique())

# Filter columns where fylkesnummer is 8 (Telemark)
df_fylker = df_fylker[df_fylker["Fylkesnummer"].isin([8, 40])]
df_fylker = df_fylker[df_fylker["Enhet"] == "Personer"]
df_fylker = df_fylker[
    df_fylker["Anmodning, vedtak og faktisk bosetting"].isin(
        ["Anmodning om bosetting", "Faktisk bosetting"]
    )
]
df_fylker = df_fylker.drop(columns=["Enhet", "Fylkesnummer"])

# Rename column "Anmodning, vedtak og faktisk bosetting" til "Kategori"
df_fylker = df_fylker.rename(
    columns={"Anmodning, vedtak og faktisk bosetting": "Kategori"}
)
df_fylker = df_fylker.sort_values(by=["Kategori", "År"], ascending=True)

# dtale.show(open_browser=True)

# Konverter "Antall" til integer, og erstatt manglende verdier med NaN
df_fylker["Antall"] = pd.to_numeric(df_fylker["Antall"], errors="coerce")

# Format "År" as datetime
df_fylker["År"] = pd.to_datetime(df_fylker["År"], format="%Y")

# Remove columns "Fylke"
df_fylker = df_fylker.drop(columns=["Fylke"])

# Reset index
df_fylker = df_fylker.reset_index(drop=True)

########### Kommuner Telemark 2020 - 2023 (datasett "Alle kommuner", må summere opp til Telemark fylke)

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_kommuner = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/k/enslige_mindrearige"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_kommuner = fetch_data(
        url=url_kommuner,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Kommuner",
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

# df_kommuner.info()
df_kommuner.head()

# Filter columns where "År" is either 2020, 2021, 2022 or 2023
df_kommuner = df_kommuner[df_kommuner["År"].isin([2020, 2021, 2022, 2023])]

# Convert the column "Kommunenummer" to a string with 4 digits
df_kommuner["Kommunenummer"] = (
    df_kommuner["Kommunenummer"].astype(str).str.pad(width=4, side="left", fillchar="0")
)

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
}

## Innfylling av manglende kommunenavn

# Map the dictionary to the DataFrame using the 'Kommunenummer' column
# This creates a new Series that can be used to fill missing values in 'Kommune'
# df_kommuner["Kommune"] = df_kommuner["Kommune"].fillna(
#    df_kommuner["Kommunenummer"].map(kommuner_telemark)
# )

# dtale.show(df_kommuner, open_browser=True)

## Filtrering av rader hvor "Kommunenummer" er i kommuner_telemark.keys()

df_kommuner = df_kommuner[df_kommuner["Kommunenummer"].isin(kommuner_telemark.keys())]
# dtale.show(df_kommuner, open_browser=True)

# Filter only the enhet "Personer"
df_kommuner = df_kommuner.query("Enhet == 'Personer'")

# Drop the columns "Kommunenummer" and "Enhet"
df_kommuner = df_kommuner.drop(columns=["Enhet", "Kommunenummer"])

# Rename column "Anmodning, vedtak og faktisk bosetting" til "Kategori"
df_kommuner = df_kommuner.rename(
    columns={"Anmodning, vedtak og faktisk bosetting": "Kategori"}
)

# Remove rows where kategori is "Vedtak om bosetting" or "Opprinnelig anmodning"
df_kommuner = df_kommuner[df_kommuner["Kategori"] != "Vedtak om bosetting"]
df_kommuner = df_kommuner[df_kommuner["Kategori"] != "Opprinnelig anmodning"]

# dtale.show(df_kommuner, open_browser=True)

# Format "År" as datetime
df_kommuner["År"] = pd.to_datetime(df_kommuner["År"], format="%Y")

# Convert the column "Antall" to a numeric data type
df_kommuner["Antall"] = pd.to_numeric(df_kommuner["Antall"], errors="coerce")

# Replace NaN values in "Antall" with 0
df_kommuner["Antall"] = df_kommuner["Antall"].fillna(0)

# Group by the specified columns and aggregate the 'Antall' column
df_aggregert_fylke = df_kommuner.groupby(["År", "Kategori"], as_index=False)[
    "Antall"
].sum()

# Set "Kategori as first column"
df_aggregert_fylke = df_aggregert_fylke[["Kategori", "År", "Antall"]]


########## Merge df_fylker (fylkesdata) and df_aggregert_fylke (kommunedata) ##########

# Merge df_aggregert_fylke to df_fylker
df_telemark = pd.concat([df_fylker, df_aggregert_fylke], ignore_index=True)

# Sort by "Kategori" and "År"
df_telemark = df_telemark.sort_values(by=["Kategori", "År"], ascending=True)

# Pivot and reset index (we are not exporting the index)
df_telemark = df_telemark.pivot(
    index="År", columns="Kategori", values="Antall"
).reset_index()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "bosetting_enslige_mindreårige.csv"
task_name = "Innvandrere - Enslige mindreaarige"
github_folder = "Data/09_Innvandrere og inkludering/Bosetting av flyktninger"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_telemark, 
    file_name, 
    github_folder, 
    temp_folder, 
    keepcsv=True,
    value_columns=['Anmodning om bosetting']  # Only compare this column
)

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