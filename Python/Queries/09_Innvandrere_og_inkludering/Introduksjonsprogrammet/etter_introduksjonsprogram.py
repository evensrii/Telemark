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

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/f/intro_status_arbutd_avslutta"


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Introprogram",
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

# Fetch the most recent year in the dataframe
most_recent_year = str(df["År"].max().year)

# Filter the most recent year
df = df[df["År"] == df["År"].max()]
print(
    f"Statistikken viser andelen i arbeid eller utdanning i {df['År'].max()} som avsluttet eller avbrøt programmet i {df['År'].max() - pd.DateOffset(years=+1)}"
)


# Convert the column "Fylkesnummer" to a string with 2 digits
df["Fylkesnummer"] = (
    df["Fylkesnummer"].astype(str).str.pad(width=2, side="left", fillchar="0")
)

# print(df["Fylkesnummer"].unique())

# Apply the filters
df_filtered = df[
    (df["Kjønn"] == "Alle")
    & (
        df["Avslutningsår for introduksjonsprogrammet"]
        == "Avsluttet eller avbrutt intro for ett år siden"
    )
    & (
        df["Arbeidssituasjon etter avsluttet/avbrutt introduksjonsprogram"]
        == "I arbeid eller utdanning"
    )
    & (df["Enhet"] == "Prosent")
]

# Display the filtered DataFrame
# print(df_filtered)

fylker = {
    "03": "Oslo",
    "11": "Rogaland",
    "15": "Møre og Romsdal",
    "18": "Nordland",
    "30": "Viken",
    "34": "Innlandet",
    "38": "Vestfold og Telemark",
    "42": "Agder",
    "46": "Vestland",
    "50": "Trøndelag",
    "54": "Troms og Finnmark",
    "11": "Rogaland",
    "15": "Møre og Romsdal",
    "18": "Nordland",
    "31": "Østfold",
    "32": "Akershus",
    "33": "Buskerud",
    "34": "Innlandet",
    "39": "Vestfold",
    "40": "Telemark",
    "42": "Agder",
    "46": "Vestland",
    "50": "Trøndelag",
    "55": "Troms",
    "56": "Finnmark",
    "99": "Uoppgitt",
    "00": "Norge",
}

## Filtrering av rader hvor "Fylkesnummer" er i fylker.keys()
df_fylker = df_filtered[df_filtered["Fylkesnummer"].isin(fylker.keys())]

# print(df_fylker["Fylke"].unique())

rename_fylker = {
    "Vestfold (t.o.m. 2019, f.o.m. 2024)": "Vestfold",
    "Rogaland": "Rogaland",
    "Trøndelag (f.o.m. 2018)": "Trøndelag",
    "Vestland (f.o.m. 2020)": "Vestland",
    "Finnmark (t.o.m. 2019, f.o.m. 2024)": "Finnmark",
    "Østfold (t.o.m. 2019, f.o.m. 2024)": "Østfold",
    "Agder (f.o.m. 2020)": "Agder",
    "Telemark (t.o.m. 2019, f.o.m. 2024)": "Telemark",
    "Oslo": "Oslo",
    "Troms og Finnmark (f.o.m. 2020, t.o.m. 2023)": "Troms og Finnmark",
    "Nordland": "Nordland",
    "Buskerud (t.o.m. 2019, f.o.m. 2024)": "Buskerud",
    "Møre og Romsdal": "Møre og Romsdal",
    "Norge": "Norge",
    "Innlandet (f.o.m. 2020)": "Innlandet",
    "Viken (f.o.m. 2020, t.o.m. 2023)": "Viken",
    "Ukjent fylke": "Ukjent fylke",
    "Akershus (t.o.m. 2019, f.o.m. 2024)": "Akershus",
    "Troms (t.o.m. 2019, f.o.m. 2024)": "Troms",
    "Vestfold og Telemark (f.o.m. 2020, t.o.m. 2023)": "Vestfold og Telemark",
}

## Gi nye, ryddigere navn til fylkene
df_fylker["Fylke"] = df_fylker["Fylke"].replace(rename_fylker)

## Remove rows where "Antall" is 0 (i praksis fylker som ikke eksisterer)
df_fylker = df_fylker[df_fylker["Antall"] != "0"]
# dtale.show(df_fylker, open_browser=True)

## Rename column "Antall" to "Andel"
df_fylker = df_fylker.rename(columns={"Antall": "Andel"})

## Keep only columns Fylke and Antall, sort by Antall, descending
df_fylker = df_fylker[["Fylke", "Andel"]].sort_values(by="Andel", ascending=False)

## Rename columns to "Fylke" and "Andel {most_recent_year}"
df_fylker = df_fylker.rename(columns={"Andel": f"Andel {most_recent_year}"})

df_fylker.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "etter_introduksjonsprogram.csv"
task_name = "Innvandrere - Etter introduksjonsprogrammet"
github_folder = "Data/09_Innvandrere og inkludering/Introduksjonsprogrammet"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_fylker, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = file_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
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
