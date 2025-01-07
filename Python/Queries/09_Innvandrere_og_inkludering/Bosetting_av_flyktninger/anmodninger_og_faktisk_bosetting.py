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

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/bosatt_anmodede"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Anmodninger og faktisk bosetting",
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
#df.head()

# print(df["Kommunenummer"].unique())

# Rename column "Anmodning, vedtak og faktisk bosetting" til "Kategori"
df = df.rename(columns={"Anmodning, vedtak og faktisk bosetting": "Kategori"})

# dtale.show(open_browser=True)

# Konverter "Kommunenummer" til string med 4 siffer
df["Kommunenummer"] = df["Kommunenummer"].astype(str).str.pad(width=4, fillchar="0")
#df.info()

# Konverter "Antall" til integer, og erstatt manglende verdier med NaN
df["Antall"] = pd.to_numeric(df["Antall"], errors="coerce")

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
    "0821": "Bø (Telemark)  (t.o.m. 2019)",
    "0822": "Sauherad (t.o.m. 2019)",
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

## Innfylling av manglende kommunenavn

# Map the dictionary to the DataFrame using the 'Kommunenummer' column
# This creates a new Series that can be used to fill missing values in 'Kommune'
df["Kommune"] = df["Kommune"].fillna(df["Kommunenummer"].map(kommuner_telemark))

# dtale.show(df, open_browser=True)
# 2024-tallene bruker fortsatt gammel kommunenummerering (VTFK)

## Filtrering av rader hvor "Kommunenummer" er i kommuner_telemark.keys()

df = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]
# dtale.show(df, open_browser=True)

# Filter only the enhet "Personer"
df = df.query("Enhet == 'Personer'")

# Drop the columns "Kommunenummer" and "Enhet"
df = df.drop(columns=["Enhet"])

# Format "År" as datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Keep only "År" >= 2012
# df = df.query("År >= '2012-01-01'")

# Fjerne kolonne "Kommunenummer"
df = df.drop(columns=["Kommunenummer"])

# Merge kommuner "Bø (Telemark) (t.o.m. 2019)" og "Sauherad (t.o.m. 2019)" til "Midt-Telemark"
df["Kommune"] = df["Kommune"].replace(
    {
        "Bø (Telemark)  (t.o.m. 2019)": "Midt-Telemark",
        "Sauherad (t.o.m. 2019)": "Midt-Telemark",
    }
)

# dtale.show(df, open_browser=True)

# Group by the specified columns and aggregate the 'Antall' column
df_aggregert = df.groupby(["Kommune", "År", "Kategori"], as_index=False)["Antall"].sum()
# dtale.show(df_aggregert, open_browser=True)

#print(df_aggregert[df_aggregert["Kommune"] == "Midt-Telemark"])

# Kontroll: Summere antall flyktninger per år og kategori
df_aggregert.groupby(["År", "Kategori"], as_index=False)["Antall"].sum()

# HAR MANUELT SUMMERT OPP ENKELTKOMMUNENE I TELEMARK HOS IMDI MED TALL FRA 2023 - DETTE STEMMER! :)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "anmodninger_og_faktisk_bosetting.csv"
task_name = "Innvandrere - Anmodninger og faktisk bosetting"
github_folder = "Data/09_Innvandrere og inkludering/Bosetting av flyktninger"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_aggregert, file_name, github_folder, temp_folder, keepcsv=True)

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