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
        query_name="Flyktninger og arbeidsinnvandrere",
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

## Datasjekk
# df.head()
# df.info()

# Print the unique values in the column "Kommunenummer"
# print(df["Kommunenummer"].unique())

# Convert the object columns into category
columns_to_convert = ["Kommune", "Kjønn", "Innvandringsgrunn", "Enhet"]

for column in columns_to_convert:
    df[column] = df[column].astype("category")

# Konvertere kolonne "År" til datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Filtrere kun personer
df = df.query("Enhet == 'Personer'")

# Konverter "Kommunenummer" til string med 4 siffer
df["Kommunenummer"] = df["Kommunenummer"].astype(str).str.pad(width=4, fillchar="0")

# Konverter "Kommune" til object
df["Kommune"] = df["Kommune"].astype("object")

# Filter ved hjelp av query basert på keys in kommuner_telemark

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
df = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn
df["Kommune"] = df["Kommune"].fillna(df["Kommunenummer"].map(kommuner_telemark))

# Filter only "Alle" in the "Kjønn" column
df = df.query("Kjønn == 'Alle'")

# Remove rows with "Alle" in the "Innvandringsgrunn" column
# df = df.query("Innvandringsgrunn != 'Alle'")

# Filter only the most recent data (last year available)
most_recent_year = df["År"].max()
df = df.query("År == @most_recent_year")
print(f"Selected data from year {df['År'].max()}")

# Create a table with a row for each "Kommune", and a column for each "Innvandringsgrunn"
df_pivot = df.pivot(
    index="Kommune", columns="Innvandringsgrunn", values="Antall"
).reset_index()

# Calculate the percentage for each category relative to 'Alle'
percentage_columns = df_pivot.columns[
    2:
]  # Skip 'Innvandringsgrunn', 'Kommune', and 'Alle'
for column in percentage_columns:
    df_pivot[column] = ((df_pivot[column] / df_pivot["Alle"]) * 100).astype(
        int
    )  # Convert to percentage and format as integer

# Remove the "Innvandringsgrunn" column
df_pivot.columns.name = None

# Split the data into two dataframes, one for 'Arbeidsinnvandrere' and one for 'Flyktninger og deres familieinnvandrede'
df_pivot_arbeidsinnvandrere = df_pivot[["Kommune", "Arbeidsinnvandrere"]]
df_pivot_flyktninger = df_pivot[["Kommune", "Flyktninger og deres familieinnvandrede"]]

# Add a label column "Label" with the values from column "Kommune"
df_pivot_arbeidsinnvandrere["Label"] = df_pivot_arbeidsinnvandrere["Kommune"]
df_pivot_flyktninger["Label"] = df_pivot_flyktninger["Kommune"]

# Rename the columns to "Andel flyktninger {most_recent_year.year}" and "Andel arbeidsinnvandrere {most_recent_year.year}"
df_pivot_arbeidsinnvandrere.columns = [
    "Kommune",
    f"Andel arbeidsinnvandrere {most_recent_year.year}",
    "Label",
]
df_pivot_flyktninger.columns = [
    "Kommune",
    f"Andel flyktninger {most_recent_year.year}",
    "Label",
]

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name1 = "andel_arbeidsinnvandrere.csv"
file_name2 = "andel_flyktninger.csv"
github_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"
temp_folder = os.environ.get("TEMP_FOLDER")

# Track if any file has new data
is_any_new_data = False

# Call the function and get the "New Data" status for file 1
is_new_data1 = compare_to_github(
    df_pivot_arbeidsinnvandrere, file_name1, github_folder, temp_folder
)
if is_new_data1:
    is_any_new_data = True

# Call the function and get the "New Data" status for file 2
is_new_data2 = compare_to_github(
    df_pivot_flyktninger, file_name2, github_folder, temp_folder
)
if is_new_data2:
    is_any_new_data = True

# Write the "New Data" status for the script to a log file
with open("new_data_status.log", "w", encoding="utf-8") as log_file:
    if is_any_new_data:
        log_file.write(
            f"Innvandrere - Arbeidsinnvandrere og Flyktninger,New Data,Yes\n"
        )
    else:
        log_file.write(f"Innvandrere - Arbeidsinnvandrere og Flyktninger,New Data,No\n")


##################### Remove temporary local files #####################

delete_files_in_temp_folder()
