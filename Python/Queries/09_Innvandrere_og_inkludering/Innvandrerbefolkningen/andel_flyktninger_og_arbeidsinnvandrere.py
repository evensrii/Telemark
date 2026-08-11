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
url_kommune = "https://app-simapi-prod.azurewebsites.net/download_csv/k/befolkning_innvandringsgrunn"
url_fylke = "https://app-simapi-prod.azurewebsites.net/download_csv/f/befolkning_innvandringsgrunn"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url_kommune,
        payload=None,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Flyktninger og arbeidsinnvandrere kommuner",
        response_type="csv",  # The expected response type, either 'json' or 'csv'.
        delimiter=";",  # The delimiter for CSV data (default: ';').
        encoding="ISO-8859-1",  # The encoding for CSV data (default: 'ISO-8859-1').
    )
    df_fylke_raw = fetch_data(
        url=url_fylke,
        payload=None,
        error_messages=error_messages,
        query_name="Flyktninger og arbeidsinnvandrere fylke og landet",
        response_type="csv",
        delimiter=";",
        encoding="ISO-8859-1",
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
print(f"Selected kommune data from year {df['År'].max()}")


def calculate_pivot(df, region_col):
    """Given a filtered dataframe (one region column, Innvandringsgrunn,
    Antall), return a pivoted dataframe with one row per region and the
    percentage of 'Arbeidsinnvandrere' and 'Flyktninger og deres
    familieinnvandrede' relative to 'Alle'."""
    df_pivot = df.pivot(
        index=region_col, columns="Innvandringsgrunn", values="Antall"
    ).reset_index()

    # Calculate the percentage for each category relative to 'Alle'
    percentage_columns = df_pivot.columns[
        2:
    ]  # Skip region_col, and 'Alle'
    for column in percentage_columns:
        df_pivot[column] = ((df_pivot[column] / df_pivot["Alle"]) * 100).astype(
            int
        )  # Convert to percentage and format as integer

    df_pivot.columns.name = None
    return df_pivot


# Create a table with a row for each "Kommune", and a column for each "Innvandringsgrunn"
df_pivot_kommune = calculate_pivot(df, "Kommune")
df_pivot_kommune.rename(columns={"Kommune": "Region"}, inplace=True)

# ============================================================
# Behandle fylke- og landstall (Norge + Telemark)
# ============================================================

df_fylke = df_fylke_raw.copy()

columns_to_convert_fylke = ["Fylke", "Kjønn", "Innvandringsgrunn", "Enhet"]
for column in columns_to_convert_fylke:
    df_fylke[column] = df_fylke[column].astype("category")

df_fylke["År"] = pd.to_datetime(df_fylke["År"], format="%Y")

# Filtrere kun personer og "Alle" kjønn
df_fylke = df_fylke.query("Enhet == 'Personer'")
df_fylke = df_fylke.query("Kjønn == 'Alle'")

# Konverter "Fylkesnummer" til string med 2 siffer
df_fylke["Fylkesnummer"] = df_fylke["Fylkesnummer"].astype(str).str.pad(width=2, fillchar="0")

# Behold kun Norge ("00") og Telemark ("40")
df_fylke = df_fylke[df_fylke["Fylkesnummer"].isin(["00", "40"])]

# Fjerne parentetisk suffiks fra fylkesnavn, f.eks. "Telemark (t.o.m. 2019, f.o.m. 2024)" -> "Telemark"
df_fylke["Fylke"] = df_fylke["Fylke"].astype("object").str.replace(r"\s*\(.*\)$", "", regex=True)

# Filter only the most recent data (last year available)
most_recent_year_fylke = df_fylke["År"].max()
df_fylke = df_fylke.query("År == @most_recent_year_fylke")
print(f"Selected fylke/landet data from year {df_fylke['År'].max()}")

if most_recent_year != most_recent_year_fylke:
    print(
        "Warning: Year differs between kommune- and fylke/landstall "
        f"(kommuner: {most_recent_year.year}, fylke/landet: {most_recent_year_fylke.year})."
    )

df_pivot_fylke = calculate_pivot(df_fylke, "Fylke")
df_pivot_fylke.rename(columns={"Fylke": "Region"}, inplace=True)

# ============================================================
# Kombiner kommuner, fylke og landet til én Everviz-tabell
# per kategori
# ============================================================

df_pivot = pd.concat([df_pivot_kommune, df_pivot_fylke], ignore_index=True)

# Split the data into two dataframes, one for 'Arbeidsinnvandrere' and one for 'Flyktninger og deres familieinnvandrede'
df_pivot_arbeidsinnvandrere = df_pivot[["Region", "Arbeidsinnvandrere"]].copy()
df_pivot_flyktninger = df_pivot[["Region", "Flyktninger og deres familieinnvandrede"]].copy()

# Add a label column "Label" with the values from column "Region"
df_pivot_arbeidsinnvandrere["Label"] = df_pivot_arbeidsinnvandrere["Region"]
df_pivot_flyktninger["Label"] = df_pivot_flyktninger["Region"]

# Rename the columns to "Andel flyktninger {most_recent_year.year}" and "Andel arbeidsinnvandrere {most_recent_year.year}"
df_pivot_arbeidsinnvandrere.columns = [
    "Region",
    f"Andel arbeidsinnvandrere {most_recent_year.year}",
    "Label",
]
df_pivot_flyktninger.columns = [
    "Region",
    f"Andel flyktninger {most_recent_year.year}",
    "Label",
]

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name1 = "andel_arbeidsinnvandrere.csv"
file_name2 = "andel_flyktninger.csv"
task_name = "Innvandrere - Flyktninger og arbeidsinnvandrere"
github_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"
temp_folder = os.environ.get("TEMP_FOLDER")

# Process both files and track their status
is_new_data1 = handle_output_data(df_pivot_arbeidsinnvandrere, file_name1, github_folder, temp_folder, keepcsv=True)
is_new_data2 = handle_output_data(df_pivot_flyktninger, file_name2, github_folder, temp_folder, keepcsv=True)

# Write a single status file that indicates if either file has new data
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format - set to "Yes" if either file has new data
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if (is_new_data1 or is_new_data2) else 'No'}\n")

# Output results for debugging/testing
if is_new_data1:
    print(f"New data detected in {file_name1} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name1}.")

if is_new_data2:
    print(f"New data detected in {file_name2} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name2}.")

print(f"New data status log written to {new_data_status_file}")
