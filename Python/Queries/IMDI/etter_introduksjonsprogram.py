import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob
import dtale

# Legge til parent directory i sys.path for å importere github_functions.py
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)


# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/f/intro_status_arbutd_avslutta"

# Make a GET request to the URL to download the file
response = requests.get(url)

# Hente ut innhold (data)
url_content = response.content

if response.status_code == 200:

    df = pd.read_csv(BytesIO(url_content), delimiter=";", encoding="ISO-8859-1")

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df.info()
df.head()

# Format "År" as datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Filter the most recent year
df = df[df["År"] == df["År"].max()]
print(
    f"Statistikken viser andelen i arbeid eller utdanning i {df['År'].max()} som avsluttet eller avbrøt programmet i {df['År'].max() - pd.DateOffset(years=+1)}"
)


# Convert the column "Fylkesnummer" to a string with 2 digits
df["Fylkesnummer"] = (
    df["Fylkesnummer"].astype(str).str.pad(width=2, side="left", fillchar="0")
)

print(df["Fylkesnummer"].unique())

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
print(df_filtered)

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

print(df_fylker["Fylke"].unique())

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

#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"etter_introduksjonsprogram.csv"
df_fylker.to_csv(
    (f"../Temp/{csv_file_name}"), index=False
)  # Relativt til dette scriptet.


##################### Opplasting til Github #####################

# Legge til parent directory i sys.path for å importere github_functions.py
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../Temp/{csv_file_name}"
destination_folder = "Data/09_Innvandrere og inkludering/Introduksjonsprogrammet"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file, destination_folder, github_repo, git_branch)

##################### Remove temporary files #####################

# Delete files in folder using glob


def delete_files_in_folder(folder_path):
    # Construct the path pattern to match all files in the folder
    files = glob.glob(os.path.join(folder_path, "*"))

    # Iterate over the list of files and delete each one
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")


# Specify the folder path
folder_path = "../Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
