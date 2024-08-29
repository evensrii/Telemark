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

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

########### Fylke Telemark 2014 - 2019 + 2024 (datasett "Alle fylker")

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/f/enslige_mindrearige"
)

# Make a GET request to the URL to download the file
response = requests.get(url_fylker)

# Hente ut innhold (data)
url_fylker_content = response.content

if response.status_code == 200:

    df_fylker = pd.read_csv(
        BytesIO(url_fylker_content), delimiter=";", encoding="ISO-8859-1"
    )

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df_fylker.info()
df_fylker.head()

print(df_fylker["Fylkesnummer"].unique())

# Filter columns where fylkesnummer is 8 (Telemark)
df_fylker = df_fylker[df_fylker["Fylkesnummer"] == 8]
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

########### Kommuner Telemark 2020 - 2023 (datasett "Alle kommuner", må summere opp til Telemark fylke)

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_kommuner = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/k/enslige_mindrearige"
)

# Make a GET request to the URL to download the file
response = requests.get(url_kommuner)

# Hente ut innhold (data)
url_kommuner_content = response.content

if response.status_code == 200:

    df_kommuner = pd.read_csv(
        BytesIO(url_kommuner_content), delimiter=";", encoding="ISO-8859-1"
    )

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df_kommuner.info()
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

dtale.show(df_kommuner, open_browser=True)

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
dtale.show(df_aggregert_fylke, open_browser=True)

print(df_aggregert_fylke[df_aggregert_fylke["Kommune"] == "Midt-Telemark"])

# Kontroll: Summere antall flyktninger per år og kategori
df_aggregert_fylke.groupby(["År", "Kategori"], as_index=False)["Antall"].sum()

# HAR MANUELT SUMMERT OPP ENKELTKOMMUNENE I TELEMARK HOS IMDI MED TALL FRA 2023 - DETTE STEMMER

#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"anmodninger_og_faktisk_bosetting.csv"
df_aggregert_fylke.to_csv(
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
destination_folder = "Data/09_Innvandrere og inkludering/Bosetting av flyktninger"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
