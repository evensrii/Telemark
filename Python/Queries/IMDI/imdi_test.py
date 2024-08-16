import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob

# Legge til parent directory i sys.path for å importere github_functions.py
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)
import github_functions

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

# Finner URL vha. "Inspiser side" og fane "Network"
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/befolkning_innvandringsgrunn"

# Make a GET request to the URL to download the file
response = requests.get(url)

# Hente ut innhold (data)
url_content = response.content

if response.status_code == 200:

    df = pd.read_csv(BytesIO(url_content), delimiter=";", encoding="ISO-8859-1")

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

## Hvis jeg ønsker å lagre rådata som fil:
# csv_file = open("data/befolkning_innvandringsgrunn.csv", "wb")
# csv_file.write(url_content)
# csv_file.close()

# Basic overview of dataset
df.head()
df.shape
df.dtypes
df.info()
df[df.duplicated()]  # Sjekk for duplikater
df.isna().sum()  # Sjekk for missing values (per feature)
df.isna().sum().sum()  # Any missing values
round(df.isna().sum().sum() / df.size * 100, 1)  # Percentage of missing cells
df.describe()  # Mål for numeriske variabler
# df.describe(include="category")  # Inluderer også kategoriske variabler

# Convert the object columns into category
columns_to_convert = ["Kommune", "Kjønn", "Innvandringsgrunn", "Enhet"]

for column in columns_to_convert:
    df[column] = df[column].astype("category")

# Konvertere kolonne "År" til datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Filtrere kun prosent
df = df.query("Enhet == 'Personer'")

# Filter ved hjelp av query basert på keys in kommuner_telemark

kommuner_telemark = {
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

# Filtrer kun kommuner i Telemark
df = df.query("Kommune in @kommuner_telemark.values()")

# Filter only "Alle" in the "Kjønn" column
df = df.query("Kjønn == 'Alle'")

# Remove rows with "Alle" in the "Innvandringsgrunn" column
df = df.query("Innvandringsgrunn != 'Alle'")

# Create a table showing the sum of "Antall" by "Innvandringsgrunn" and "År"
df_grunn_aar = (
    df.groupby(["År", "Innvandringsgrunn"]).agg({"Antall": "sum"}).reset_index()
)

## Save df_grunn_aar as a csv file
df_grunn_aar.to_csv(
    "../Temp/innvandringsgrunn_telemark.csv", index=False
)  # Relativt til dette scriptet.


##################### Opplasting til Github #####################

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

source_file = "../Temp/innvandringsgrunn_telemark.csv"  # Relativt til dette scriptet.
destination_folder = "Data/09_Innvandrere og inkludering"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

github_functions.upload_to_github(
    source_file, destination_folder, github_repo, git_branch
)

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
