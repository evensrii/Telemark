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

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/bosatt_anmodede"

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

# Filter only the enhet "Personer"
df = df.query("Enhet == 'Personer'")

# Drop the columns "Kommunenummer" and "Enhet"
df = df.drop(columns=["Kommunenummer", "Enhet"])

# Format "År" as datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"anmodninger_og_faktisk_bosetting_tall.csv"
df.to_csv((f"../Temp/{csv_file_name}"), index=False)  # Relativt til dette scriptet.

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
