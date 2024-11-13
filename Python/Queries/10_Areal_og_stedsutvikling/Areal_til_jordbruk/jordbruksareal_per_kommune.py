import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
from pyjstat import pyjstat
import datetime as dt
import sys
import os
import glob

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09594/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg:KommSummer",
                "values": [
                    "K-4001",
                    "K-4003",
                    "K-4005",
                    "K-4010",
                    "K-4012",
                    "K-4014",
                    "K-4016",
                    "K-4018",
                    "K-4020",
                    "K-4022",
                    "K-4024",
                    "K-4026",
                    "K-4028",
                    "K-4030",
                    "K-4032",
                    "K-4034",
                    "K-4036",
                ],
            },
        },
        {
            "code": "ArealKlasse",
            "selection": {"filter": "vs:ArealHovedklasser2", "values": ["15-16"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
}

resultat = requests.post(POST_URL, json=payload)

if resultat.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat.status_code}")

dataset = pyjstat.Dataset.read(resultat.text)
df_original = dataset.write("dataframe")
df_original.head()
df_original.info()

df = df_original.copy()

# Dropp kolonner "arealklasse" og "statistikkvariabel"
df = df.drop(columns=["arealklasse", "statistikkvariabel"])

# Endre kolonnenavn til Kommunenavn, År og Jordbruksareal
df.columns = ["Kommunenavn", "År", "Jordbruksareal"]

# Lag en variabel "siste_aar" som inneholder siste år i datasettet
siste_aar = df["År"].max()

# Legg til siste år i parentes i kolonnenavnet "Jordbruksareal", eks. "Jordbruksareal (2019)"
df.columns = ["Kommunenavn", "År", f"Jordbruksareal ({siste_aar})"]

# Fjern kolonnen "År"
df = df.drop(columns=["År"])

# Multiply by 1000 to get hectares
df[f"Jordbruksareal ({siste_aar})"] = df[f"Jordbruksareal ({siste_aar})"] * 1000

# Show as integer/no decimals
df[f"Jordbruksareal ({siste_aar})"] = df[f"Jordbruksareal ({siste_aar})"].astype(int)

# Sort by "Jordbruksareal (2019)" descending
df = df.sort_values(by=f"Jordbruksareal ({siste_aar})", ascending=False)

############# Save dfs as a csv files

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"jordbruksareal_per_kommune.csv"
df.to_csv((f"../../Temp/{csv_file_name}"), index=False)  # Relativt til dette scriptet.

##################### Opplasting til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv_file_name}"
destination_folder = "Data/10_Areal- og stedsutvikling/Areal til jordbruk"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
folder_path = "../../Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
