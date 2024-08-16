import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat


################# Spørring #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09817/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg_single:KommGjeldende",
                "values": [
                    "4001",
                    "4003",
                    "4005",
                    "4010",
                    "4012",
                    "4014",
                    "4016",
                    "4018",
                    "4020",
                    "4022",
                    "4024",
                    "4026",
                    "4028",
                    "4030",
                    "4032",
                    "4034",
                    "4036",
                ],
            },
        },
        {"code": "InnvandrKat", "selection": {"filter": "item", "values": ["B"]}},
        {"code": "Landbakgrunn", "selection": {"filter": "item", "values": ["999"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["AndelBefolkning"]},
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
df = dataset.write("dataframe")
df.head()
df.info()

# Print the first value in the column "år"
print(f"Her hentes tallene for{df['år'].unique()}")

################# Bearbeiding datasett #################

# Keep only the columns "region" and "value"
df = df[["region", "value"]]

# Rename headers to "Kommune" and "Andel"
df.columns = ["Kommune", "Andel"]

# Round the values in the column to 0 decimals, and convert to integer
df["Andel"] = df["Andel"].astype(int)

# Create a third column identical to "Kommune", but name the column "Labels"
df["Labels"] = df["Kommune"]

################# Eksportere datasett til csv #################

df.to_csv("andel_innvandrere_bosatt.csv", index=False)  # Relativt til dette scriptet.

##################### Pushe til Github #####################

# Legge til parent directory i sys.path for å importere github_functions.py
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)
import github_functions

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

source_file = "../Temp/andel_innvandrere_bosatt.csv"  # Relativt til dette scriptet.
destination_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

github_functions.upload_to_github(
    source_file, destination_folder, github_repo, git_branch
)
