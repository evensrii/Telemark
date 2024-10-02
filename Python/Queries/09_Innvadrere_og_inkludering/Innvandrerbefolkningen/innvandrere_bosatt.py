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

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = "andel_innvandrere_bosatt.csv"

df.to_csv((f"../../Temp/{csv_file_name}"), index=False)  # Relativt til dette scriptet.

##################### Pushe til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv_file_name}"
destination_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
