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
url = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/k/flyktning_botid_flytting"
)

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
print(f"Most recent year in table is {df['År'].max()}")

# Convert the column "Kommunenummer" to a string with 4 digits
df["Kommunenummer"] = (
    df["Kommunenummer"].astype(str).str.pad(width=4, side="left", fillchar="0")
)

# Filter rows where "Enhet" is "Personer"
df = df[df["Enhet"] == "Personer"]

# Filter rows where "Sekundærflytting" is "Bosatt her", "Flyttet hit" or "Uoppgitt".
df = df[df["Sekundærflytting"].isin(["Bosatt her", "Flyttet hit", "Uoppgitt"])]

# Filter rows where "Botid i Norge" is "Alle"
df = df[df["Botid i Norge"] == "Alle"]

# Remove colums "Enhet"
df = df.drop(columns=["Enhet"])

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
df_kommuner = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn

# Map the dictionary to the DataFrame using the 'Kommunenummer' column
# This creates a new Series that can be used to fill missing values in 'Kommune'
df_kommuner["Kommune"] = df_kommuner["Kommune"].fillna(
    df_kommuner["Kommunenummer"].map(kommuner_telemark)
)

# dtale.show(df_kommuner, open_browser=True)


#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"sekundærflytting.csv"
df_kommuner.to_csv(
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
