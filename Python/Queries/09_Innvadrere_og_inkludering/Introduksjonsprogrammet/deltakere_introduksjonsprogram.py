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

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

########### Fylke Telemark 2005 - 2019 + 2024 (datasett "Alle fylker")

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = "https://app-simapi-prod.azurewebsites.net/download_csv/f/intro_deltakere"

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

# Filter columns
df_fylker = df_fylker[
    df_fylker["Fylkesnummer"].isin([8, 40])
]  # Fanger opp Telemark fom. 2024
df_fylker = df_fylker[df_fylker["Enhet"] == "Personer"]
df_fylker = df_fylker[df_fylker["Kjønn"] == "Alle"]

# Drop columns
df_fylker = df_fylker.drop(columns=["Enhet", "Fylkesnummer", "Fylke", "Kjønn"])
df_fylker = df_fylker.sort_values(by=["År"], ascending=True)

# Format "År" as datetime
df_fylker["År"] = pd.to_datetime(df_fylker["År"], format="%Y")


########### Kommuner Telemark 2020 - 2023 (datasett "Alle kommuner", må aggregere til Telemark fylke)

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_kommuner = (
    "https://app-simapi-prod.azurewebsites.net/download_csv/k/intro_deltakere"
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


## Filtrering av rader hvor "Kommunenummer" er i kommuner_telemark.keys()

df_kommuner = df_kommuner[df_kommuner["Kommunenummer"].isin(kommuner_telemark.keys())]
# dtale.show(df_kommuner, open_browser=True)

## Innfylling av manglende kommunenavn
# Map the dictionary to the DataFrame using the 'Kommunenummer' column
# This creates a new Series that can be used to fill missing values in 'Kommune'
df_kommuner["Kommune"] = df_kommuner["Kommune"].fillna(
    df_kommuner["Kommunenummer"].map(kommuner_telemark)
)

# Filter columns
df_kommuner = df_kommuner[df_kommuner["Enhet"] == "Personer"]
df_kommuner = df_kommuner[df_kommuner["Kjønn"] == "Alle"]

# Drop the columns "Kommunenummer" and "Enhet"
df_kommuner = df_kommuner.drop(columns=["Enhet", "Kommunenummer", "Kjønn"])

# Format "År" as datetime
df_kommuner["År"] = pd.to_datetime(df_kommuner["År"], format="%Y")

# Convert the column "Antall" to a numeric data type
df_kommuner["Antall"] = pd.to_numeric(df_kommuner["Antall"], errors="coerce")

# Replace NaN values in "Antall" with 0
df_kommuner["Antall"] = df_kommuner["Antall"].fillna(0)

# Group by the specified columns and aggregate the 'Antall' column
df_aggregert_fylke = df_kommuner.groupby(["År"], as_index=False)["Antall"].sum()

# dtale.show(df_aggregert_fylke, open_browser=True)


########## Merge df_fylker (fylkesdata) and df_aggregert_fylke (kommunedata) ##########

# Merge df_aggregert_fylke to df_fylker
df_telemark = pd.concat([df_fylker, df_aggregert_fylke], ignore_index=True)

# Sort by "År"
df_telemark = df_telemark.sort_values(by=["År"], ascending=True)

# Rename columns to "År" and "Deltakere"
df_telemark = df_telemark.rename(columns={"År": "År", "Antall": "Deltakere"})


#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"deltakere_introduksjonsprogrammet.csv"
df_telemark.to_csv(
    (f"../../Temp/{csv_file_name}"), index=False
)  # Relativt til dette scriptet.

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
folder_path = "../../Temp"

# Call the function to delete files
delete_files_in_folder(folder_path)
