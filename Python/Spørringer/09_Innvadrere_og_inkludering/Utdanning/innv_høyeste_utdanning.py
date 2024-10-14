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


######## Per september 2024 vises VTFK-tall for 2022, men sørger for at scriptet også fanger opp Telemarkstallene (2024) når de kommer. ######################################################

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url_fylker = "https://app-simapi-prod.azurewebsites.net/download_csv/f/utdanningsniva"

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

# Set data types
df_fylker["Fylkesnummer"] = df_fylker["Fylkesnummer"].astype(str).str.zfill(2)
df_fylker["År"] = pd.to_datetime(df_fylker["År"], format="%Y")

# Fange opp VTFK, og etterhvert også TFK-tallene.
df_fylker = df_fylker[df_fylker["Fylkesnummer"].isin(["38", "40"])]
# dtale.show(df_fylker, open_browser=True)

# Get the most recent year in the dataset
most_recent_year = df_fylker["År"].max()

# Filter rows based on the most recent year
df_fylker = df_fylker[df_fylker["År"] == most_recent_year]

# Filter based on other criteria
df_fylker = df_fylker[df_fylker["Enhet"] == "Prosent"]
df_fylker = df_fylker[df_fylker["Bakgrunn"] != "Hele befolkningen"]
df_fylker = df_fylker[df_fylker["Kjønn"] == "Alle"]
df_fylker = df_fylker[df_fylker["Utdanningsnivå"] != "Alle"]

# Remove columns
df_fylker = df_fylker.drop(columns=["Enhet", "Fylke", "Kjønn"])

# Rename columns
df_fylker = df_fylker.rename(columns={"Antall": "Andel"})

# Reset index
df_fylker = df_fylker.reset_index(drop=True)


# Pivotere til rett format
df_fylker_pivot = df_fylker.pivot_table(
    index=["Bakgrunn", "År", "Fylkesnummer"], columns="Utdanningsnivå", values="Andel"
).reset_index()

# Flatten the MultiIndex columns
df_fylker_pivot.columns = [col for col in df_fylker_pivot.columns]

# Move columns "År" and "Fylkesnummer" to the end
df_fylker_pivot = df_fylker_pivot[
    [
        "Bakgrunn",
        "Grunnskole",
        "Videregående skole",
        "Universitet og høgskole",
        "Ingen utdanning",
        "Uoppgitt",
    ]
]


#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"høyeste_utdanning_innv.csv"
df_fylker_pivot.to_csv(
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
destination_folder = "Data/09_Innvandrere og inkludering/Utdanningsnivå Telemark"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
