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
from pyjstat import pyjstat

################# Fullført - Innvandrere #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/13628/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {"code": "Region", "selection": {"filter": "item", "values": ["08"]}},
        {
            "code": "FullforingVGO",
            "selection": {"filter": "item", "values": ["1a", "2a", "4b"]},
        },
        {"code": "UtdProgram", "selection": {"filter": "item", "values": ["00"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Prosent"]},
        },
    ],
    "response": {"format": "json-stat2"},
}

resultat = requests.post(POST_URL, json=payload)

if resultat.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat.status_code}")

dataset = pyjstat.Dataset.read(resultat.text)
df_innvandrere = dataset.write("dataframe")
df_innvandrere.info()
df_innvandrere.head()

# Remove columns
df_innvandrere = df_innvandrere.drop(
    columns=["region", "kjønn", "todelt utdanningsprogram", "statistikkvariabel"]
)

# Group by 'fullføringsgrad' and 'intervall (år)', and sum the 'value' column
df_innvandrere["fullføringsgrad"] = "Fullført"
df_innvandrere = (
    df_innvandrere.groupby(["fullføringsgrad", "intervall (år)"])["value"]
    .sum()
    .reset_index()
)

# Legge til kategori for innvandrere
df_innvandrere["gruppe"] = "Innvandrere"


################# Fullført - Hele befolkningen #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/12971/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {"code": "Region", "selection": {"filter": "item", "values": ["08"]}},
        {
            "code": "FullforingVGO",
            "selection": {"filter": "item", "values": ["1a", "2a", "4b"]},
        },
        {"code": "UtdProgram", "selection": {"filter": "item", "values": ["00"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Prosent"]},
        },
    ],
    "response": {"format": "json-stat2"},
}

resultat = requests.post(POST_URL, json=payload)

if resultat.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat.status_code}")

dataset = pyjstat.Dataset.read(resultat.text)
df_befolkningen = dataset.write("dataframe")
df_befolkningen.info()
df_befolkningen.head()
# dtale.show(df_befolkningen, open_browser=True)

# Remove columns
df_befolkningen = df_befolkningen.drop(
    columns=["region", "kjønn", "todelt utdanningsprogram", "statistikkvariabel"]
)

# Group by 'fullføringsgrad' and 'intervall (år)', and sum the 'value' column
df_befolkningen["fullføringsgrad"] = "Fullført"
df_befolkningen = (
    df_befolkningen.groupby(["fullføringsgrad", "intervall (år)"])["value"]
    .sum()
    .reset_index()
)

# Legge til kategori for innvandrere
df_befolkningen["gruppe"] = "Hele befolkningen"


########## Merge datasett ##########

# Merge df_aggregert_fylke to df_fylker
df_telemark = pd.concat([df_befolkningen, df_innvandrere], ignore_index=True)

# Create new column "Oppstartsår" containing the number before "-" in the "intervall (år)" column
df_telemark["År"] = df_telemark["intervall (år)"].str.split("-").str[0]

# Remove column "fullføringsgrad"
df_telemark = df_telemark.drop(columns=["fullføringsgrad", "intervall (år)"])

# Rename columns to "Andel", "Gruppe" and "År"
df_telemark = df_telemark.rename(columns={"value": "Andel", "gruppe": "Gruppe"})

# Reorder columns to "År", "Gruppe" and "Andel"
df_telemark = df_telemark[["År", "Gruppe", "Andel"]]

# Set the number of decimals in the "Andel" column to 1
df_telemark["Andel"] = df_telemark["Andel"].round(1)

# Pivotere til rett format
df_telemark_pivot = df_telemark.pivot_table(
    index=["År"], columns="Gruppe", values="Andel"
).reset_index()

# Flatten the MultiIndex columns
df_telemark_pivot.columns = [col for col in df_telemark_pivot.columns]


#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"fullført_vgo_innv_befolk.csv"
df_telemark_pivot.to_csv(
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
