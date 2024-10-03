import requests
import sys
import os
import glob
import pandas as pd
import numpy as np
from pyjstat import pyjstat

################# Felte elg #################

### Fylker 2024, sammenslåtte tidsserier

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/03432/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {
            "code": "Region",
            "selection": {"filter": "agg:KommFylker", "values": ["F-40"]},
        },
        {"code": "Dyr", "selection": {"filter": "item", "values": ["00a"]}},
    ],
    "response": {"format": "json-stat2"},
}

resultat = requests.post(POST_URL, json=payload)

if resultat.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat.status_code}")

dataset = pyjstat.Dataset.read(resultat.text)
df_elg = dataset.write("dataframe")
df_elg.head()
df_elg.info()


################# Felte hjort #################

### Fylker 2024, sammenslåtte tidsserier

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/03434/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {
            "code": "Region",
            "selection": {"filter": "agg:KommFylker", "values": ["F-40"]},
        },
        {"code": "Dyr", "selection": {"filter": "item", "values": ["00a"]}},
    ],
    "response": {"format": "json-stat2"},
}

resultat = requests.post(POST_URL, json=payload)

if resultat.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat.status_code}")

dataset = pyjstat.Dataset.read(resultat.text)
df_hjort = dataset.write("dataframe")
df_hjort.head()
df_hjort.info()


df_elg = df_elg.drop(columns=["region", "alder", "statistikkvariabel"])
df_elg.columns = ["Jaktår", "Antall felte elg"]

df_hjort = df_hjort.drop(columns=["region", "alder", "statistikkvariabel"])
df_hjort.columns = ["Jaktår", "Antall felte hjort"]

df_hjort.info()

## Tall for felte hjort mangler for 2008/2009. Bruker lineær interpolering (fra numpy) for å estimere verdien.
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].replace(0, np.nan)
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].interpolate(
    method="linear"
)
df_hjort["Antall felte hjort"] = df_hjort["Antall felte hjort"].round()

# Merge dataframes
df = pd.merge(df_elg, df_hjort, on="Jaktår")

# Remove everything after "-" in "Jaktår" (including "-")
df["Jaktår"] = df["Jaktår"].str.split("-").str[0]

############# Save dfs as a csv files

csv = "antall_felte_elg_og_hjort.csv"
df.to_csv((f"../../Temp/{csv}"), index=False)


##################### Opplasting til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv}"
destination_folder = "Data/04_Klima og ressursforvaltning/Ressursforvaltning"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
