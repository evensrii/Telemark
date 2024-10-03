import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat

################# Spørring #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/03440/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {"code": "Region", "selection": {"filter": "vs:FylkerJakt", "values": ["40"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["1", "2"]}},
        {
            "code": "Alder",
            "selection": {
                "filter": "item",
                "values": [
                    "00-19a",
                    "20-29",
                    "30-39",
                    "40-49",
                    "50-59",
                    "60-69",
                    "070+",
                ],
            },
        },
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["BetaltJegeravg"]},
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

# Drop kolonner "region" og "statistikkvariabel"
df = df.drop(columns=["region", "statistikkvariabel"])

# Rename og inkludere "siste_år" i colonnenavnet "Antall aktive jegere"
siste_år = df["intervall (år)"].max()
df.columns = ["Kjønn", "Alder", "Jaktår", f"Antall aktive jegere {siste_år}"]

# Drop kolonne "Jaktår"
df = df.drop(columns=["Jaktår"])

# Print the first value in the column "år"
print(f"Her hentes tallene for jaktåret {siste_år}")

# Pivoter på kolonne "Kjønn"
df_pivot = df.pivot(
    index="Alder", columns="Kjønn", values=f"Antall aktive jegere {siste_år}"
).reset_index()

df_pivot.columns.name = None  # Remove the multi-index name for columns

## Skal ha aldersfordeling i prosent for hvert kjønn

# Calculate the total sum of both 'Kvinner' and 'Menn' across all rows
total_kvinner = df_pivot["Kvinner"].sum()
total_menn = df_pivot["Menn"].sum()

# Create the 'Andel kvinner' and 'Andel menn' columns as percentages of the total within each gender
df_pivot["Andel kvinner"] = (df_pivot["Kvinner"] / total_kvinner) * 100
df_pivot["Andel menn"] = (df_pivot["Menn"] / total_menn) * 100

# Set the row order as "Under 20 år", "20-29 år", "30-39 år", "40-49 år", "50-59 år", "60-69 år" and "70 år eller eldre"
df_pivot = df_pivot.reindex([6, 0, 1, 2, 3, 4, 5]).reset_index(drop=True)

# Drop columns Kvinner og Menn
df_pivot = df_pivot.drop(columns=["Kvinner", "Menn"])

# Round values in andel to 1 decimals
df_pivot["Andel kvinner"] = df_pivot["Andel kvinner"].round(1)
df_pivot["Andel menn"] = df_pivot["Andel menn"].round(1)

# Set column names as "Alder", "Kvinner" and "Menn"
df_pivot.columns = ["Alder", f"Kvinner {siste_år}", f"Menn {siste_år}"]


############# Save dfs as a csv files

csv = "andel_jegere.csv"
df_pivot.to_csv((f"../../Temp/{csv}"), index=False)


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
