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

# Telemark, siste år.

## Har kun andeler, og kun for Vestfold og Telemark samlet. Kan dermed ikke regne ut for Telemark samlet basert på absolutte tall.
## Utarbeider scriptet slik at det i framtiden kan hente ut tall for kun Telemark.

###### Andel innvandrere i lavinntekt ######

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/f/vedvarende_lavinntekt"

# Make a GET request to the URL to download the file
response = requests.get(url)

# Hente ut innhold (data)
url = response.content

if response.status_code == 200:

    df = pd.read_csv(BytesIO(url), delimiter=";", encoding="ISO-8859-1")

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

df.info()
df.head()

# Set data types
df["Fylkesnummer"] = df["Fylkesnummer"].astype(str).str.zfill(2)
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Fange opp VTFK, og etterhvert også TFK-tallene.
df = df[df["Fylkesnummer"].isin(["38", "40"])]
# dtale.show(df, open_browser=True)

# Get the most recent year in the dataset
most_recent_year = df["År"].max()

# Add the year ({most_recent_year.year}) to the "Andel" column name
andel_column_name = f"Andel ({most_recent_year.year})"
df = df.rename(columns={"Antall": andel_column_name})

# Filter rows based on the most recent year
df = df[df["År"] == most_recent_year]

# Filter based on other criteria
df = df[df["Bakgrunn"] != "Hele befolkningen"]
df = df[~((df["Bakgrunn"] == "Innvandrere") & (df["Verdensregion"] == "Alle"))]

# Replace values
df["Verdensregion"] = df["Verdensregion"].replace(
    {
        "Øst-Europa utenom EU, Asia (inkl.Tyrkia), Afrika, Sør- og Mellom-Amerika og Oseania utenom Australia og New Zealand": "Gruppe 2-land (Asia, Afrika, Latin-Amerika osv.)",
        "EU/EFTA-land, Nord-Amerika, Australia og New Zealand": "Gruppe 1-land (EU, Storbritannia, USA osv.)",
        "Alle": "Befolkningen unntatt innvandrere",
    }
)

# Convert "Andel" to numeric and round to nearest integer
df[andel_column_name] = pd.to_numeric(
    df[andel_column_name].str.replace(",", "."), errors="coerce"
)
df[andel_column_name] = df[andel_column_name].round(0).astype(int)

# Rename columns
df = df.rename(columns={"Verdensregion": "Gruppe"})

# Remove columns
df = df.drop(columns=["Bakgrunn", "Fylkesnummer", "Fylke", "År", "Enhet"])

# Reset index
df = df.reset_index(drop=True)


#### Save df as a csv file

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name = f"andel_innvandre_i_lavinntekt.csv"
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
destination_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"  # Mapper som ikke eksisterer vil opprettes automatisk.
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
