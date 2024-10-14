import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob

## Imdi gir ingen direkte url til .xlsx-fil, må trykke JS-knapp som trigger "OnClick"-event.
# Jeg bruker requests for å simulere nedlasting av filen.

# Finner URL vha. "Inspiser side" og fane "Network"
url = "https://app-simapi-prod.azurewebsites.net/download_csv/k/befolkning_innvandringsgrunn"

# Make a GET request to the URL to download the file
response = requests.get(url)

# Hente ut innhold (data)
url_content = response.content

if response.status_code == 200:

    df = pd.read_csv(
        BytesIO(url_content),
        delimiter=";",
        encoding="ISO-8859-1",
        dtype={"Kommunenummer": str},
    )

else:
    print(f"Failed to download the file. Status code: {response.status_code}")

## Datasjekk
df.head()
df.info()

# Print the unique values in the column "Kommunenummer"
print(df["Kommunenummer"].unique())

# Convert the object columns into category
columns_to_convert = ["Kommune", "Kjønn", "Innvandringsgrunn", "Enhet"]

for column in columns_to_convert:
    df[column] = df[column].astype("category")

# Konvertere kolonne "År" til datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Filtrere kun personer
df = df.query("Enhet == 'Personer'")

# Konverter "Kommunenummer" til string med 4 siffer
df["Kommunenummer"] = df["Kommunenummer"].astype(str).str.pad(width=4, fillchar="0")

# Konverter "Kommune" til object
df["Kommune"] = df["Kommune"].astype("object")

# Filter ved hjelp av query basert på keys in kommuner_telemark

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
df = df[df["Kommunenummer"].isin(kommuner_telemark.keys())]

## Innfylling av manglende kommunenavn
df["Kommune"] = df["Kommune"].fillna(df["Kommunenummer"].map(kommuner_telemark))

# Filter only "Alle" in the "Kjønn" column
df = df.query("Kjønn == 'Alle'")

# Remove rows with "Alle" in the "Innvandringsgrunn" column
# df = df.query("Innvandringsgrunn != 'Alle'")

# Filter only the most recent data (last year available)
most_recent_year = df["År"].max()
df = df.query("År == @most_recent_year")
print(f"Selected data from year {df['År'].max()}")

# Create a table with a row for each "Kommune", and a column for each "Innvandringsgrunn"
df_pivot = df.pivot(
    index="Kommune", columns="Innvandringsgrunn", values="Antall"
).reset_index()

# Calculate the percentage for each category relative to 'Alle'
percentage_columns = df_pivot.columns[
    2:
]  # Skip 'Innvandringsgrunn', 'Kommune', and 'Alle'
for column in percentage_columns:
    df_pivot[column] = ((df_pivot[column] / df_pivot["Alle"]) * 100).astype(
        int
    )  # Convert to percentage and format as integer

# Remove the "Innvandringsgrunn" column
df_pivot.columns.name = None

# Split the data into two dataframes, one for 'Arbeidsinnvandrere' and one for 'Flyktninger og deres familieinnvandrede'
df_pivot_arbeidsinnvandrere = df_pivot[["Kommune", "Arbeidsinnvandrere"]]
df_pivot_flyktninger = df_pivot[["Kommune", "Flyktninger og deres familieinnvandrede"]]

# Rename the columns to "Andel flyktninger {most_recent_year.year}" and "Andel arbeidsinnvandrere {most_recent_year.year}"
df_pivot_arbeidsinnvandrere.columns = [
    "Kommune",
    f"Andel arbeidsinnvandrere {most_recent_year.year}",
]
df_pivot_flyktninger.columns = ["Kommune", f"Andel flyktninger {most_recent_year.year}"]


#### Save dfs as csv files

# Ønsket filnavn <----------- MÅ ENDRES MANUELT!
csv_file_name1 = f"andel_arbeidsinnvandrere.csv"
csv_file_name2 = f"andel_flyktninger.csv"

df_pivot_arbeidsinnvandrere.to_csv(
    (f"../../Temp/{csv_file_name1}"), index=False
)  # Relativt til dette scriptet.

df_pivot_flyktninger.to_csv(
    (f"../../Temp/{csv_file_name2}"), index=False
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

csv_file1 = f"../../Temp/{csv_file_name1}"
destination_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file1, destination_folder, github_repo, git_branch)

csv_file2 = f"../../Temp/{csv_file_name2}"
destination_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file2, destination_folder, github_repo, git_branch)

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
