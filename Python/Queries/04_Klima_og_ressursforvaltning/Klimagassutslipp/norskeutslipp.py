import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob
import openpyxl

##### Trikset var her at jeg måtte starte en session for å simulere at jeg besøkte nettsiden før jeg lasted ned!

##### Lagrer fullt datasett fra Telemark (med info om hvert anlegg fra hvert år) + datasett for Everviz-figur

### Kjøre spørring (Excel --> Pandas DataFrame)

# URL for the main page or the page that needs to be visited first
initial_url = "https://www.norskeutslipp.no/no/Komponenter/Utslipp/Klimagasser-CO2-ekvivalenter/?ComponentType=utslipp&ComponentPageID=1166&SectorID=600"

# URL for the Excel file
download_url = "https://www.norskeutslipp.no/Templates/NorskeUtslipp/Pages/exportTableData.aspx?PageID=1166&ComponentType=utslipp&exportType=temaexcel&epslanguage=no"

# Create a session object to persist cookies (her samles cookies og brukes automatisk videre)
session = requests.Session()

# Step 1: Send an initial request to the main page to establish the session
session.get(initial_url)

# Step 2: Attempt to download the Excel file using the same session
response = session.get(download_url)

# Step 3: Check if the request was successful
if response.status_code == 200:

    ## Save the content to an Excel file and inspect it manually
    # with open("downloaded_file.xlsx", "wb") as f:
    #    f.write(response.content)
    # print(
    #    "File saved as 'downloaded_file.xlsx'. Inspect the file to check its content."
    # )

    # Load the Excel file into pandas using the 'openpyxl' engine
    excel_data = BytesIO(response.content)
    df_downloaded = pd.read_excel(excel_data, engine="openpyxl")

    # Display the DataFrame
    print(df_downloaded)
else:
    print(f"Failed to download the file. Status code: {response.status_code}")

### Data cleaning

df = df_downloaded.copy()

df.info()

# Drop rows with all na values
df = df.dropna(how="all")

# Make the first row the header and remove that row
df.columns = df.iloc[0]  # Set the first row as the header
df = df[1:].reset_index(drop=True)  # Drop the first row and reset index in one step

# Filter Fylke == "Telemark"
df = df[df["Fylke"] == "Telemark"]

# Convert "År" to datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")
df = df[df["År"] > "2000"]

# Convert "Årlig utslipp til luft" to numeric
df["Årlig utslipp til luft"] = df["Årlig utslipp til luft"].astype(str)
df["Årlig utslipp til luft"] = (
    df["Årlig utslipp til luft"].str.replace(",", ".", regex=False).str.strip()
)
df["Årlig utslipp til luft"] = pd.to_numeric(
    df["Årlig utslipp til luft"], errors="coerce"
)

df = df.round(2)

### "Fullt datasett" for Telemark (kjekt å ha)

df_norske_detaljert = df.copy()

#### Everviz-figur

# Kolonne 1 (Norskeutslipp.no)
df = df.groupby("År")["Årlig utslipp til luft"].sum().reset_index()

# Kolonne 2 (Mdir - industriutslipp) <--- Denne genereres fra det andre scriptet, "klimagassutslipp.py"
url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/04_Klima%20og%20ressursforvaltning/Klimagassutslipp/klimagassutslipp_telemark.csv"
df_industri = pd.read_csv(url)
df_industri = df_industri[df_industri["Sektor"] == "Industri, olje og gass"]
df_industri = df_industri.groupby("År")["Utslipp"].sum().reset_index()
# convert "År" to datetime
df_industri["År"] = pd.to_datetime(df_industri["År"], format="%Y-%m-%d")

# Merge df and df_industri by "År"
df = pd.merge(df, df_industri, on="År", how="outer")

df.columns = ["År", "Utslipp (norskeutslipp.no)", "Utslipp (Mdir)"]

# Create a new column 'Mdir enkeltår' and move the values for the specified years
df["Mdir enkeltår"] = df["Utslipp (Mdir)"].where(
    df["År"].dt.year.isin([2009, 2011, 2013])
)

# Set the values for those years in the original column to NaN (optional)
df.loc[df["År"].dt.year.isin([2009, 2011, 2013]), "Utslipp (Mdir)"] = None

# Round all float64 values to 2 decimals
df = df.round(2)

df_norskeutslipp_everviz = df.copy()


############# Save dfs as a csv files

csv1 = f"norskeutslipp_detaljert_telemark.csv"
df_norske_detaljert.to_csv((f"../../Temp/{csv1}"), index=False)

csv2 = f"norskeutslipp_everviz.csv"
df_norskeutslipp_everviz.to_csv((f"../../Temp/{csv2}"), index=False)


##################### Opplasting til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file1 = f"../../Temp/{csv1}"
csv_file2 = f"../../Temp/{csv2}"
destination_folder = "Data/04_Klima og ressursforvaltning/Klimagassutslipp"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file1, destination_folder, github_repo, git_branch)
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
