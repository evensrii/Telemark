import requests
from io import BytesIO
from io import StringIO
import numpy as np
import pandas as pd
import datetime as dt
import sys
import os
import glob

## Dette scriptet gir data for donut-figur (csv x2) og stolpediagram (csv x1)

## Mdir gir ingen direkte url til .xlsx-fil, så jeg bruker requests for å simulere nedlasting av filen

url = "https://www.miljodirektoratet.no/globalassets/netttjenester/klimagassutslipp-kommuner/utslippsstatistikk_alle_kommuner.xlsx"

# Make a GET request to the URL to simulate downloading the file
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:

    # Use BytesIO to create a file-like object from the content (BytesIO lar deg jobbe med binære filer.)
    excel_file = BytesIO(response.content)

    # Use pandas ExcelFile to work with multiple sheets
    xls = pd.ExcelFile(excel_file)

    # Get the sheet names
    sheet_names = xls.sheet_names

    # Create a dictionary to store DataFrames for each sheet
    sheet_data = {}

    # Iterate through each sheet and read the data into a DataFrame
    for sheet_name in sheet_names:
        sheet_data[sheet_name] = xls.parse(sheet_name)

    # Now 'sheet_data' is a dictionary where keys are sheet names and values are DataFrames
    # You can access individual DataFrames like sheet_data['Sheet1']
    print(sheet_data)
else:
    print(f"Failed to download the file. Status code: {response.status_code}")

# Hente ut aktuelt ark
df = sheet_data["Oversikt - detaljert"]

# Basic overview of dataset
df.head()
df.info()

########################### HOVED-DATASETT (2009 - d.d.)

# Filter Telemark fylke
df_telemark = df[df["Fylke"] == "Telemark"]

# Rename column name "Utslipp (tonn CO₂-ekvivalenter)" to "Utslipp"
df_telemark.columns = df_telemark.columns.str.replace(
    "Utslipp (tonn CO2-ekvivalenter)", "Utslipp"
)

# Remove columns "Fylke", "Kommunenummer" and "Utslippskilde"
df_telemark = df_telemark.drop(columns=["Fylke", "Kommunenummer", "Klimagass"])

# Convert column 'År' from integer to datetime format
df_telemark["År"] = pd.to_datetime(
    df_telemark["År"], format="%Y"
)  # %Y indicates the format of the input date

# Round the values in the "Utslipp" column to two decimal places
df_telemark["Utslipp"] = df_telemark["Utslipp"].round(2)

########################### DONUT-FIGUR (siste år)

## Funksjon for å gruppere data etter sektor og summere, for henholdsvis nyeste år (recent_year = True) eller året før (recent_year = False)


def beregne_utslipp(recent_year=True):
    # Step 1: Create a copy of the original dataframe
    df_donut = df_telemark.copy()

    # Step 2: Fetch the most recent year in the dataframe
    most_recent_year = df_donut["År"].max()

    # Step 3: Determine the target year based on the recent_year flag
    if recent_year:
        target_year = most_recent_year
    else:
        target_year = most_recent_year - pd.DateOffset(years=1)

    # Step 4: Filter rows where "År" is the target year
    df_donut = df_donut[df_donut["År"] == target_year]

    # Step 5: Group by 'Sektor' and sum 'Utslipp'
    df_donut = df_donut.groupby(["År", "Sektor"]).agg({"Utslipp": "sum"}).reset_index()

    # Step 6: Add the year to the "Utslipp" column name
    utslipp_column_name = f"Utslipp ({target_year.year})"
    df_donut = df_donut.rename(columns={"Utslipp": utslipp_column_name})

    # Step 7: Remove column "År"
    df_donut = df_donut.drop(columns="År")

    # Step 8: Group "Oppvarming", "Luftfart", and "Energiforsyning" into "Annet utslipp"
    sectors_to_group = ["Oppvarming", "Luftfart", "Energiforsyning"]

    # Calculate the sum of "Utslipp" for the selected sectors
    annet_sum = df_donut[df_donut["Sektor"].isin(sectors_to_group)][
        utslipp_column_name
    ].sum()

    # Remove the original sectors from the DataFrame
    df_donut = df_donut[~df_donut["Sektor"].isin(sectors_to_group)]

    # Append the new row with the combined "Annet utslipp"
    new_row = pd.DataFrame(
        [{"Sektor": "Annet utslipp", utslipp_column_name: annet_sum}]
    )
    df_donut = pd.concat([df_donut, new_row], ignore_index=True)

    # (Optional) Sort the DataFrame if needed
    df_donut = df_donut.sort_values(
        by=utslipp_column_name, ascending=False
    ).reset_index(drop=True)

    # Round the values in the "Utslipp" column to nearest integer
    df_donut[utslipp_column_name] = df_donut[utslipp_column_name].round()

    return df_donut


df_donut = beregne_utslipp(recent_year=True)  # True = siste år

########################### ENDRING FRA NEST SISTE TIL SISTE

df_nest_siste = beregne_utslipp(recent_year=False)  # False = året før siste år

# Extract the years from the column names (for dynamic column naming)
year_next_most_recent = df_nest_siste.columns[-1][
    -5:-1
]  # I.e. extracts the year (2021) from "Utslipp (2021)"
year_most_recent = df_donut.columns[-1][
    -5:-1
]  # I.e. extracts the year (2022) from "Utslipp (2022)"

## Merge de to tabellene og beregne prosentvis endring

# Step 1: Merge the two DataFrames on "Sektor"
df_pst_endring = pd.merge(df_donut, df_nest_siste, on="Sektor", how="inner")

# Step 2: Rename the columns to reflect the years
df_pst_endring = df_pst_endring.rename(
    columns={"Utslipp (2021)": "I fjor", "Utslipp (2022)": "I år"}
)

# Step 3: Dynamically set the new column name
pst_endring_column_name = f"Pst_endring_{year_next_most_recent}_{year_most_recent}"

# Step 4: Calculate the percentage change and add it as a new column
df_pst_endring[pst_endring_column_name] = (
    (df_pst_endring["I år"] - df_pst_endring["I fjor"]) / df_pst_endring["I fjor"]
) * 100

# Remove columns "I fjor" and "I år"
df_pst_endring = df_pst_endring.drop(columns=["I fjor", "I år"])

# Round the values in the "Pst_endring" column to one decimal
df_pst_endring[pst_endring_column_name] = df_pst_endring[pst_endring_column_name].round(
    1
)

############# Save dfs as a csv files

csv1 = f"klimagassutslipp_telemark.csv"
df_telemark.to_csv((f"../../Temp/{csv1}"), index=False)

csv2 = f"klimagassutslipp_donut.csv"
df_donut.to_csv((f"../../Temp/{csv2}"), index=False)

csv3 = f"klimagassutslipp_endring.csv"
df_pst_endring.to_csv((f"../../Temp/{csv3}"), index=False)


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
csv_file3 = f"../../Temp/{csv3}"
destination_folder = "Data/04_Klima og ressursforvaltning/Klimagassutslipp"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file1, destination_folder, github_repo, git_branch)
upload_file_to_github(csv_file2, destination_folder, github_repo, git_branch)
upload_file_to_github(csv_file3, destination_folder, github_repo, git_branch)


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
