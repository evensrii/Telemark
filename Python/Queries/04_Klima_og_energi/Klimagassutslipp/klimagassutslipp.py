import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

## Dette scriptet gir data for donut-figur (csv x2) og stolpediagram (csv x1)

## Mdir gir ingen direkte url til .xlsx-fil, så jeg bruker requests for å simulere nedlasting av filen

# url = "https://www.miljodirektoratet.no/globalassets/netttjenester/klimagassutslipp-kommuner/utslippsstatistikk_alle_kommuner.xlsx"

url = "https://www.miljodirektoratet.no/sharepoint/downloaditem/?id=01FM3LD2WOT7QL4D6Y5FC3JYADXGI6A3FA"

try:
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
            try:
                sheet_data[sheet_name] = xls.parse(sheet_name)
            except Exception as e:
                error_message = f"Failed to parse sheet '{sheet_name}': {str(e)}"
                print(error_message)
                error_messages.append(error_message)

        # Check if the specific sheet exists in the Excel file
        if "Oversikt - detaljert" in sheet_data:
            # Hente ut aktuelt ark
            df = sheet_data["Oversikt - detaljert"]

            # Basic overview of dataset
            # df.head()
            # df.info()
        else:
            error_message = "Sheet 'Oversikt - detaljert' not found in the Excel file."
            print(error_message)
            error_messages.append(error_message)
    else:
        error_message = (
            f"Failed to download the file. Status code: {response.status_code}"
        )
        print(error_message)
        error_messages.append(error_message)

except requests.exceptions.RequestException as e:
    error_message = f"Error occurred while downloading the file: {str(e)}"
    print(error_message)
    error_messages.append(error_message)

except Exception as e:
    error_message = f"An unexpected error occurred: {str(e)}"
    print(error_message)
    error_messages.append(error_message)

# Notify yourself of errors, if any
if error_messages:
    notify_errors(error_messages, script_name="Extract_Miljodirektoratet_Data")
else:
    print("All tasks completed successfully.")

########################### HOVED-DATASETT (2009 - d.d.)

# Filter Telemark fylke
df_telemark = df[df["Fylke"] == "Telemark"]

# Rename column name "Utslipp (tonn CO₂-ekvivalenter)" to "Utslipp"
df_telemark.columns = df_telemark.columns.str.replace(
    "Utslipp (tonn CO2-ekvivalenter)", "Utslipp"
)

# Remove columns "Fylke", "Kommunenummer" and "Utslippskilde"
df_telemark = df_telemark.drop(columns=["Fylke", "Kommunenummer","Klimagass"])

# Ensure År is integer type
df_telemark["År"] = df_telemark["År"].astype(int)

# Round the values in the "Utslipp" column to two decimal places
df_telemark["Utslipp"] = df_telemark["Utslipp"].round(2)

########################### DONUT-FIGUR (siste år)

## Funksjon for å gruppere data etter sektor og summere, for henholdsvis nyeste år (recent_year = True) eller året før (recent_year = False)


def beregne_utslipp(recent_year=True):
    # Step 1: Create a copy of the original dataframe
    df_donut = df_telemark.copy()

    # Step 2: Fetch the most recent year in the dataframe and ensure it's an integer
    most_recent_year = int(df_donut["År"].max())

    # Step 3: Determine the target year based on the recent_year flag
    if recent_year:
        target_year = most_recent_year
    else:
        target_year = most_recent_year - 1  

    # Step 4: Filter rows where "År" is the target year
    df_donut = df_donut[df_donut["År"] == target_year]

    # Step 5: Group by 'Sektor' and sum 'Utslipp'
    df_donut = df_donut.groupby(["År", "Sektor"]).agg({"Utslipp": "sum"}).reset_index()

    # Step 6: Add the year to the "Utslipp" column name
    utslipp_column_name = f"Utslipp ({target_year})"  
    df_donut = df_donut.rename(columns={"Utslipp": utslipp_column_name})

    # Step 7: Remove column "År"
    df_donut = df_donut.drop(columns="År")

    # Step 8: Group "Oppvarming", "Luftfart", and "Energiforsyning" into "Annet utslipp"
    sectors_to_group = ["Oppvarming", "Luftfart", "Energiforsyning"]

    # Calculate the sum of "Utslipp" for the selected sectors
    annet_sum = df_donut[df_donut["Sektor"].isin(sectors_to_group)][utslipp_column_name].sum()

    # Remove the original sectors from the DataFrame
    df_donut = df_donut[~df_donut["Sektor"].isin(sectors_to_group)]

    # Append the new row with the combined "Annet utslipp"
    new_row = pd.DataFrame([{"Sektor": "Annet utslipp", utslipp_column_name: annet_sum}])
    df_donut = pd.concat([df_donut, new_row], ignore_index=True)

    # (Optional) Sort the DataFrame if needed
    df_donut = df_donut.sort_values(by=utslipp_column_name, ascending=False).reset_index(drop=True)

    # Round the values in the "Utslipp" column to nearest integer
    df_donut[utslipp_column_name] = df_donut[utslipp_column_name].round()

    return df_donut


df_donut = beregne_utslipp(recent_year=True)  # True = siste år

########################### ENDRING FRA NEST SISTE TIL SISTE

df_nest_siste = beregne_utslipp(recent_year=False)  # False = året før siste år

# Extract the years from the column names (for dynamic column naming)
year_next_most_recent = df_nest_siste.columns[-1][-5:-1]  # I.e. extracts the year (2021) from "Utslipp (2021)"
year_most_recent = df_donut.columns[-1][-5:-1]  # I.e. extracts the year (2022) from "Utslipp (2022)"

## Merge de to tabellene og beregne prosentvis endring

# Step 1: Merge the two DataFrames on "Sektor"
df_pst_endring = pd.merge(df_donut, df_nest_siste, on="Sektor", how="inner")

# Step 2: Rename the columns to reflect the years
df_pst_endring = df_pst_endring.rename(
    columns={f"Utslipp ({year_next_most_recent})": "I fjor", f"Utslipp ({year_most_recent})": "I år"}
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

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name1 = "klimagassutslipp_telemark.csv"
file_name2 = "klimagassutslipp_donut.csv"
file_name3 = "klimagassutslipp_endring.csv"
task_name = "Klima og energi - Sektorvise utslipp"
github_folder = "Data/04_Klima og ressursforvaltning/Klimagassutslipp"
temp_folder = os.environ.get("TEMP_FOLDER")

# Process all files and track their status
is_new_data1 = handle_output_data(df_telemark, file_name1, github_folder, temp_folder, keepcsv=True)
is_new_data2 = handle_output_data(df_donut, file_name2, github_folder, temp_folder, keepcsv=True)
is_new_data3 = handle_output_data(df_pst_endring, file_name3, github_folder, temp_folder, keepcsv=True)

# Write a single status file that indicates if any file has new data
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format - set to "Yes" if any file has new data
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if (is_new_data1 or is_new_data2 or is_new_data3) else 'No'}\n")

# Output results for debugging/testing
if is_new_data1:
    print(f"New data detected in {file_name1} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name1}.")

if is_new_data2:
    print(f"New data detected in {file_name2} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name2}.")

if is_new_data3:
    print(f"New data detected in {file_name3} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name3}.")

print(f"New data status log written to {new_data_status_file}")
