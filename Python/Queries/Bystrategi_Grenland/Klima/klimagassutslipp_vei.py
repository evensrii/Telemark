import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors

from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

## Dette scriptet gir data for donut-figur (csv x2) og stolpediagram (csv x1)

## Mdir gir ingen direkte url til .xlsx-fil, så jeg bruker requests for å simulere nedlasting av filen

url = "https://www.miljodirektoratet.no/globalassets/netttjenester/klimagassutslipp-kommuner/utslippsstatistikk_alle_kommuner.xlsx"

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
        if "Veitrafikk" in sheet_data:
            # Hente ut aktuelt ark
            df = sheet_data["Veitrafikk"]

            # Basic overview of dataset
            # df.head()
            # df.info()
        else:
            error_message = "Sheet 'Veitrafikk' not found in the Excel file."
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
df_telemark = df_telemark.rename(columns={"Utslipp (tonn CO2-ekvivalenter)": "Utslipp"})

# Remove columns "Fylke", "Kommunenummer" and "Utslippskilde"
df_telemark = df_telemark.drop(columns=["Fylke", "Kommunenummer","Klimagass"])

# Ensure År is integer type
df_telemark["År"] = df_telemark["År"].astype(int)

# Round the values in the "Utslipp" column to two decimal places
df_telemark["Utslipp"] = df_telemark["Utslipp"].round(2)


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "klimagassutslipp_vei_telemark.csv"
task_name = "Klima og energi - Utslipp fra vei"
github_folder = "Data/Bystrategi_Grenland/Klima"
temp_folder = os.environ.get("TEMP_FOLDER")

# Process all files and track their status
is_new_data = handle_output_data(df_telemark, file_name, github_folder, temp_folder, keepcsv=True)

# Write a single status file that indicates if any file has new data
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format - set to "Yes" if any file has new data
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print(f"New data detected in {file_name} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name}.")

print(f"New data status log written to {new_data_status_file}")
