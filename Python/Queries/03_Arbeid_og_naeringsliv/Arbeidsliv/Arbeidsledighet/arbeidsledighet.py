import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
import numpy as np

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import compare_to_github
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

################# Import latest data #################

# URL for the CSV file
url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Arbeidsledighet/arbeidsledighet.csv"

try:
    # Fetch the data
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Read the CSV data into a pandas DataFrame
    df_ledighet = pd.read_csv(StringIO(response.text), sep=';')
    
except Exception as e:
    error_message = f"Error loading unemployment data: {str(e)}"
    error_messages.append(error_message)
    print(error_message)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("Failed to load unemployment data")

## Convert the "Dato" column to datetime
df_ledighet['Dato'] = pd.to_datetime(df_ledighet['Dato'], format='%d.%m.%Y')

## Identify the latest date in the dato column
latest_date = df_ledighet['Dato'].max()
month_year = latest_date.strftime('%Y_%m')
next_month_year = (latest_date + pd.DateOffset(months=1)).strftime('%Y_%m')

################# Import new monthly data, if any #################

url_monthly = f"https://github.com/evensrii/Telemark/raw/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Arbeidsledighet/arbeidsledighet_{next_month_year}.xlsx"

# Check if new monthly data exists
try:
    response = requests.head(url_monthly)
    new_data_exists = response.status_code == 200
except Exception as e:
    new_data_exists = False
    print(f"Error checking for new data: {str(e)}")

if new_data_exists:
    try:
        response = requests.get(url_monthly)
        response.raise_for_status()
        
        # Column names for the dataframes
        column_names = ['År', 'År-måned', 'Nivå', 'Geografisk enhet', 'Arbeidsmarkedsstatus', 'Kjønn', 'Antall personer', 'Andel av arbeidsstyrken']
        
        # Import data for each sheet using the function
        df_fylker = import_excel_sheet(BytesIO(response.content), 'Fylker', 'B:I', 'K:R', column_names)
        df_landet = import_excel_sheet(BytesIO(response.content), 'Landet', 'B:I', 'K:R', column_names)
        df_telemark = import_excel_sheet(BytesIO(response.content), 'Telemark', 'B:I', 'K:R', column_names)
        
        # Stack all dataframes vertically
        df_latest_month = pd.concat([df_fylker, df_landet, df_telemark], axis=0, ignore_index=True)
        
        # Append new data to existing dataset
        df_ledighet = pd.concat([df_ledighet, df_latest_month], axis=0, ignore_index=True)
        
    except Exception as e:
        error_message = f"Error loading unemployment data: {str(e)}"
        error_messages.append(error_message)
        print(error_message)
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError("Failed to load unemployment data")
else:
    print(f"No new data found for {next_month_year}")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "arbeidsledighet.csv"
task_name = "NAV - Arbeidsledighet"
github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/NAV/Arbeidsledighet"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_ledighet, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")