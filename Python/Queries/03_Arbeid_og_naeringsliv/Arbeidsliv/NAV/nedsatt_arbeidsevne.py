import requests
import sys
import os
import glob
import csv
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

def import_excel_sheet(excel_content, sheet_name, range1, range2, column_names):
    """
    Import and process Excel data from two different column ranges.
    
    Args:
        excel_content: BytesIO object containing Excel file
        sheet_name: Name of the sheet to read
        range1: First range of columns (e.g., 'B:I')
        range2: Second range of columns (e.g., 'K:R')
        column_names: List of column names to apply to both ranges
    
    Returns:
        DataFrame with processed and combined data
    """
    df_range1 = pd.read_excel(excel_content, sheet_name=sheet_name, skiprows=1, header=0, usecols=range1)
    df_range2 = pd.read_excel(excel_content, sheet_name=sheet_name, skiprows=1, header=0, usecols=range2)
    
    df_range1.columns = column_names
    df_range2.columns = column_names
    
    df_range2 = df_range2.dropna(how='all')
    df_combined = pd.concat([df_range1, df_range2], axis=0, ignore_index=True)
    
    # Handle numeric columns
    df_combined['Antall personer'] = df_combined['Antall personer'].replace('*', np.nan)
    df_combined['Antall personer'] = pd.to_numeric(df_combined['Antall personer'], errors='coerce').astype('Int64')
    
    # Convert percentage values to numeric
    df_combined['Andel av befolkningen'] = pd.to_numeric(df_combined['Andel av befolkningen'], errors='coerce')
    
    return df_combined

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

################# Import latest data #################

# URL for the CSV file
url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Arbeidsledighet/nedsatt_arbeidsevne.csv"

try:
    # Fetch the data
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Read the CSV data into a pandas DataFrame
    df_nedsatt = pd.read_csv(
        StringIO(response.text),
        sep=',',
        decimal='.',  # Use comma as decimal separator
        thousands=None  # Don't interpret thousands separators
    )
    
except Exception as e:
    error_message = f"Error loading unemployment data: {str(e)}"
    error_messages.append(error_message)
    print(error_message)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("Failed to load unemployment data")

column_names = ['Nivå', 'Geografisk enhet', 'Kjønn', 'Alder', 'Antall personer', 'Andel av befolkningen', 'Dato']
df_nedsatt.columns = column_names

## Handle asterisk values
df_nedsatt['Antall personer'] = df_nedsatt['Antall personer'].replace('*', np.nan)
df_nedsatt['Antall personer'] = pd.to_numeric(df_nedsatt['Antall personer'], errors='coerce').astype('Int64')

## Convert the "Dato" column to datetime
df_nedsatt['Dato'] = pd.to_datetime(df_nedsatt['Dato'])

## Identify the latest date in the dato column
latest_date = df_nedsatt['Dato'].max()
month_year = latest_date.strftime('%Y-%m')
next_months_file = (latest_date + pd.DateOffset(months=2)).strftime('%Y-%m')  # Format as YYYY-MM

################# Import new monthly data, if any #################

# Try all possible day suffixes (01-31) for the monthly file
base_url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Arbeidsledighet/"
new_data_exists = False
url_monthly = None

# Try each day of the month as suffix
for day in range(1, 32):  # 1 to 31
    test_url = f"{base_url}{next_months_file}-{str(day).zfill(2)}.xlsx"
    try:
        response = requests.head(test_url)
        if response.status_code == 200:
            new_data_exists = True
            url_monthly = test_url
            print(f"Found monthly data file: {test_url}")
            break
    except Exception as e:
        continue

if not new_data_exists:
    print(f"No new data found for {next_months_file} with any day suffix")

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
        
        # Remove column "År" from the dataframe
        df_latest_month = df_latest_month.drop(columns=['År'])
        
        # Convert "År-måned" to datetime "Dato" column
        # First clean the data by removing decimals and ensuring proper format
        df_latest_month['År-måned'] = df_latest_month['År-måned'].astype(float).astype(int).astype(str)  # Convert to integer to remove decimals
        
        # Create year and month components
        year = df_latest_month['År-måned'].str[:4]
        month = df_latest_month['År-måned'].str[4:]
        
        # Create date string in YYYY-MM-DD format
        date_str = year + '-' + month + '-01'
        
        # Convert to datetime
        df_latest_month['Dato'] = pd.to_datetime(date_str, format='%Y-%m-%d')
        df_latest_month = df_latest_month.drop(columns=['År-måned'])

        # Ensure values are numeric
        df_latest_month['Andel av arbeidsstyrken'] = pd.to_numeric(df_latest_month['Andel av arbeidsstyrken'], errors='coerce')

        # Divide andel by 100
        df_latest_month['Andel av arbeidsstyrken'] = df_latest_month['Andel av arbeidsstyrken'] / 100

        # Append new data to existing dataset
        df_nedsatt = pd.concat([df_nedsatt, df_latest_month], axis=0, ignore_index=True)

    except Exception as e:
        error_message = f"Error loading unemployment data: {str(e)}"
        error_messages.append(error_message)
        print(error_message)
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError("Failed to load unemployment data")
else:
    print(f"No new data found for {next_months_file}")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "nedsatt_arbeidsevne.csv"
task_name = "NAV - Nedsatt arbeidsevne"
github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/NAV/Arbeidsledighet"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_nedsatt, file_name, github_folder, temp_folder, keepcsv=True)

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