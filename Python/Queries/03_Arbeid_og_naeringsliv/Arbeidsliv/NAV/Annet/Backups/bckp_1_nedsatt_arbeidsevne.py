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
        range1: First range of columns (e.g., 'A:I')
        range2: Second range of columns (e.g., 'K:S')
        column_names: List of column names to apply to both ranges
    
    Returns:
        DataFrame with processed and combined data
    """
    # Read both ranges from Excel with proper range and header, applying column names directly
    df_range1 = pd.read_excel(excel_content, sheet_name=sheet_name, usecols=range1, skiprows=1, names=column_names)
    df_range2 = pd.read_excel(excel_content, sheet_name=sheet_name, usecols=range2, skiprows=1, names=column_names)
    
    # Drop rows where all values are NaN
    df_range2 = df_range2.dropna(how='all')
    
    # Combine the dataframes
    df_combined = pd.concat([df_range1, df_range2], axis=0, ignore_index=True)
    
    # Handle numeric columns
    df_combined['Antall personer'] = df_combined['Antall personer'].replace('*', np.nan)
    df_combined['Antall personer'] = pd.to_numeric(df_combined['Antall personer'], errors='coerce')  # Keep as float
    
    # Convert percentage values to numeric
    df_combined['Andel av befolkningen'] = pd.to_numeric(df_combined['Andel av befolkningen'], errors='coerce')
    
    # Convert Dato column to proper datetime format
    # First convert to integer to remove decimals
    df_combined['Dato'] = df_combined['Dato'].astype(float).astype(int).astype(str)
    # Extract year and month
    df_combined['year'] = df_combined['Dato'].str[:4]
    df_combined['month'] = df_combined['Dato'].str[4:]
    # Create date string in YYYY-MM-DD format with day=01
    df_combined['Dato'] = df_combined['year'] + '-' + df_combined['month'] + '-01'
    # Convert to datetime
    df_combined['Dato'] = pd.to_datetime(df_combined['Dato'], format='%Y-%m-%d')
    # Drop temporary columns
    df_combined = df_combined.drop(['year', 'month'], axis=1)
    
    # Drop the unnecessary columns
    df_combined = df_combined.drop(['Månednummer', 'År'], axis=1)
    
    return df_combined

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

try:
    ################# Import latest data #################

    # URL for the CSV file
    url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Nedsatt%20arbeidsevne/nedsatt_arbeidsevne.csv"

    # Fetch the data
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Read the CSV data into a pandas DataFrame
    df_nedsatt = pd.read_csv(
        StringIO(response.text),
        sep=',',
        decimal='.',  # Use comma as decimal separator
        thousands=None,  # Don't interpret thousands separators
        low_memory=False,  # Prevent mixed type inference warning
        dtype={
            'Antall personer': str,  # Convert to numeric later
            'Andel av befolkningen': str  # Convert to numeric later
        }
    )
    
    # Set column names
    column_names = ['Nivå','Geografisk enhet','Kjønn','Alder','Antall personer','Andel av befolkningen','Dato']
    df_nedsatt.columns = column_names

    ## Handle asterisk values and convert to numeric
    df_nedsatt['Antall personer'] = df_nedsatt['Antall personer'].replace('*', np.nan)
    df_nedsatt['Antall personer'] = pd.to_numeric(df_nedsatt['Antall personer'], errors='coerce')  # Keep as float

    ## Convert percentage values to numeric
    df_nedsatt['Andel av befolkningen'] = pd.to_numeric(df_nedsatt['Andel av befolkningen'], errors='coerce')

    ## Convert the "Dato" column to datetime with consistent format
    df_nedsatt['Dato'] = pd.to_datetime(df_nedsatt['Dato'])

    ## Identify the latest date in the dato column
    latest_date = df_nedsatt['Dato'].max()
    month_year = latest_date.strftime('%Y-%m')
    next_months_file = (latest_date + pd.DateOffset(months=2)).strftime('%Y-%m')  # Format as YYYY-MM

    ################# Import new monthly data, if any #################

    # Try all possible day suffixes (01-31) for the monthly file
    base_url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Nedsatt%20arbeidsevne/"
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
            
            # Define column names for the dataframe - these match the exact order in Excel (A:I and K:S)
            column_names = ['Månednummer', 'År', 'Nivå', 'Geografisk enhet', 'Kjønn', 'Alder', 'Antall personer', 'Andel av befolkningen', 'Dato']

            # Import data for each sheet using the function
            df_landet_18_29 = import_excel_sheet(BytesIO(response.content), 'Nedsatt 18 - 29 år landet', 'A:I', 'K:S', column_names)
            df_landet_18_66 = import_excel_sheet(BytesIO(response.content), 'Nedsatt 18 - 66 år landet', 'A:I', 'K:S', column_names)
            df_fylker_18_29 = import_excel_sheet(BytesIO(response.content), 'Nedsatt 18 - 29 år fylker', 'A:I', 'K:S', column_names)
            df_fylker_18_66 = import_excel_sheet(BytesIO(response.content), 'Nedsatt 18 - 66 år fylker', 'A:I', 'K:S', column_names)
            df_telemark_18_29 = import_excel_sheet(BytesIO(response.content), 'Nedsatt 18 - 29 år Telemark', 'A:I', 'K:S', column_names)       
            df_telemark_18_66 = import_excel_sheet(BytesIO(response.content), 'Nedsatt 18 - 66 år Telemark', 'A:I', 'K:S', column_names)

            # Replace values "Alle aldre" with "18 - 66 år" in df_landet_18_66, df_fylker_18_66, df_telemark_18_66
            df_landet_18_66['Alder'] = df_landet_18_66['Alder'].replace('Alle aldre', '18 - 66 år')
            df_fylker_18_66['Alder'] = df_fylker_18_66['Alder'].replace('Alle aldre', '18 - 66 år')
            df_telemark_18_66['Alder'] = df_telemark_18_66['Alder'].replace('Alle aldre', '18 - 66 år')

            # Replace values "Alle aldre" with "18 - 29 år" in df_landet_18_29, df_fylker_18_29, df_telemark_18_29
            df_landet_18_29['Alder'] = df_landet_18_29['Alder'].replace('Under 30 år', '18 - 29 år')
            df_fylker_18_29['Alder'] = df_fylker_18_29['Alder'].replace('Under 30 år', '18 - 29 år')
            df_telemark_18_29['Alder'] = df_telemark_18_29['Alder'].replace('Under 30 år', '18 - 29 år')

            # Stack all dataframes vertically
            df_latest_month = pd.concat([df_landet_18_29, df_landet_18_66, df_fylker_18_29, df_fylker_18_66, df_telemark_18_29, df_telemark_18_66], axis=0, ignore_index=True)

            # Standardize county names
            county_name_mapping = {
                'Trøndelag - Trööndelage': 'Trøndelag',
                'Troms - Romsa - Tromssa ': 'Troms',  # Note: includes trailing space
                'Finnmark - Finnmárku - Finmarkku': 'Finnmark',
                'Nordland - Nordlánnda': 'Nordland'
            }
            
            df_latest_month['Geografisk enhet'] = df_latest_month['Geografisk enhet'].replace(county_name_mapping)
            
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
    github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/NAV/Nedsatt arbeidsevne"
    temp_folder = os.environ.get("TEMP_FOLDER")

    # Create a copy for comparison, keeping numeric types
    df_compare = df_nedsatt.copy()
    
    # Ensure consistent numeric types
    df_compare['Antall personer'] = pd.to_numeric(df_compare['Antall personer'], errors='coerce')
    df_compare['Andel av befolkningen'] = pd.to_numeric(df_compare['Andel av befolkningen'], errors='coerce')
    
    # Call the function and get the "New Data" status
    is_new_data = handle_output_data(df_compare, file_name, github_folder, temp_folder, keepcsv=True, value_columns=['Antall personer', 'Andel av befolkningen'])  # Specify which columns to compare

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

except Exception as e:
    error_message = f"Error loading unemployment data: {str(e)}"
    error_messages.append(error_message)
    print(error_message)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("Failed to load unemployment data")