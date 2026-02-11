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
import warnings

# Suppress openpyxl warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.email_functions import notify_errors
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

    ## Identify existing months and find gaps
    # Extract year-month strings from existing data
    existing_months = set(df_nedsatt['Dato'].dt.strftime('%Y-%m').unique())
    latest_date = df_nedsatt['Dato'].max()
    earliest_date = df_nedsatt['Dato'].min()

    print(f"Existing data range: {earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")

    # Define the search range: last 2 years up to current date + 3 months
    current_date = pd.Timestamp.now()
    search_start_date = current_date - pd.DateOffset(years=2)
    search_end_date = current_date + pd.DateOffset(months=3)

    # Generate expected monthly sequence for the search period
    expected_months = []
    check_date = search_start_date.replace(day=1)
    while check_date <= search_end_date:
        expected_months.append(check_date.strftime('%Y-%m'))
        check_date = check_date + pd.DateOffset(months=1)

    # Find missing months
    missing_months = []
    for month_str in expected_months:
        if month_str not in existing_months:
            missing_months.append(month_str)

    print(f"Missing months in dataset: {missing_months}")
    if any(month >= current_date.strftime('%Y-%m') for month in missing_months):
        print("(These may not be available yet.)")

    ################# Search for files to fill gaps and add new data #################

    base_url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Nedsatt%20arbeidsevne/"
    found_files = []

    # Find files that could contain data for missing months
    # Use a set to track unique files and avoid duplicates
    unique_files = set()
    found_files = []
    
    # First, collect all potential files (both gap-filling and new data)
    all_potential_files = set()
    
    # For each missing month, check for files 1-3 months later
    for missing_month in missing_months:
        missing_date = pd.to_datetime(missing_month + '-01')
        
        # Check files 1-3 months after the missing month
        for months_ahead in range(1, 4):  # 1, 2, 3 months ahead
            check_date = missing_date + pd.DateOffset(months=months_ahead)
            
            # Look for files with both -16.xlsx and -17.xlsx patterns
            for pattern in ['-16.xlsx', '-17.xlsx']:
                filename_pattern = check_date.strftime('%Y-%m') + pattern
                all_potential_files.add(filename_pattern)
    
    # Also check for new data files (up to 3 months ahead of latest data)
    if latest_date:
        for months_ahead in range(1, 4):  # 1, 2, 3 months ahead
            future_date = latest_date + pd.DateOffset(months=months_ahead)
            for pattern in ['-16.xlsx', '-17.xlsx']:
                filename_pattern = future_date.strftime('%Y-%m') + pattern
                all_potential_files.add(filename_pattern)
    
    # Get list of available files from GitHub directory using GitHub API
    
    try:
        # Use GitHub API to get directory contents
        api_url = "https://api.github.com/repos/evensrii/Telemark/contents/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Nedsatt%20arbeidsevne"
        response = requests.get(api_url)
        response.raise_for_status()
        
        files_data = response.json()
        available_files = []
        
        # Process each file in the directory
        for file_info in files_data:
            if file_info['type'] == 'file' and file_info['name'].endswith('.xlsx'):
                filename = file_info['name']
                if filename.endswith('-16.xlsx') or filename.endswith('-17.xlsx'):
                    # Extract date from filename (YYYY-MM-16.xlsx or YYYY-MM-17.xlsx)
                    try:
                        date_part = filename.replace('-16.xlsx', '').replace('-17.xlsx', '')
                        file_publication_date = pd.to_datetime(filename.replace('.xlsx', ''), format='%Y-%m-%d')
                        data_month = pd.to_datetime(date_part + '-01', format='%Y-%m-%d')
                        
                        # Construct the raw GitHub URL
                        raw_url = base_url + filename
                        
                        available_files.append((filename, raw_url, file_publication_date, data_month))
                    except Exception as parse_error:
                        print(f"Error parsing filename {filename}: {parse_error}")
                        continue
        
        print(f"Found {len(available_files)} Excel files in GitHub directory")
        
    except Exception as e:
        print(f"Error fetching file list from GitHub API: {e}")
        available_files = []
    
    # Now find files that match our potential file patterns
    for filename_pattern in all_potential_files:
        for filename, url_monthly, file_publication_date, data_month in available_files:
            if filename == filename_pattern and filename not in unique_files:
                unique_files.add(filename)
                found_files.append((filename, url_monthly, file_publication_date, data_month))
                print(f"Found file to process: {filename} (contains data for {data_month.strftime('%Y-%m')})")
                break
    

    print(f"Total files found to process: {len(found_files)}")
    
    # Sort files by data month to process them in chronological order
    found_files.sort(key=lambda x: x[3])  # Sort by data_month
    
    if not found_files:
        print(f"No new Excel files found with data for missing months")
    else:
        print(f"\nFiles to be processed:")
        for filename, url_monthly, file_publication_date, data_month in found_files:
            print(f"  - {filename}: Contains {data_month.strftime('%B %Y')} data (published {file_publication_date.strftime('%Y-%m-%d')})")

    # Process each found file
    for filename, url_monthly, file_publication_date, data_month in found_files:
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
            
            # Filter out data that already exists in the dataset
            new_dates = df_latest_month['Dato'].unique()
            existing_dates = df_nedsatt['Dato'].unique()
            dates_to_add = [date for date in new_dates if date not in existing_dates]
            
            if dates_to_add:
                df_new_data = df_latest_month[df_latest_month['Dato'].isin(dates_to_add)]
                df_nedsatt = pd.concat([df_nedsatt, df_new_data], axis=0, ignore_index=True)
                print(f"Successfully processed {filename} - Added data for dates: {[pd.Timestamp(d).strftime('%Y-%m-%d') for d in dates_to_add]}")
            else:
                print(f"Data from {filename} already exists in dataset - skipping")

        except Exception as e:
            error_message = f"Error loading data from {filename}: {str(e)}"
            error_messages.append(error_message)
            print(error_message)
            notify_errors(error_messages, script_name=script_name)
            raise RuntimeError(f"Failed to load data from {filename}")
    
    if not found_files:
        print("No new Excel files were processed.")
    
    # Sort the final dataset by date and other key columns for consistency
    df_nedsatt = df_nedsatt.sort_values(['Dato', 'Nivå', 'Geografisk enhet', 'Kjønn', 'Alder'], ignore_index=True)

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