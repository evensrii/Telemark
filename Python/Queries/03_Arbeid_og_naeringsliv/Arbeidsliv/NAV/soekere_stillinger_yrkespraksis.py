import requests
import sys
import os
import glob
import csv
from io import BytesIO
from io import StringIO
import pandas as pd
import numpy as np

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

def import_job_seeker_sheet(excel_content, sheet_name):
    """
    Import job seeker data by occupational background from Excel sheet.
    
    Args:
        excel_content: BytesIO object containing Excel file
        sheet_name: Name of the sheet to read
    
    Returns:
        DataFrame with job seeker data by occupational background
    """
    try:
        # Read the sheet from columns B:G, skipping the first row and using the second row as header
        df = pd.read_excel(excel_content, sheet_name=sheet_name, skiprows=1, header=0, usecols='B:G')
        
        # Define expected column names based on sheet type
        if 'Landet' in sheet_name:
            expected_columns = ['År', 'År-måned', 'Nåværende Fylke Nr Navn', 'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak', 'Praksis yrke grovgruppe ISCO08']
        elif 'Fylker' in sheet_name:
            expected_columns = ['År', 'År-måned', 'Fylker', 'Nåværende Fylke Nr Navn', 'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak', 'Praksis yrke grovgruppe ISCO08']
        elif 'Telemark' in sheet_name:
            expected_columns = ['År', 'År-måned', 'Telemark kommuner', 'Nåværende Fylke Nr Navn', 'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak', 'Praksis yrke grovgruppe ISCO08']
        else:
            expected_columns = ['År', 'År-måned', 'Geografisk enhet', 'Nåværende Fylke Nr Navn', 'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak', 'Praksis yrke grovgruppe ISCO08']
        
        # Rename columns to match expected structure
        if len(df.columns) == len(expected_columns):
            df.columns = expected_columns
        
        # Clean up the data
        df = df.dropna(how='all')  # Remove completely empty rows
        
        # Handle numeric columns - convert asterisks to NaN and ensure proper data types
        df['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak'] = df['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak'].replace('*', np.nan)
        df['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak'] = pd.to_numeric(df['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak'], errors='coerce').astype('float64')
        
        return df
        
    except Exception as e:
        print(f"Error importing sheet {sheet_name}: {str(e)}")
        return pd.DataFrame()

def import_job_vacancy_sheet(excel_content, sheet_name):
    """
    Import job vacancy data from Excel sheet.
    
    Args:
        excel_content: BytesIO object containing Excel file
        sheet_name: Name of the sheet to read
    
    Returns:
        DataFrame with job vacancy data
    """
    try:
        # Define column range and expected columns based on sheet type
        if 'Telemark' in sheet_name:
            # Telemark sheet: B:F columns
            df = pd.read_excel(excel_content, sheet_name=sheet_name, skiprows=1, header=0, usecols='B:F')
            expected_columns = ['År-måned', 'Telemark kommuner', 'Arbeidssted kommunenavn', 'Stilling yrke grovgruppe ISCO08', 'Tilgang stillinger i perioden']
        elif 'Fylker' in sheet_name:
            # Fylker sheet: B:F columns
            df = pd.read_excel(excel_content, sheet_name=sheet_name, skiprows=1, header=0, usecols='B:F')
            expected_columns = ['År-måned', 'Empty_column', 'Arbeidssted fylkesnavn', 'Stilling yrke grovgruppe ISCO08', 'Tilgang stillinger i perioden']
        else:
            # Landet sheet: B:D columns
            df = pd.read_excel(excel_content, sheet_name=sheet_name, skiprows=1, header=0, usecols='B:D')
            expected_columns = ['År-måned', 'Stilling yrke grovgruppe ISCO08', 'Tilgang stillinger i perioden']
        
        # Rename columns to match expected structure
        if len(df.columns) == len(expected_columns):
            df.columns = expected_columns
        
        # Clean up the data
        df = df.dropna(how='all')  # Remove completely empty rows
        
        # Handle numeric columns - convert asterisks to NaN and ensure proper data types
        df['Tilgang stillinger i perioden'] = df['Tilgang stillinger i perioden'].replace('*', np.nan)
        df['Tilgang stillinger i perioden'] = pd.to_numeric(df['Tilgang stillinger i perioden'], errors='coerce').astype('float64')
        
        return df
        
    except Exception as e:
        print(f"Error importing sheet {sheet_name}: {str(e)}")
        return pd.DataFrame()

def check_sheet_exists(excel_content, sheet_name):
    """
    Check if a sheet exists in the Excel file.
    
    Args:
        excel_content: BytesIO object containing Excel file
        sheet_name: Name of the sheet to check
    
    Returns:
        Boolean indicating if sheet exists
    """
    try:
        excel_file = pd.ExcelFile(excel_content)
        return sheet_name in excel_file.sheet_names
    except Exception as e:
        print(f"Error checking sheet {sheet_name}: {str(e)}")
        return False

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Define sheets to process #################

# Define the sheets we want to process
target_sheets = {
    'job_seekers': [
        'Landet yrkesbakgrunn arbeidssøk',
        'Fylker yrkesbakgrunn arbeidssøk', 
        'Telemark yrkesbakgrunn arbeidss'
    ],
    'job_vacancies': [
        'Landet stilling',
        'Fylker stilling',
        'Telemark stilling'
    ]
}

################# Search for Excel files #################

# Use the same file search logic as arbeidsledighet.py
base_url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Arbeidsledighet/"

# Search for all available Excel files from the last 3 years
current_date = pd.Timestamp.now()
found_files = []

# Search for files from the last 36 months (3 years)
for i in range(36):
    check_date = current_date - pd.DateOffset(months=i)
    month_year = check_date.strftime('%Y-%m')
    test_url = f"{base_url}{month_year}-05.xlsx"
    
    try:
        response = requests.head(test_url)
        if response.status_code == 200:
            found_files.append((test_url, check_date))
            print(f"Found file: {test_url}")
    except Exception as e:
        continue

if not found_files:
    print("No Excel files found to process")
    sys.exit(1)

# Sort files by date (oldest first) for chronological processing
found_files.sort(key=lambda x: x[1])
print(f"Found {len(found_files)} Excel files to process")

################# Process all Excel files #################

# Initialize lists to store all dataframes from all files
all_job_seeker_dataframes = []
all_job_vacancy_dataframes = []

for file_url, file_date in found_files:
    print(f"\nProcessing file: {file_url}")
    
    try:
        # Download the Excel file
        response = requests.get(file_url)
        response.raise_for_status()
        excel_content = BytesIO(response.content)
        
        # Check which sheets are available
        excel_file = pd.ExcelFile(excel_content)
        available_sheets = excel_file.sheet_names
        print(f"Available sheets: {available_sheets}")
        
        # Initialize dictionaries to store dataframes for this file
        job_seeker_dataframes = {}
        job_vacancy_dataframes = {}
        
        ################# Process job seeker sheets #################
        
        print("=== Processing Job Seeker Sheets ===")
        for sheet_name in target_sheets['job_seekers']:
            if sheet_name in available_sheets:
                print(f"Processing sheet: {sheet_name}")
                df = import_job_seeker_sheet(excel_content, sheet_name)
                if not df.empty:
                    job_seeker_dataframes[sheet_name] = df
                    print(f"Successfully imported {len(df)} rows from {sheet_name}")
                else:
                    print(f"No data found in sheet: {sheet_name}")
            else:
                print(f"Sheet not found: {sheet_name}")
        
        ################# Process job vacancy sheets #################
        
        print("=== Processing Job Vacancy Sheets ===")
        for sheet_name in target_sheets['job_vacancies']:
            if sheet_name in available_sheets:
                print(f"Processing sheet: {sheet_name}")
                df = import_job_vacancy_sheet(excel_content, sheet_name)
                if not df.empty:
                    job_vacancy_dataframes[sheet_name] = df
                    print(f"Successfully imported {len(df)} rows from {sheet_name}")
                else:
                    print(f"No data found in sheet: {sheet_name}")
            else:
                print(f"Sheet not found: {sheet_name}")
    
        ################# Merge job seeker dataframes for this file #################
        
        if job_seeker_dataframes:
            processed_dfs = []
            
            for sheet_name, df in job_seeker_dataframes.items():
                df_processed = df.copy()
                
                if 'Landet' in sheet_name:
                    df_processed['Nivå'] = 'Landet'
                    df_processed['Geografisk enhet'] = df_processed['Nåværende Fylke Nr Navn']
                elif 'Fylker' in sheet_name:
                    df_processed['Nivå'] = 'Fylker'
                    df_processed['Geografisk enhet'] = df_processed['Nåværende Fylke Nr Navn'].str.replace(r'^\d+\s+', '', regex=True)
                    county_name_mapping = {
                        'Trøndelag - Trööndelage': 'Trøndelag',
                        'Troms - Romsa - Tromssa ': 'Troms',
                        'Finnmark - Finnmárku - Finmarkku': 'Finnmark',
                        'Nordland - Nordlánnda': 'Nordland'
                    }
                    df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].replace(county_name_mapping)
                elif 'Telemark' in sheet_name:
                    df_processed['Nivå'] = 'Telemark kommuner'
                    df_processed['Geografisk enhet'] = df_processed['Nåværende Fylke Nr Navn'].str.replace(r'^\d+\s+', '', regex=True)
                
                standardized_columns = ['År', 'År-måned', 'Nivå', 'Geografisk enhet', 'Praksis yrke grovgruppe ISCO08', 'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak']
                df_processed = df_processed[standardized_columns]
                processed_dfs.append(df_processed)
            
            # Combine all processed dataframes for this file
            df_seekers_file = pd.concat(processed_dfs, axis=0, ignore_index=True)
            all_job_seeker_dataframes.append(df_seekers_file)
            print(f"Merged job seeker data for this file: {len(df_seekers_file)} rows")
        
        ################# Merge job vacancy dataframes for this file #################
        
        if job_vacancy_dataframes:
            processed_vacancy_dfs = []
            
            for sheet_name, df in job_vacancy_dataframes.items():
                df_processed = df.copy()
                
                if 'Landet' in sheet_name:
                    df_processed['Nivå'] = 'Landet'
                    df_processed['Geografisk enhet'] = 'Landet'
                elif 'Fylker' in sheet_name:
                    if 'Empty_column' in df_processed.columns:
                        df_processed = df_processed.rename(columns={'Empty_column': 'Nivå'})
                    df_processed['Geografisk enhet'] = df_processed['Arbeidssted fylkesnavn']
                    df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].str.replace(r'^\d+\s+', '', regex=True)
                    county_name_mapping = {
                        'Trøndelag - Trööndelage': 'Trøndelag',
                        'Troms - Romsa - Tromssa ': 'Troms',
                        'Finnmark - Finnmárku - Finmarkku': 'Finnmark',
                        'Nordland - Nordlánnda': 'Nordland'
                    }
                    df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].replace(county_name_mapping)
                elif 'Telemark' in sheet_name:
                    df_processed['Nivå'] = 'Telemark kommuner'
                    df_processed['Geografisk enhet'] = df_processed['Arbeidssted kommunenavn']
                    df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].str.replace(r'^\d+\s+', '', regex=True)
                
                standardized_vacancy_columns = ['År-måned', 'Nivå', 'Geografisk enhet', 'Stilling yrke grovgruppe ISCO08', 'Tilgang stillinger i perioden']
                df_processed = df_processed[standardized_vacancy_columns]
                processed_vacancy_dfs.append(df_processed)
            
            # Combine all processed dataframes for this file
            df_vacancies_file = pd.concat(processed_vacancy_dfs, axis=0, ignore_index=True)
            all_job_vacancy_dataframes.append(df_vacancies_file)
            print(f"Merged job vacancy data for this file: {len(df_vacancies_file)} rows")
    
    except Exception as e:
        error_message = f"Error processing Excel file {file_url}: {str(e)}"
        error_messages.append(error_message)
        print(error_message)
        # Continue processing other files instead of stopping completely
        continue

################# Combine all files into final datasets #################

print(f"\n=== Combining Data from All Files ===")

# Combine all job seeker dataframes from all files
if all_job_seeker_dataframes:
    df_seekers = pd.concat(all_job_seeker_dataframes, axis=0, ignore_index=True)
    print(f"Combined job seeker data: {len(df_seekers)} total rows from {len(all_job_seeker_dataframes)} files")
else:
    df_seekers = pd.DataFrame()
    print("No job seeker data found in any files")

# Combine all job vacancy dataframes from all files  
if all_job_vacancy_dataframes:
    df_vacancies = pd.concat(all_job_vacancy_dataframes, axis=0, ignore_index=True)
    print(f"Combined job vacancy data: {len(df_vacancies)} total rows from {len(all_job_vacancy_dataframes)} files")
else:
    df_vacancies = pd.DataFrame()
    print("No job vacancy data found in any files")

################# Create final dataset directly #################

print("\n=== Creating Final Dataset with Single Occupation Column ===")

# Create final dataset structure directly from raw data
final_data = []

# Helper function to convert År-måned to proper date format
def convert_date(ar_maaned):
    if pd.isna(ar_maaned):
        return None
    ar_maaned_str = str(int(float(ar_maaned)))  # Convert to integer to remove decimals
    year = ar_maaned_str[:4]
    month = ar_maaned_str[4:]
    return pd.to_datetime(f"{year}-{month}-01", format='%Y-%m-%d')

# Process job seekers data
if not df_seekers.empty:
    print("Processing job seekers data...")
    for _, row in df_seekers.iterrows():
        if pd.notna(row['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak']) and row['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak'] > 0:
            final_data.append({
                'Dato': convert_date(row['År-måned']),
                'Nivå': row['Nivå'],
                'Geografisk enhet': row['Geografisk enhet'],
                'Yrke': row['Praksis yrke grovgruppe ISCO08'],
                'Antall søkere': float(row['Sum helt ledige, delvis ledige og arbeidssøkere på tiltak']),
                'Antall utlyste stillinger': None
            })

# Process job vacancies data
if not df_vacancies.empty:
    print("Processing job vacancies data...")
    for _, row in df_vacancies.iterrows():
        if pd.notna(row['Tilgang stillinger i perioden']) and row['Tilgang stillinger i perioden'] > 0:
            final_data.append({
                'Dato': convert_date(row['År-måned']),
                'Nivå': row['Nivå'],
                'Geografisk enhet': row['Geografisk enhet'],
                'Yrke': row['Stilling yrke grovgruppe ISCO08'],
                'Antall søkere': None,
                'Antall utlyste stillinger': float(row['Tilgang stillinger i perioden'])
            })

# Create final dataframe
if final_data:
    df_final = pd.DataFrame(final_data)
    
    # Group by key dimensions and aggregate (in case there are duplicates)
    df_final = df_final.groupby(['Dato', 'Nivå', 'Geografisk enhet', 'Yrke']).agg({
        'Antall søkere': 'sum',
        'Antall utlyste stillinger': 'sum'
    }).reset_index()
    
    # Replace 0 with None for cleaner data
    df_final['Antall søkere'] = df_final['Antall søkere'].replace(0, None)
    df_final['Antall utlyste stillinger'] = df_final['Antall utlyste stillinger'].replace(0, None)
    
    # Ensure proper data types (following memory about float64 for github_functions compatibility)
    df_final['Antall søkere'] = df_final['Antall søkere'].astype('float64')
    df_final['Antall utlyste stillinger'] = df_final['Antall utlyste stillinger'].astype('float64')
    
    # Sort the data
    df_final = df_final.sort_values(['Dato', 'Nivå', 'Geografisk enhet', 'Yrke']).reset_index(drop=True)
    
    print(f"Created final dataset with {len(df_final)} rows")
    print(f"Columns: {list(df_final.columns)}")
    print(f"Sample data:\n{df_final.head(10)}")
    print(f"Unique occupation categories: {df_final['Yrke'].nunique()}")
    print(f"Date range: {df_final['Dato'].min()} to {df_final['Dato'].max()}")
    
    # Show summary by geographic level
    print(f"\nData distribution by geographic level:")
    print(df_final['Nivå'].value_counts())
    
    # Show sample of occupation categories
    print(f"\nSample occupation categories:")
    print(df_final['Yrke'].value_counts().head(10))
    
else:
    print("No data available for creating final dataset")
    df_final = pd.DataFrame(columns=['Dato', 'Nivå', 'Geografisk enhet', 'Yrke', 'Antall søkere', 'Antall utlyste stillinger'])

print("\n=== Processing completed for all files ===")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

if 'df_final' in locals() and not df_final.empty:
    # Sort the final dataset for consistent ordering
    df_final = df_final.sort_values(
        by=['Dato', 'Nivå', 'Geografisk enhet', 'Yrke'],
        ascending=[True, True, True, True]
    ).reset_index(drop=True)

    file_name = "soekere_stillinger_yrkespraksis.csv"
    task_name = "Opplaering og kompetanse - Sokere og stillinger etter yrkespraksis"
    github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/NAV/Arbeidsledighet"
    temp_folder = os.environ.get("TEMP_FOLDER")

    # Create a copy for comparison and ensure proper data types
    df_compare = df_final.copy()
    
    # Convert any Int64 columns to regular int to avoid type conflicts
    for col in df_compare.select_dtypes(include=['Int64']).columns:
        df_compare[col] = df_compare[col].fillna(0).astype('int64')

    # Specify which columns contain the actual values we want to compare
    value_columns = df_compare.select_dtypes(include=['Int64', 'float64']).columns.tolist()

    print(f"\n=== Saving Final Dataset ===")
    print(f"Final dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
    print(f"Saving as: {file_name}")
    print(f"Value columns for comparison: {value_columns}")

    # Call the function and get the "New Data" status
    is_new_data = handle_output_data(df_compare, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns)

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

else:
    print("\n=== No Data to Save ===")
    print("Final dataset is empty or not created - nothing to save")

print("\nScript completed successfully!")