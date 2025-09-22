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

# For testing, let's try to find the most recent file
# We'll search for files from the last few months
current_date = pd.Timestamp.now()
found_files = []

# Search for files from the last 6 months
for i in range(6):
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

# Sort files by date (oldest first) to process chronologically
found_files.sort(key=lambda x: x[1], reverse=False)

print(f"Found {len(found_files)} files to process:")
for file_url, file_date in found_files:
    print(f"  - {file_url} (Date: {file_date.strftime('%Y-%m')})")

################# Process all Excel files #################

# Initialize lists to store dataframes from all files
all_job_seeker_dataframes = []
all_job_vacancy_dataframes = []

# Process each file
for file_index, (file_url, file_date) in enumerate(found_files, 1):
    print(f"\n=== Processing file {file_index}/{len(found_files)}: {file_url} ===")
    
    try:
        # Download the Excel file
        response = requests.get(file_url)
        response.raise_for_status()
        excel_content = BytesIO(response.content)
        
        # Check which sheets are available
        excel_file = pd.ExcelFile(excel_content)
        available_sheets = excel_file.sheet_names
        print(f"Available sheets in file: {available_sheets}")
        
        # Initialize dictionaries to store dataframes for this file
        job_seeker_dataframes = {}
        job_vacancy_dataframes = {}
    
    ################# Process job seeker sheets #################
    
    print("\n=== Processing Job Seeker Sheets ===")
    for sheet_name in target_sheets['job_seekers']:
        if sheet_name in available_sheets:
            print(f"Processing sheet: {sheet_name}")
            df = import_job_seeker_sheet(excel_content, sheet_name)
            if not df.empty:
                job_seeker_dataframes[sheet_name] = df
                print(f"Successfully imported {len(df)} rows from {sheet_name}")
                print(f"Columns: {list(df.columns)}")
                print(f"Sample data:\n{df.head()}\n")
            else:
                print(f"No data found in sheet: {sheet_name}\n")
        else:
            print(f"Sheet not found: {sheet_name}\n")
    
    ################# Merge job seeker dataframes #################
    
    if job_seeker_dataframes:
        print("=== Merging Job Seeker DataFrames ===")
        
        # Process each dataframe to add Nivå and Geografisk enhet columns
        processed_dfs = []
        
        for sheet_name, df in job_seeker_dataframes.items():
            df_processed = df.copy()
            
            if 'Landet' in sheet_name:
                # Add Nivå column for national level data
                df_processed['Nivå'] = 'Landet'
                # Rename the geographic column to standard name
                df_processed['Geografisk enhet'] = df_processed['Nåværende Fylke Nr Navn']
                
            elif 'Fylker' in sheet_name:
                # Add Nivå column for county level data
                df_processed['Nivå'] = 'Fylker'
                # Clean up geographic names by removing numeric codes like "03 Oslo" -> "Oslo"
                df_processed['Geografisk enhet'] = df_processed['Nåværende Fylke Nr Navn'].str.replace(r'^\d+\s+', '', regex=True)
                
                # Standardize county names (same mapping as in arbeidsledighet.py)
                county_name_mapping = {
                    'Trøndelag - Trööndelage': 'Trøndelag',
                    'Troms - Romsa - Tromssa ': 'Troms',  # Note: includes trailing space
                    'Finnmark - Finnmárku - Finmarkku': 'Finnmark',
                    'Nordland - Nordlánnda': 'Nordland'
                }
                df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].replace(county_name_mapping)
                
            elif 'Telemark' in sheet_name:
                # Add Nivå column for municipal level data
                df_processed['Nivå'] = 'Telemark kommuner'
                # Clean up geographic names by removing numeric codes like "4001 Porsgrunn" -> "Porsgrunn"
                df_processed['Geografisk enhet'] = df_processed['Nåværende Fylke Nr Navn'].str.replace(r'^\d+\s+', '', regex=True)
            
            # Keep only the standardized columns
            standardized_columns = ['År', 'År-måned', 'Nivå', 'Geografisk enhet', 'Praksis yrke grovgruppe ISCO08', 'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak']
            df_processed = df_processed[standardized_columns]
            
            processed_dfs.append(df_processed)
            print(f"Processed {sheet_name}: {len(df_processed)} rows")
        
        # Combine all processed dataframes
        df_seekers = pd.concat(processed_dfs, axis=0, ignore_index=True)
        
        print(f"\nMerged job seeker data: {len(df_seekers)} total rows")
        print(f"Columns: {list(df_seekers.columns)}")
        print(f"Sample merged data:\n{df_seekers.head()}")
        print(f"Nivå distribution:\n{df_seekers['Nivå'].value_counts()}")
        
    else:
        print("No job seeker dataframes to merge")
        df_seekers = pd.DataFrame()
    
    ################# Process job vacancy sheets #################
    
    print("\n=== Processing Job Vacancy Sheets ===")
    for sheet_name in target_sheets['job_vacancies']:
        if sheet_name in available_sheets:
            print(f"Processing sheet: {sheet_name}")
            df = import_job_vacancy_sheet(excel_content, sheet_name)
            if not df.empty:
                job_vacancy_dataframes[sheet_name] = df
                print(f"Successfully imported {len(df)} rows from {sheet_name}")
                print(f"Columns: {list(df.columns)}")
                print(f"Sample data:\n{df.head()}\n")
            else:
                print(f"No data found in sheet: {sheet_name}\n")
        else:
            print(f"Sheet not found: {sheet_name}\n")
    
    ################# Merge job vacancy dataframes #################
    
    if job_vacancy_dataframes:
        print("=== Merging Job Vacancy DataFrames ===")
        
        # Process each dataframe to add Nivå and Geografisk enhet columns
        processed_vacancy_dfs = []
        
        for sheet_name, df in job_vacancy_dataframes.items():
            df_processed = df.copy()
            
            if 'Landet' in sheet_name:
                # Add Nivå column for national level data
                df_processed['Nivå'] = 'Landet'
                # For Landet, there's no geographic column, so we'll use 'Landet' as the geographic entity
                df_processed['Geografisk enhet'] = 'Landet'
                
            elif 'Fylker' in sheet_name:
                # Rename the 'Empty_column' to 'Nivå' (it should be filled with 'Fylker')
                if 'Empty_column' in df_processed.columns:
                    df_processed = df_processed.rename(columns={'Empty_column': 'Nivå'})
                
                # Use 'Arbeidssted fylkesnavn' as the geographic entity
                df_processed['Geografisk enhet'] = df_processed['Arbeidssted fylkesnavn']
                
                # Clean up geographic names by removing numeric codes if present
                df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].str.replace(r'^\d+\s+', '', regex=True)
                
                # Standardize county names (same mapping as in arbeidsledighet.py)
                county_name_mapping = {
                    'Trøndelag - Trööndelage': 'Trøndelag',
                    'Troms - Romsa - Tromssa ': 'Troms',  # Note: includes trailing space
                    'Finnmark - Finnmárku - Finmarkku': 'Finnmark',
                    'Nordland - Nordlánnda': 'Nordland'
                }
                df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].replace(county_name_mapping)
                
            elif 'Telemark' in sheet_name:
                # Add Nivå column for municipal level data
                df_processed['Nivå'] = 'Telemark kommuner'
                # Use 'Arbeidssted kommunenavn' as the geographic entity
                df_processed['Geografisk enhet'] = df_processed['Arbeidssted kommunenavn']
                
                # Clean up geographic names by removing numeric codes like "4001 Porsgrunn" -> "Porsgrunn"
                df_processed['Geografisk enhet'] = df_processed['Geografisk enhet'].str.replace(r'^\d+\s+', '', regex=True)
            
            # Keep only the standardized columns
            standardized_vacancy_columns = ['År-måned', 'Nivå', 'Geografisk enhet', 'Stilling yrke grovgruppe ISCO08', 'Tilgang stillinger i perioden']
            df_processed = df_processed[standardized_vacancy_columns]
            
            processed_vacancy_dfs.append(df_processed)
            print(f"Processed {sheet_name}: {len(df_processed)} rows")
        
        # Combine all processed dataframes
        df_vacancies = pd.concat(processed_vacancy_dfs, axis=0, ignore_index=True)
        
        print(f"\nMerged job vacancy data: {len(df_vacancies)} total rows")
        print(f"Columns: {list(df_vacancies.columns)}")
        print(f"Sample merged data:\n{df_vacancies.head()}")
        print(f"Nivå distribution:\n{df_vacancies['Nivå'].value_counts()}")
        
    else:
        print("No job vacancy dataframes to merge")
        df_vacancies = pd.DataFrame()
    
    ################# Merge seekers and vacancies #################
    
    if not df_seekers.empty and not df_vacancies.empty:
        print("\n=== Merging Job Seekers and Vacancies ===")
        
        # Prepare seekers data for merge
        df_seekers_prep = df_seekers.copy()
        df_seekers_prep = df_seekers_prep.rename(columns={
            'Praksis yrke grovgruppe ISCO08': 'Yrkespraksis',
            'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak': 'Antall søkere'
        })
        
        # Prepare vacancies data for merge
        df_vacancies_prep = df_vacancies.copy()
        df_vacancies_prep = df_vacancies_prep.rename(columns={
            'Stilling yrke grovgruppe ISCO08': 'Yrke utlyst stilling',
            'Tilgang stillinger i perioden': 'Antall utlyste stillinger'
        })
        
        # Merge on År-måned, Nivå, and Geografisk enhet to get matching records
        df_merged = pd.merge(
            df_seekers_prep[['År', 'År-måned', 'Nivå', 'Geografisk enhet', 'Yrkespraksis', 'Antall søkere']],
            df_vacancies_prep[['År-måned', 'Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger']],
            on=['År-måned', 'Nivå', 'Geografisk enhet'],
            how='outer'
        )
        
        # Convert År-måned to proper date format (YYYY-MM-DD)
        # First clean the data by removing decimals and ensuring proper format
        df_merged['År-måned'] = df_merged['År-måned'].astype(float).astype(int).astype(str)  # Convert to integer to remove decimals
        
        # Create year and month components
        year = df_merged['År-måned'].str[:4]
        month = df_merged['År-måned'].str[4:]
        
        # Create date string in YYYY-MM-DD format (first day of the month)
        date_str = year + '-' + month + '-01'
        
        # Convert to datetime and rename to Dato
        df_merged['Dato'] = pd.to_datetime(date_str, format='%Y-%m-%d')
        
        # Reorder columns to match your specification
        final_columns = ['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger', 'Yrkespraksis', 'Antall søkere', 'Dato']
        df_merged = df_merged[final_columns]
        
        # Sort the merged data for consistent ordering
        df_merged = df_merged.sort_values(
            by=['Dato', 'Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Yrkespraksis'],
            ascending=[True, True, True, True, True]
        ).reset_index(drop=True)
        
        print(f"Merged dataset: {len(df_merged)} total rows")
        print(f"Columns: {list(df_merged.columns)}")
        print(f"Sample merged data:\n{df_merged.head(10)}")
        print(f"\nData summary:")
        print(f"  - Unique Dato values: {df_merged['Dato'].nunique()}")
        print(f"  - Unique Nivå values: {df_merged['Nivå'].nunique()}")
        print(f"  - Unique Geografisk enhet values: {df_merged['Geografisk enhet'].nunique()}")
        print(f"  - Records with both seekers and vacancies: {len(df_merged.dropna())}")
        print(f"  - Records with only seekers data: {len(df_merged[df_merged['Antall utlyste stillinger'].isna() & df_merged['Antall søkere'].notna()])}")
        print(f"  - Records with only vacancy data: {len(df_merged[df_merged['Antall søkere'].isna() & df_merged['Antall utlyste stillinger'].notna()])}")
        
    elif not df_seekers.empty:
        print("\n=== Only Job Seekers Data Available ===")
        df_merged = df_seekers.copy()
        df_merged = df_merged.rename(columns={
            'Praksis yrke grovgruppe ISCO08': 'Yrkespraksis',
            'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak': 'Antall søkere'
        })
        df_merged['Yrke utlyst stilling'] = None
        df_merged['Antall utlyste stillinger'] = None
        
        # Convert År-måned to proper date format
        df_merged['År-måned'] = df_merged['År-måned'].astype(float).astype(int).astype(str)
        year = df_merged['År-måned'].str[:4]
        month = df_merged['År-måned'].str[4:]
        date_str = year + '-' + month + '-01'
        df_merged['Dato'] = pd.to_datetime(date_str, format='%Y-%m-%d')
        
        final_columns = ['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger', 'Yrkespraksis', 'Antall søkere', 'Dato']
        df_merged = df_merged[final_columns]
        print(f"Job seekers only dataset: {len(df_merged)} total rows")
        
    elif not df_vacancies.empty:
        print("\n=== Only Job Vacancies Data Available ===")
        df_merged = df_vacancies.copy()
        df_merged = df_merged.rename(columns={
            'Stilling yrke grovgruppe ISCO08': 'Yrke utlyst stilling',
            'Tilgang stillinger i perioden': 'Antall utlyste stillinger'
        })
        df_merged['Yrkespraksis'] = None
        df_merged['Antall søkere'] = None
        
        # Convert År-måned to proper date format
        df_merged['År-måned'] = df_merged['År-måned'].astype(float).astype(int).astype(str)
        year = df_merged['År-måned'].str[:4]
        month = df_merged['År-måned'].str[4:]
        date_str = year + '-' + month + '-01'
        df_merged['Dato'] = pd.to_datetime(date_str, format='%Y-%m-%d')
        
        final_columns = ['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger', 'Yrkespraksis', 'Antall søkere', 'Dato']
        df_merged = df_merged[final_columns]
        print(f"Job vacancies only dataset: {len(df_merged)} total rows")
        
    else:
        print("\n=== No Data Available for Merging ===")
        df_merged = pd.DataFrame(columns=['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger', 'Yrkespraksis', 'Antall søkere', 'Dato'])
    
    ################# Save results to CSV #################
    
    if not df_merged.empty:
        # Get the script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Create output filename with timestamp
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"soekere_stillinger_yrkespraksis_{timestamp}.csv"
        output_path = os.path.join(script_dir, output_filename)
        
        # Save to CSV with semicolon separator (Norwegian standard)
        df_merged.to_csv(output_path, sep=';', index=False, encoding='utf-8')
        print(f"\n=== CSV Export ===")
        print(f"Saved merged dataset to: {output_path}")
        print(f"Total rows exported: {len(df_merged)}")
        print(f"Columns exported: {list(df_merged.columns)}")
    else:
        print("\n=== CSV Export ===")
        print("No data to export - df_merged is empty")
    
    ################# Summary #################
    
    print("\n=== SUMMARY ===")
    print(f"Job seeker sheets processed: {len(job_seeker_dataframes)}")
    for sheet_name, df in job_seeker_dataframes.items():
        print(f"  - {sheet_name}: {len(df)} rows")
    
    print(f"Job vacancy sheets processed: {len(job_vacancy_dataframes)}")
    for sheet_name, df in job_vacancy_dataframes.items():
        print(f"  - {sheet_name}: {len(df)} rows")
    
except Exception as e:
    error_message = f"Error processing Excel file {latest_file_url}: {str(e)}"
    error_messages.append(error_message)
    print(error_message)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("Failed to process Excel file")

print("\nScript completed successfully!")
