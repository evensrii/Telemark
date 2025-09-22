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
github_repo = "evensrii/Telemark"

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

################# Check for existing data and find missing months #################

print("=== Checking for Existing Data ===")

# Try to get existing data from GitHub to determine what we need to fetch
github_file_url = f"https://raw.githubusercontent.com/{github_repo}/refs/heads/main/Data/02_Oppl%C3%A6ring%20og%20kompetanse/Kompetansebehov/soekere_stillinger_yrkespraksis.csv"

existing_data = None
latest_existing_date = None
missing_months = []

try:
    response = requests.get(github_file_url)
    if response.status_code == 200:
        # Try semicolon separator first, then comma if that fails
        try:
            existing_data = pd.read_csv(StringIO(response.text), sep=';')
        except:
            existing_data = pd.read_csv(StringIO(response.text), sep=',')
        
        # Check if we have actual data (more than just headers)
        if len(existing_data) > 0 and 'Dato' in existing_data.columns:
            # Convert Dato column to datetime for analysis
            existing_data['Dato'] = pd.to_datetime(existing_data['Dato'])
            
            # Find the latest date in existing data
            latest_existing_date = existing_data['Dato'].max()
            print(f"Latest existing data: {latest_existing_date.strftime('%Y-%m')}")
            
            # Find missing months within the last 2 years
            current_date = pd.Timestamp.now()
            analysis_start = current_date - pd.DateOffset(years=2)
            
            # Create complete monthly sequence
            all_months = pd.date_range(start=analysis_start, end=current_date, freq='MS')
            existing_months = pd.to_datetime(existing_data['Dato'].dt.to_period('M').astype(str))
            
            missing_months = []
            for month in all_months:
                month_key = month.strftime('%Y-%m')
                if not any(existing_months.dt.strftime('%Y-%m') == month_key):
                    missing_months.append(month_key)
            
            if missing_months:
                print(f"Found {len(missing_months)} missing months in existing data:")
                for month in missing_months:
                    print(f"  - {month}")
            else:
                print("No gaps found in existing data within the last 2 years")
        else:
            print("Existing CSV file found but contains no data - will fetch all available data")
            latest_existing_date = None
            missing_months = []
    else:
        print("No existing data found on GitHub - will fetch all available data")
        
except Exception as e:
    print(f"Could not fetch existing data: {e}")
    print("Will proceed to fetch all available data")

################# Search for files to process #################

print(f"\n=== Searching for Files to Process ===")

base_url = f"https://raw.githubusercontent.com/{github_repo}/refs/heads/main/Data/03_Arbeid%20og%20n%C3%A6ringsliv/01_Arbeidsliv/NAV/Arbeidsledighet/"

found_files = []

# Search for files that could contain data for missing months
# Files are typically published 1-2 months after the data period
for missing_month in missing_months:
    # Convert missing month string to timestamp for date arithmetic
    missing_date = pd.Timestamp(missing_month + '-01')
    
    # Check for files 1-3 months after the missing data month
    for offset in range(1, 4):  # 1, 2, 3 months after
        file_date = missing_date + pd.DateOffset(months=offset)
        month_year_to_check = file_date.strftime('%Y-%m')
        
        # Check for file with -05 suffix (standard pattern)
        test_url = f"{base_url}{month_year_to_check}-05.xlsx"
        try:
            response = requests.head(test_url)
            if response.status_code == 200:
                # Check if we haven't already found this file
                if not any(url == test_url for url, _ in found_files):
                    found_files.append((test_url, file_date))
                    print(f"Found file for missing month {missing_month}: {test_url}")
        except Exception as e:
            continue

# Also search for new files after the latest existing date
if latest_existing_date:
    search_start = latest_existing_date + pd.DateOffset(months=1)
    search_end = pd.Timestamp.now() + pd.DateOffset(months=6)
    
    print(f"Searching for new files from {search_start.strftime('%Y-%m')} onwards...")
    
    search_range = pd.date_range(start=search_start, end=search_end, freq='MS')
    for search_date in search_range:
        # Files are typically published 1-2 months after the data period
        # So search for files that could contain this month's data
        for offset in range(1, 4):
            file_date = search_date + pd.DateOffset(months=offset)
            if file_date > pd.Timestamp.now() + pd.DateOffset(months=6):
                break
                
            month_year_to_check = file_date.strftime('%Y-%m')
            test_url = f"{base_url}{month_year_to_check}-05.xlsx"
            
            try:
                response = requests.head(test_url)
                if response.status_code == 200:
                    if not any(url == test_url for url, _ in found_files):
                        found_files.append((test_url, file_date))
                        print(f"Found new file: {test_url} (likely contains data for {search_date.strftime('%Y-%m')})")
                        break  # Found file for this month, no need to check further offsets
            except Exception as e:
                continue
else:
    # No existing data, search for all available files in the last 2 years
    print("No existing data found - searching for all available files...")
    current_date = pd.Timestamp.now()
    start_date = current_date - pd.DateOffset(years=2)
    end_date = current_date + pd.DateOffset(months=6)
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    for search_date in date_range:
        file_date_str = search_date.strftime('%Y-%m')
        test_url = f"{base_url}{file_date_str}-05.xlsx"
        
        try:
            response = requests.head(test_url)
            if response.status_code == 200:
                found_files.append((test_url, search_date))
                print(f"Found file: {test_url}")
        except Exception as e:
            continue

print(f"Total files found to process: {len(found_files)}")
new_data_exists = len(found_files) > 0

if not found_files:
    print("No new files found to process.")
    print("Script completed - no new data to fetch.")
    exit()

# Sort found files by date and process all of them
found_files.sort(key=lambda x: x[1])  # Sort by date

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
        
        ################# Process data from this file #################
        
        # If we found job seeker data, process it
        if job_seeker_dataframes:
            print("=== Processing Job Seeker DataFrames from this file ===")
            
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
            
            if processed_dfs:
                df_seekers_file = pd.concat(processed_dfs, axis=0, ignore_index=True)
                all_job_seeker_dataframes.append(df_seekers_file)
                print(f"Added {len(df_seekers_file)} job seeker rows from this file")
        
        # If we found job vacancy data, process it
        if job_vacancy_dataframes:
            print("=== Processing Job Vacancy DataFrames from this file ===")
            
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
            
            if processed_vacancy_dfs:
                df_vacancies_file = pd.concat(processed_vacancy_dfs, axis=0, ignore_index=True)
                all_job_vacancy_dataframes.append(df_vacancies_file)
                print(f"Added {len(df_vacancies_file)} job vacancy rows from this file")
    
    except Exception as e:
        error_message = f"Error processing Excel file {file_url}: {str(e)}"
        error_messages.append(error_message)
        print(error_message)
        # Continue processing other files even if one fails
        continue

################# Combine data from all files #################

print(f"\n=== Combining Data from All {len(found_files)} Files ===")

# Combine job seeker data from all files
if all_job_seeker_dataframes:
    df_seekers = pd.concat(all_job_seeker_dataframes, axis=0, ignore_index=True)
    print(f"Combined job seeker data: {len(df_seekers)} total rows from {len(all_job_seeker_dataframes)} files")
else:
    print("No job seeker data found across all files")
    df_seekers = pd.DataFrame()

# Combine job vacancy data from all files
if all_job_vacancy_dataframes:
    df_vacancies = pd.concat(all_job_vacancy_dataframes, axis=0, ignore_index=True)
    print(f"Combined job vacancy data: {len(df_vacancies)} total rows from {len(all_job_vacancy_dataframes)} files")
else:
    print("No job vacancy data found across all files")
    df_vacancies = pd.DataFrame()

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
    
elif not df_seekers.empty:
    print("\n=== Only Job Seekers Data Available ===")
    df_merged = df_seekers.copy()
    df_merged = df_merged.rename(columns={
        'Praksis yrke grovgruppe ISCO08': 'Yrkespraksis',
        'Sum helt ledige, delvis ledige og arbeidssøkere på tiltak': 'Antall søkere'
    })
    df_merged['Yrke utlyst stilling'] = None
    df_merged['Antall utlyste stillinger'] = None
    
elif not df_vacancies.empty:
    print("\n=== Only Job Vacancies Data Available ===")
    df_merged = df_vacancies.copy()
    df_merged = df_merged.rename(columns={
        'Stilling yrke grovgruppe ISCO08': 'Yrke utlyst stilling',
        'Tilgang stillinger i perioden': 'Antall utlyste stillinger'
    })
    df_merged['Yrkespraksis'] = None
    df_merged['Antall søkere'] = None
    
else:
    print("\n=== No Data Available for Merging ===")
    df_merged = pd.DataFrame(columns=['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger', 'Yrkespraksis', 'Antall søkere', 'Dato'])

# Convert År-måned to proper date format if we have data
if not df_merged.empty and 'År-måned' in df_merged.columns:
    df_merged['År-måned'] = df_merged['År-måned'].astype(float).astype(int).astype(str)
    year = df_merged['År-måned'].str[:4]
    month = df_merged['År-måned'].str[4:]
    date_str = year + '-' + month + '-01'
    df_merged['Dato'] = pd.to_datetime(date_str, format='%Y-%m-%d')
    
    # Reorder columns
    final_columns = ['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Antall utlyste stillinger', 'Yrkespraksis', 'Antall søkere', 'Dato']
    df_merged = df_merged[final_columns]
    
    # Sort the merged data
    df_merged = df_merged.sort_values(
        by=['Dato', 'Nivå', 'Geografisk enhet'],
        ascending=[True, True, True]
    ).reset_index(drop=True)
    
    print(f"Final merged dataset: {len(df_merged)} total rows")
    print(f"Date range: {df_merged['Dato'].min()} to {df_merged['Dato'].max()}")

################# Transform data for chart format #################

print("\n=== Transforming Data for Chart Format ===")

if not df_merged.empty:
    # Step 1: Aggregate job seekers by their occupational background (Yrkespraksis)
    print("Aggregating job seekers by occupational background...")
    seekers_agg = df_merged.groupby(['Nivå', 'Geografisk enhet', 'Yrkespraksis', 'Dato']).agg({
        'Antall søkere': 'first'  # Since all rows for same Yrkespraksis have same value
    }).reset_index()
    seekers_agg = seekers_agg.rename(columns={'Yrkespraksis': 'Yrke'})
    
    # Step 2: Aggregate job openings by occupation (Yrke utlyst stilling)
    print("Aggregating job openings by occupation...")
    openings_agg = df_merged.groupby(['Nivå', 'Geografisk enhet', 'Yrke utlyst stilling', 'Dato']).agg({
        'Antall utlyste stillinger': 'first'  # Since all rows for same occupation have same value
    }).reset_index()
    openings_agg = openings_agg.rename(columns={'Yrke utlyst stilling': 'Yrke'})
    
    # Step 3: Check for mismatches between occupation categories
    print("\nAnalyzing occupation category mismatches...")
    
    # Get unique occupations from each dataset
    seekers_occupations = set(seekers_agg['Yrke'].unique())
    openings_occupations = set(openings_agg['Yrke'].unique())
    
    # Find mismatches
    seekers_only = seekers_occupations - openings_occupations
    openings_only = openings_occupations - seekers_occupations
    common_occupations = seekers_occupations & openings_occupations
    
    print(f"Total occupation categories in job seekers data: {len(seekers_occupations)}")
    print(f"Total occupation categories in job openings data: {len(openings_occupations)}")
    print(f"Common occupation categories: {len(common_occupations)}")
    
    if seekers_only:
        print(f"\nOccupations with job seekers but NO job openings ({len(seekers_only)}):")
        for occ in sorted(seekers_only):
            count = seekers_agg[seekers_agg['Yrke'] == occ]['Antall søkere'].sum()
            print(f"  - {occ}: {count} job seekers")
    
    if openings_only:
        print(f"\nOccupations with job openings but NO job seekers ({len(openings_only)}):")
        for occ in sorted(openings_only):
            count = openings_agg[openings_agg['Yrke'] == occ]['Antall utlyste stillinger'].sum()
            print(f"  - {occ}: {count} job openings")
    
    # Step 4: Merge job seekers and job openings data
    print(f"\nMerging aggregated data...")
    df_chart = pd.merge(
        seekers_agg, 
        openings_agg, 
        on=['Nivå', 'Geografisk enhet', 'Yrke', 'Dato'], 
        how='outer'
    )
    
    # Fill NaN values with 0 for missing data
    df_chart['Antall søkere'] = df_chart['Antall søkere'].fillna(0)
    df_chart['Antall utlyste stillinger'] = df_chart['Antall utlyste stillinger'].fillna(0)
    
    # Reorder columns for better readability
    df_chart = df_chart[['Nivå', 'Geografisk enhet', 'Yrke', 'Antall søkere', 'Antall utlyste stillinger', 'Dato']]
    
    # Sort by number of job seekers (descending) for better chart ordering
    df_chart = df_chart.sort_values(['Dato', 'Nivå', 'Geografisk enhet', 'Antall søkere'], 
                                   ascending=[True, True, True, False]).reset_index(drop=True)
    
    print(f"Transformed dataset: {len(df_chart)} rows (one per occupation)")
    print(f"Sample transformed data:")
    print(df_chart.head(10))
    
    # Use the transformed data for saving
    df_merged = df_chart
    
else:
    print("No data to transform - df_merged is empty")

# Sort the final dataset for consistent ordering if we have data
if not df_merged.empty:
    df_merged = df_merged.sort_values(
        by=['Dato', 'Nivå', 'Geografisk enhet', 'Yrke'],
        ascending=[True, True, True, True]
    ).reset_index(drop=True)

################# Summary #################

print(f"\n=== SUMMARY ===")
print(f"Files processed: {len(found_files)}")
print(f"Job seeker data files found: {len(all_job_seeker_dataframes)}")
print(f"Job vacancy data files found: {len(all_job_vacancy_dataframes)}")

if error_messages:
    print(f"\nErrors encountered: {len(error_messages)}")
    for error in error_messages:
        print(f"  - {error}")
    notify_errors(error_messages, script_name=script_name)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #################

file_name = "soekere_stillinger_yrkespraksis.csv"
task_name = "Opplaering og kompetanse - Sokere stillinger yrkespraksis"
github_folder = "Data/02_Opplæring og kompetanse/Kompetansebehov"
temp_folder = os.environ.get("TEMP_FOLDER")

# Create a copy for comparison and convert Int64 to regular int to avoid type conflicts
df_compare = df_merged.copy()
for col in df_compare.select_dtypes(include=['Int64']).columns:
    # Convert Int64 to regular int, filling NaN with 0
    df_compare[col] = df_compare[col].fillna(0).astype('int64')

# Specify which columns contain the actual values we want to compare
value_columns = ['Antall søkere', 'Antall utlyste stillinger']

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

print("\nScript completed successfully!")
