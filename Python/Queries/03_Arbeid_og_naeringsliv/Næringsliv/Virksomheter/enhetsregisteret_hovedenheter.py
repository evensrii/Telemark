import requests
import sys
import os
import glob
import io
import pandas as pd
from pyjstat import pyjstat
import json
import gzip
import shutil

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect error messages
error_messages = []

# Initialize df_telemark as None
df_telemark = None

try:
    # Define paths
    temp_folder = os.environ.get("TEMP_FOLDER", "temp")
    os.makedirs(temp_folder, exist_ok=True)
    
    gz_file_path = os.path.join(temp_folder, "enheter.json.gz")
    json_file_path = os.path.join(temp_folder, "enheter.json")

    # Download and extract data
    print("Downloading file from Enhetsregisteret...")
    url = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"
    try:
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        print(f"Total file size: {total_size / (1024*1024):.1f} MB")

        if response.status_code != 200:
            raise RuntimeError(f"Failed to download file. HTTP Status: {response.status_code}")

        print("Writing downloaded content to file...")
        with open(gz_file_path, "wb") as f:
            if total_size == 0:
                f.write(response.content)
            else:
                downloaded = 0
                for data in response.iter_content(chunk_size=8192):
                    downloaded += len(data)
                    f.write(data)
                    done = int(50 * downloaded / total_size)
                    print(f"\rDownload progress: [{'=' * done}{' ' * (50-done)}] {downloaded}/{total_size} bytes", end='', flush=True)
        print("\nDownload complete!")

        print("\nExtracting Gzip file...")
        try:
            with gzip.open(gz_file_path, "rb") as f_in:
                with open(json_file_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print("Extraction complete!")
            
            # Verify the extracted file exists and has content
            if not os.path.exists(json_file_path):
                raise RuntimeError("Extracted JSON file not found")
            
            file_size = os.path.getsize(json_file_path)
            if file_size == 0:
                raise RuntimeError("Extracted JSON file is empty")
            
            print(f"Extracted file size: {file_size / (1024*1024):.1f} MB")

        except Exception as gz_error:
            raise RuntimeError(f"Failed to extract gzip file: {str(gz_error)}")

        # Clean up gz file
        os.remove(gz_file_path)
        print("Cleaned up gzip file")

    except Exception as dl_error:
        if os.path.exists(gz_file_path):
            os.remove(gz_file_path)
        if os.path.exists(json_file_path):
            os.remove(json_file_path)
        raise RuntimeError(f"Download/extraction failed: {str(dl_error)}")

    # Process JSON data
    print("Processing JSON file...")
    telemark_kommunenummer = [
        "4001", "4003", "4005", "4010", "4012", "4014", "4016", "4018", "4020", 
        "4022", "4024", "4026", "4028", "4030", "4032", "4034", "4036"
    ]

    # Read and filter JSON data
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        filtered_data = [
            enhet for enhet in data 
            if enhet.get('forretningsadresse', {}).get('kommunenummer') in telemark_kommunenummer
        ]

    print(f"Found {len(filtered_data)} companies in Telemark")

    if not filtered_data:
        raise RuntimeError("No companies found matching the filter criteria")

    # Convert to DataFrame and process nested columns
    full_df = pd.DataFrame(filtered_data)
    full_df.set_index('organisasjonsnummer', inplace=True)

    # Process nested columns
    nested_columns = [
        'forretningsadresse', 'postadresse', 'organisasjonsform',
        'institusjonellSektorkode', 'naeringskode1', 'naeringskode2', 'naeringskode3'
    ]

    for column in nested_columns:
        if column in full_df.columns:
            try:
                valid_rows = full_df[full_df[column].notna()]
                if not valid_rows.empty:
                    df_nested = pd.json_normalize(valid_rows[column])
                    df_nested.index = valid_rows.index
                    df_nested.columns = [f'{column}.{col}' for col in df_nested.columns]
                    
                    full_df = full_df.drop(columns=[column])
                    for col in df_nested.columns:
                        full_df[col] = df_nested[col]
            except Exception as e:
                print(f"Warning: Could not process column {column}: {str(e)}")

    # Reset index to get organisasjonsnummer back as a column
    full_df.reset_index(inplace=True)

    # Extract specified columns
    columns_to_extract = [
        'navn', 'organisasjonsnummer', 'overordnetEnhet', 'antallAnsatte',
        'institusjonellSektorkode.kode', 'institusjonellSektorkode.beskrivelse',
        'aktivitet', 'forretningsadresse.kommune', 'postadresse.kommune',
        'organisasjonsform.beskrivelse', 'naeringskode1.kode',
        'naeringskode1.beskrivelse', 'naeringskode2.kode',
        'naeringskode2.beskrivelse', 'naeringskode3.kode',
        'naeringskode3.beskrivelse'
    ]

    existing_columns = [col for col in columns_to_extract if col in full_df.columns]
    df_edit = full_df[existing_columns].copy()

    # Format and clean data
    df_telemark = df_edit.copy()

    # Rename columns
    rename_map = {
        'organisasjonsnummer': 'Org. nr.',
        'navn': 'Navn',
        'overordnetEnhet': 'Overordnet enhet',
        'organisasjonsform.beskrivelse': 'Organisasjonsform',
        'institusjonellSektorkode.beskrivelse': 'Sektor',
        'institusjonellSektorkode.kode': 'Sektorkode',
        'aktivitet': 'Aktivitet',
        'forretningsadresse.kommune': 'Forretningsadresse - Kommune',
        'postadresse.kommune': 'Postadresse - Kommune',
        'naeringskode1.kode': 'NACE 1',
        'naeringskode1.beskrivelse': 'NACE 1 - Bransje',
        'naeringskode2.kode': 'NACE 2',
        'naeringskode2.beskrivelse': 'NACE 2 - Bransje',
        'naeringskode3.kode': 'NACE 3',
        'naeringskode3.beskrivelse': 'NACE 3 - Bransje',
        'antallAnsatte': 'Antall ansatte'
    }

    df_telemark = df_telemark.rename(columns=rename_map)

    # Format text columns
    def format_company_name(name):
        # Handle NaN values
        if pd.isna(name):
            return name
        
        # List of words that should always be capitalized
        special_words = ['Vestfold', 'Telemark', 'Norge', 'Norway', 'Skien', 'Porsgrunn', 'Bamble', 
                        'Kragerø', 'Drangedal', 'Nome', 'Midt-Telemark', 'Tinn', 
                        'Hjartdal', 'Seljord', 'Kviteseid', 'Nissedal', 'Fyresdal', 
                        'Tokke', 'Vinje', 'Notodden', 'Siljan']
        
        # Split the name into words
        words = str(name).lower().split()
        
        # Capitalize the first word
        if words:
            words[0] = words[0].capitalize()
        
        # Handle special cases
        for i, word in enumerate(words):
            # Check if the word should always be capitalized
            for special_word in special_words:
                if word.lower() == special_word.lower():
                    words[i] = special_word
                    break
        
            # Handle "AS" at the end
            if i == len(words) - 1 and word.lower() == "as":
                words[i] = "AS"

            # Handle "HF" at the end
            if i == len(words) - 1 and word.lower() == "hf":
                words[i] = "HF"

            # Handle "AS" at the end
            if i == len(words) - 1 and word.lower() == "nav":
                words[i] = "NAV"
        
        return " ".join(words)

    def format_municipality_name(name):
        # Handle NaN values
        if pd.isna(name):
            return name
        
        name = str(name)
        if name.upper() == "MIDT-TELEMARK":
            return "Midt-Telemark"
        return name.lower().capitalize()

    df_telemark['Navn'] = df_telemark['Navn'].apply(format_company_name)
    df_telemark['Forretningsadresse - Kommune'] = df_telemark['Forretningsadresse - Kommune'].apply(format_municipality_name)
    df_telemark['Postadresse - Kommune'] = df_telemark['Postadresse - Kommune'].apply(format_municipality_name)

    # Clean Aktivitet column
    df_telemark['Aktivitet'] = df_telemark['Aktivitet'].str.replace('[\[\]]', '', regex=True)

    # Sort the dataframe
    df_telemark = df_telemark.sort_values(by=['Antall ansatte', 'NACE 1'], ascending=[False, True])

    # Clean up the temporary JSON file
    if os.path.exists(json_file_path):
        os.remove(json_file_path)
        print("Cleaned up temporary files")

except Exception as e:
    error_messages.append(f"Error in script execution: {str(e)}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("A critical error occurred during script execution.")

# Output handling
if df_telemark is not None:
    file_name = "enhetsregisteret_hovedenheter.csv"
    task_name = "Arbeid og naeringsliv - Enhetsregisteret - Hovedenheter"
    github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter"
    temp_folder = os.environ.get("TEMP_FOLDER")

    # Only specify the one column we know is truly numeric
    value_columns = ['Antall ansatte']

    # Ignore all columns that might contain codes or text that look like numbers
    ignore_patterns = [
        'Org. nr.',
        'NACE 1',
        'NACE 2',
        'NACE 3',
        'Sektorkode',
        'Overordnet enhet',
        'NACE 1 - Bransje',
        'NACE 2 - Bransje',
        'NACE 3 - Bransje'
    ]

    # Call the function and get the "New Data" status
    is_new_data = handle_output_data(
        df_telemark, 
        file_name, 
        github_folder, 
        temp_folder, 
        keepcsv=True,
        value_columns=value_columns,
        ignore_patterns=ignore_patterns
    )

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