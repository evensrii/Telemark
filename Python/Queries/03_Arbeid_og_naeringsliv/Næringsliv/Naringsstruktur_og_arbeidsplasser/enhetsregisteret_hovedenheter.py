import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
import json

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

try:
    # Base URL for the API
    url = "https://data.brreg.no/enhetsregisteret/api/enheter"

    # List of municipality numbers for Telemark
    telemark_kommunenummer = [
        "4001", "4003", "4005", "4010", "4012", "4014", "4016", "4018", "4020", 
        "4022", "4024", "4026", "4028", "4030", "4032", "4034", "4036"
    ]

    # Query parameters
    params = {
        "forretningsadresse.kommunenummer": telemark_kommunenummer,
        "fraAntallAnsatte": 10,
        "size": 100,  # Adjust the size as needed
        "konkurs": False
    }

    all_enheter = []

    # Loop through pages until no more data is found
    try:
        for page in range(1, 100):
            params['page'] = page
            response = requests.get(url, params=params)
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Write the JSON response to a file for inspection
                    #with open(f'response_page_{page}.json', 'w', encoding='utf-8') as f:
                    #    json.dump(data, f, ensure_ascii=False, indent=2)
                    
                    # Check if '_embedded' key is present
                    if '_embedded' in data:
                        enheter_list = data['_embedded']['enheter']
                        all_enheter.extend(enheter_list)
                    else:
                        print(f"Page {page} does not contain '_embedded': {data}")
                        print(f"Stopping query as no more data is available.")
                        break  # Exit the loop as we've reached the end of data
                except ValueError as e:
                    error_messages.append(f"Error decoding JSON on page {page}: {str(e)}")
                    break  # Exit on JSON decode error
            else:
                error_messages.append(f"Error fetching data from page {page}: {response.status_code}")
                break  # Exit on HTTP error
    except Exception as e:
        error_messages.append(f"Error during API pagination: {str(e)}")

    # Convert the collected data to a pandas DataFrame
    try:
        full_df = pd.DataFrame(all_enheter)
        
        # Un-nest nested columns
        nested_columns = ['forretningsadresse', 'postadresse', 'organisasjonsform', 
                         'institusjonellSektorkode', 'naeringskode1', 'naeringskode2', 'naeringskode3']
        for column in nested_columns:
            if column in full_df.columns:
                df = pd.json_normalize(full_df[column])
                df.columns = [f'{column}.{col}' for col in df.columns]
                full_df = full_df.drop(columns=[column]).merge(df, left_index=True, right_index=True)
    except Exception as e:
        error_messages.append(f"Error processing DataFrame: {str(e)}")

    try:
        # Extract and format specified columns
        columns_to_extract = [
            'navn',
            'organisasjonsnummer',
            'overordnetEnhet',
            'antallAnsatte',
            'institusjonellSektorkode.kode',
            'institusjonellSektorkode.beskrivelse',
            'aktivitet',
            'forretningsadresse.kommune',
            'postadresse.kommune',
            'organisasjonsform.beskrivelse',
            'naeringskode1.kode',  
            'naeringskode1.beskrivelse',
            'naeringskode2.kode',
            'naeringskode2.beskrivelse',
            'naeringskode3.kode',
            'naeringskode3.beskrivelse'
        ]

        df_edit = full_df[columns_to_extract]

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

        df_edit = df_edit.rename(columns=rename_map)

        # Format text columns
        def format_company_name(name):
            # Handle NaN values
            if pd.isna(name):
                return name
            
            # List of words that should always be capitalized
            special_words = ['Vestfold', 'Telemark', 'Skien', 'Porsgrunn', 'Bamble', 
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
            
            return " ".join(words)

        def format_municipality_name(name):
            # Handle NaN values
            if pd.isna(name):
                return name
            
            name = str(name)
            if name.upper() == "MIDT-TELEMARK":
                return "Midt-Telemark"
            return name.lower().capitalize()

        df_edit['Navn'] = df_edit['Navn'].apply(format_company_name)
        df_edit['Forretningsadresse - Kommune'] = df_edit['Forretningsadresse - Kommune'].apply(format_municipality_name)
        df_edit['Postadresse - Kommune'] = df_edit['Postadresse - Kommune'].apply(format_municipality_name)

        # Remove all "[" and "]" from the column "Aktivitet"
        df_edit['Aktivitet'] = df_edit['Aktivitet'].str.replace('[\[\]]', '', regex=True)

        # Sort the dataframe
        df_edit = df_edit.sort_values(by=['Antall ansatte', 'NACE 1'], ascending=[False, True])

    except Exception as e:
        error_messages.append(f"Error in data formatting: {str(e)}")

except Exception as e:
    error_messages.append(f"Unexpected error in script execution: {str(e)}")


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "enhetsregisteret_hovedenheter.csv"
task_name = "Arbeid og naeringsliv - Enhetsregisteret - Hovedenheter"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter"
temp_folder = os.environ.get("TEMP_FOLDER")

# Ensure temp folder exists
if not os.path.exists(temp_folder):
    os.makedirs(temp_folder)

# Save DataFrame to CSV in temp folder
temp_file_path = os.path.join(temp_folder, file_name)
df_edit.to_csv(temp_file_path, index=False)
print(f"Saved file to {temp_file_path}")

try:
    # Download existing file from GitHub for comparison
    github_file_path = f"{github_folder}/{file_name}"
    existing_df = download_github_file(github_file_path)
    
    if existing_df is None:
        print(f"No existing file found on GitHub. Will upload new file: {file_name}")
        upload_github_file(temp_file_path, github_file_path)
        is_new_data = True
    else:
        # Compare only the 'Antall ansatte' column for numeric differences
        existing_df['Antall ansatte'] = pd.to_numeric(existing_df['Antall ansatte'], errors='coerce')
        df_edit['Antall ansatte'] = pd.to_numeric(df_edit['Antall ansatte'], errors='coerce')
        
        # Check if there are any differences
        if not df_edit.equals(existing_df):
            print(f"Changes detected in the data. Uploading new version to GitHub: {file_name}")
            upload_github_file(temp_file_path, github_file_path)
            is_new_data = True
        else:
            print("No changes detected in the data.")
            is_new_data = False

except Exception as e:
    error_messages.append(f"Error in GitHub comparison: {str(e)}")
    is_new_data = False

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