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

# Initialize df variable
df = None

## Finner lenke til dataene under "Om denne visualiseringen --> Tallgrunnlag" her: https://www.ssb.no/natur-og-miljo/areal/arealprofiler

url = "https://www.ssb.no/natur-og-miljo/areal/arealprofiler/_/attachment/download/e4df8809-39df-4c28-94c2-997fdbd2a6db:00ad9738b90efb99f39fb0247125832e538d3e71/arealprofiler2020_ettark.xlsx"

try:
    # Make a GET request to the URL to download the Excel file
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        excel_file = BytesIO(response.content)

        # Read the Excel file (single sheet with 6 columns)
        df = pd.read_excel(excel_file)
        
        # Ensure numeric columns are float64 for compatibility with github_functions.py
        numeric_columns = df.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            df[col] = df[col].astype('float64')
        
        print(f"Data successfully loaded. Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
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

##################### Databehandling #####################

#df.info()

## Drop column "kommunenr"
df = df.drop(columns=["kommunenr"])

## Convert "år" to integer BEFORE renaming to prevent float/int comparison issues
df["år"] = df["år"].astype('int64')

## Rename columns using explicit mapping to avoid position errors
## Original columns after drop: ['statistikkvariabel', 'kommune', 'år', 'verdi', 'klasse']
df = df.rename(columns={
    'statistikkvariabel': 'Kategori',
    'kommune': 'Kommune', 
    'år': 'År',
    'verdi': 'Verdi',
    'klasse': 'Type'
})

## Ensure "Verdi" is consistent with GitHub storage format (string)
## Round to prevent floating-point precision issues, then convert to string to match GitHub format
df["Verdi"] = df["Verdi"].astype('float64').round(10).astype(str)

## Handle NaN values in "Type" column to prevent comparison issues  
## The GitHub data stores NaN as string 'nan', so we need to match that format
df["Type"] = df["Type"].fillna('nan').astype(str)

## Filter the kommuner "Skien", "Porsgrunn" and "Siljan"
df = df[df["Kommune"].isin(["Skien", "Porsgrunn", "Siljan"])]

## Sort data for consistent ordering and reset index to prevent comparison issues
df = df.sort_values(['Kategori', 'Kommune', 'År', 'Type']).reset_index(drop=True)


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

if df is not None:
    file_name = "arealprofiler.csv"
    task_name = "Bystrategi Grenland - Arealprofiler"
    github_folder = "Data/Bystrategi_Grenland/Areal_og_byutvikling"
    temp_folder = os.environ.get("TEMP_FOLDER")

    # Process all files and track their status
    is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

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
else:
    print("No data to process due to errors during download.")

# Notify yourself of errors, if any
if error_messages:
    notify_errors(error_messages, script_name="Extract_Miljodirektoratet_Data")
else:
    print("All tasks completed successfully.")