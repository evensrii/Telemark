import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

##### Trikset var her at jeg måtte starte en session for å simulere at jeg besøkte nettsiden før jeg lasted ned!

##### Lagrer fullt datasett fra Telemark (med info om hvert anlegg fra hvert år) + datasett for Everviz-figur

# Example list of error messages to collect errors during execution
error_messages = []

try:
    ### Kjøre spørring (Excel --> Pandas DataFrame)

    # URL for the main page or the page that needs to be visited first
    initial_url = "https://www.norskeutslipp.no/no/Komponenter/Utslipp/Klimagasser-CO2-ekvivalenter/?ComponentType=utslipp&ComponentPageID=1166&SectorID=600"

    # URL for the Excel file
    download_url = "https://www.norskeutslipp.no/Templates/NorskeUtslipp/Pages/exportTableData.aspx?PageID=1166&ComponentType=utslipp&exportType=temaexcel&epslanguage=no"

    # Create a session object to persist cookies (her samles cookies og brukes automatisk videre)
    session = requests.Session()

    # Step 1: Send an initial request to the main page to establish the session
    initial_response = session.get(initial_url)
    if initial_response.status_code != 200:
        raise ValueError(
            f"Failed to access the initial page. Status code: {initial_response.status_code}"
        )

    # Step 2: Attempt to download the Excel file using the same session
    response = session.get(download_url)

    # Step 3: Check if the request was successful
    if response.status_code == 200:
        # Load the Excel file into pandas using the 'openpyxl' engine
        excel_data = BytesIO(response.content)
        try:
            df_downloaded = pd.read_excel(excel_data, engine="openpyxl")
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {e}")

        # Display the DataFrame
        # print(df_downloaded)
    else:
        raise ValueError(
            f"Failed to download the file. Status code: {response.status_code}"
        )

except Exception as e:
    error_message = f"An error occurred: {str(e)}"
    print(error_message)
    error_messages.append(error_message)

# Notify yourself of errors, if any
if error_messages:
    notify_errors(error_messages, script_name="Extract_NorskeUtslipp_Data")
else:
    print("All tasks completed successfully.")


### Data cleaning

df = df_downloaded.copy()

#df.info()

# Drop rows with all na values
df = df.dropna(how="all")

# Make the first row the header and remove that row
df.columns = df.iloc[0]  # Set the first row as the header
df = df[1:].reset_index(drop=True)  # Drop the first row and reset index in one step

# Filter Fylke == "Telemark"
df = df[df["Fylke"] == "Telemark"]

# Ensure all columns containing "NACE" (case insensitive) stay as strings with 3 decimal places
nace_columns = [col for col in df.columns if 'nace' in col.lower()]
for col in nace_columns:
    df[col] = df[col].astype(str).str.strip()
    # Ensure 3 decimal places for NACE codes
    df[col] = df[col].apply(lambda x: f"{float(x):.3f}" if '.' in x else x)

# Debug print for NACE codes
print("\nUnique NACE codes in the dataset:")
for col in nace_columns:
    print(f"\n{col}:")
    print(df[col].unique())

# Convert "År" to datetime
df["År"] = pd.to_datetime(df["År"], format="%Y")
df = df[df["År"] > "2000"]

# Convert "Årlig utslipp til luft" to numeric
df["Årlig utslipp til luft"] = df["Årlig utslipp til luft"].astype(str)
df["Årlig utslipp til luft"] = (
    df["Årlig utslipp til luft"].str.replace(",", ".", regex=False).str.strip()
)
df["Årlig utslipp til luft"] = pd.to_numeric(
    df["Årlig utslipp til luft"], errors="coerce"
)

df = df.round(2)

### "Fullt datasett" for Telemark (kjekt å ha)

df_norske_detaljert = df.copy()


#### Everviz-figur

# Kolonne 1 (Norskeutslipp.no)
df = df.groupby("År")["Årlig utslipp til luft"].sum().reset_index()

# Format "År" as year only
df["År"] = df["År"].dt.year


# Kolonne 2 (Mdir - industriutslipp) <--- Denne genereres fra det andre scriptet, "klimagassutslipp.py"
url = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/04_Klima%20og%20ressursforvaltning/Klimagassutslipp/klimagassutslipp_telemark.csv"
df_industri = pd.read_csv(url)
df_industri = df_industri[df_industri["Sektor"] == "Industri, olje og gass"]
df_industri = df_industri.groupby("År")["Utslipp"].sum().reset_index()
# convert "År" to datetime
# df_industri["År"] = pd.to_datetime(df_industri["År"], format="%Y-%m-%d").dt.year

# Merge df and df_industri by "År"
df = pd.merge(df, df_industri, on="År", how="outer")

df.columns = [
    "År",
    "Utslipp fra landbasert industri (norskeutslipp.no)",
    "Totale utslipp fra industrien (Mdir)",
]

# Create a new column 'Mdir enkeltår' and move the values for the specified years
df["Mdir enkeltår"] = df["Totale utslipp fra industrien (Mdir)"].where(
    df["År"].isin([2009, 2011, 2013])
)

# Set the values for those years in the original column to NaN (optional)
df.loc[df["År"].isin([2009, 2011, 2013]), "Totale utslipp fra industrien (Mdir)"] = pd.NA

# Print column names and sample data for debugging
print("\nColumn names in DataFrame:")
print(df.columns.tolist())
print("\nSample data:")
print(df.head())

# Fill NaN values with empty string for consistent comparison
df = df.fillna('')

# Round all float64 values to 2 decimals
df = df.round(2)

df_norskeutslipp_everviz = df.copy()


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name1 = "norskeutslipp_detaljert_telemark.csv"
file_name2 = "norskeutslipp_everviz.csv"
task_name = "Klima og energi - Utslipp fra landbasert industri"
github_folder = "Data/04_Klima og ressursforvaltning/Klimagassutslipp"
temp_folder = os.environ.get("TEMP_FOLDER")

# Process both files and track their status
# First file: Monitor specific columns and ignore NACE codes
is_new_data1 = handle_output_data(
    df_norske_detaljert,
    'norskeutslipp_detaljert_telemark.csv',
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=['Årlig utslipp til luft'],
    ignore_patterns=['nace']
)

# Second file: Monitor specific value columns
is_new_data2 = handle_output_data(
    df_norskeutslipp_everviz,
    file_name2,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=['Utslipp fra landbasert industri (norskeutslipp.no)', 'Totale utslipp fra industrien (Mdir)', 'Mdir enkeltår']
)

# Write a single status file that indicates if either file has new data
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format - set to "Yes" if either file has new data
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},multiple_files,{'Yes' if (is_new_data1 or is_new_data2) else 'No'}\n")

# Output results for debugging/testing
if is_new_data1:
    print(f"New data detected in {file_name1} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name1}.")

if is_new_data2:
    print(f"New data detected in {file_name2} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name2}.")

print(f"New data status log written to {new_data_status_file}")