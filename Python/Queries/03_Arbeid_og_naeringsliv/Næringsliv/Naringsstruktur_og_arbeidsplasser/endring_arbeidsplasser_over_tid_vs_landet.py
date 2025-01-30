import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# Endepunkt for SSB API
POST_URL_sysselsatte = "https://data.ssb.no/api/v0/no/table/13472/"

################## ARBEIDSPLASSER I KOMMUNER (15-74 ÅR) SISTE ÅR!! (Dvs. sysselsatte etter arbeidssted)

# Spørring for å hente ut data fra SSB
payload_sysselsatte = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommSummer",
        "values": [
          "K-4001",
          "K-4003",
          "K-4005",
          "K-4010",
          "K-4012",
          "K-4014",
          "K-4016",
          "K-4018",
          "K-4020",
          "K-4022",
          "K-4024",
          "K-4026",
          "K-4028",
          "K-4030",
          "K-4032",
          "K-4034",
          "K-4036"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "6100",
          "6500.6",
          "6500.7",
          "A+B+D+E.5-7",
          "A+B+D+E.0-1+9"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselEtterArbste"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "8"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_sysselsatte = fetch_data(
        url=POST_URL_sysselsatte,
        payload=payload_sysselsatte,
        error_messages=error_messages,
        query_name="Sysselsatte, kommuner",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Create a mask for private sector
private_sector_mask = df_sysselsatte['sektor'] == 'Privat sektor'

# Split the data into private and public sectors
private_sector = df_sysselsatte[private_sector_mask].copy()
public_sector = df_sysselsatte[~private_sector_mask].copy()

# Sum up all public sector values for each region
public_sector_sum = public_sector.groupby(['region', 'statistikkvariabel', 'år'])['value'].sum().reset_index()
public_sector_sum['sektor'] = 'Offentlig sektor'

# Combine private and public sector data
df_processed = pd.concat([public_sector_sum, private_sector], ignore_index=True)

# Clean up the region names and add Label column
df_processed["region"] = df_processed["region"].str.replace(r'\s*\(\d{4}-\d{4}\)', '', regex=True)
df_processed = df_processed.rename(columns={
    "region": "Kommune", 
    "value": "Antall sysselsatte"
})
df_processed["Label"] = df_processed["Kommune"]

# Sort by Kommune and sektor
df_processed = df_processed.sort_values(['Kommune', 'sektor'])

# Calculate total employed in Telemark for the latest year
latest_year = df_processed['år'].max()
total_employed = df_processed[df_processed['år'] == latest_year]['Antall sysselsatte'].sum()
print(f"\nAntall sysselsatte i {latest_year}: {total_employed}")

# Calculate percentage distribution between sectors for latest year
sector_distribution = df_processed[df_processed['år'] == latest_year].groupby('sektor')['Antall sysselsatte'].sum()
sector_percentages = (sector_distribution / sector_distribution.sum() * 100).round(1)
sector_df = pd.DataFrame({
    'Sektor': sector_percentages.index,
    'Prosent sysselsatte': sector_percentages.values
})
print("\nFordeling mellom sektorer:")
print(sector_df)

# Create dataframe with total employment by kommune
kommune_total = df_processed[df_processed['år'] == latest_year].groupby('Kommune')['Antall sysselsatte'].sum().reset_index()
kommune_total = kommune_total.sort_values('Antall sysselsatte', ascending=False)

# Add the latest year in parentheses after the "Antall sysselsatte" title
kommune_total.rename(columns={'Antall sysselsatte': f'Antall sysselsatte ({latest_year})'}, inplace=True)

print("\nAntall sysselsatte per kommune:")
print(kommune_total)

################## ARBEIDSPLASSER (SYSSELSATTE PERSONER ETTER ARBEIDSSTED) I TELEMARK

years_str = [str(year) for year in range(2015, int(latest_year) + 1)]

# Spørring for å hente ut data fra SSB
payload_sysselsatte_over_tid = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommFylker",
        "values": [
          "F-40"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselEtterArbste"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": years_str
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_sysselsatte_over_tid = fetch_data(
        url=POST_URL_sysselsatte,
        payload=payload_sysselsatte_over_tid,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Sysselsatte over tid, Telemark",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Drop columns "region", "sektor", "statistikkvariabel"
df_sysselsatte_over_tid = df_sysselsatte_over_tid.drop(columns=["region", "sektor", "statistikkvariabel"])

# Rename to "År" and "Antall sysselsatte"
df_sysselsatte_over_tid = df_sysselsatte_over_tid.rename(columns={"år": "År", "value": "Antall sysselsatte"})

# Calculate year-over-year percentage change
df_sysselsatte_over_tid['Endring fra året før (%)'] = (df_sysselsatte_over_tid['Antall sysselsatte'].pct_change() * 100).round(1)

# Create period column (e.g., "2015-2016")
df_sysselsatte_over_tid['Periode'] = df_sysselsatte_over_tid['År'].astype(str).shift(1) + '-' + df_sysselsatte_over_tid['År'].astype(str)

# Drop the first row since it won't have a previous year for comparison
df_sysselsatte_over_tid = df_sysselsatte_over_tid.iloc[1:].copy()

# Drop rows "År", "Antall sysselsatte"
df_sysselsatte_over_tid = df_sysselsatte_over_tid.drop(columns=["År", "Antall sysselsatte"])

# Move "Periode" column to the front
df_sysselsatte_over_tid = df_sysselsatte_over_tid[['Periode', 'Endring fra året før (%)']]

# Set column names to "Periode" and "Telemark"
df_sysselsatte_over_tid.columns = ['Periode', 'Telemark']

print("\nEndring i antall sysselsatte fra år til år:")
print(df_sysselsatte_over_tid[['Periode', 'Telemark']])


################## ARBEIDSPLASSER (SYSSELSATTE PERSONER ETTER ARBEIDSSTED) I LANDET

# Spørring for å hente ut data fra SSB
payload_sysselsatte_over_tid_landet = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselEtterArbste"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": years_str
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_sysselsatte_over_tid_landet = fetch_data(
        url=POST_URL_sysselsatte,
        payload=payload_sysselsatte_over_tid_landet,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Sysselsatte over tid, landet",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Process national data the same way as Telemark data
# Drop columns "region", "sektor", "statistikkvariabel"
df_sysselsatte_over_tid_landet = df_sysselsatte_over_tid_landet.drop(columns=["region", "sektor", "statistikkvariabel"])

# Rename to "År" and "Antall sysselsatte"
df_sysselsatte_over_tid_landet = df_sysselsatte_over_tid_landet.rename(columns={"år": "År", "value": "Antall sysselsatte"})

# Calculate year-over-year percentage change
df_sysselsatte_over_tid_landet['Endring fra året før (%)'] = (df_sysselsatte_over_tid_landet['Antall sysselsatte'].pct_change() * 100).round(1)

# Create period column (e.g., "2015-2016")
df_sysselsatte_over_tid_landet['Periode'] = df_sysselsatte_over_tid_landet['År'].astype(str).shift(1) + '-' + df_sysselsatte_over_tid_landet['År'].astype(str)

# Drop the first row since it won't have a previous year for comparison
df_sysselsatte_over_tid_landet = df_sysselsatte_over_tid_landet.iloc[1:].copy()

# Drop rows "År", "Antall sysselsatte"
df_sysselsatte_over_tid_landet = df_sysselsatte_over_tid_landet.drop(columns=["År", "Antall sysselsatte"])

# Move "Periode" column to the front
df_sysselsatte_over_tid_landet = df_sysselsatte_over_tid_landet[['Periode', 'Endring fra året før (%)']]

# Set column names to "Periode" and "Landet"
df_sysselsatte_over_tid_landet.columns = ['Periode', 'Landet']



# Merge Telemark and national data
df_combined = df_sysselsatte_over_tid.merge(df_sysselsatte_over_tid_landet[['Periode', 'Landet']], on='Periode')

print("\nEndring i antall sysselsatte (%) fra år til år (Telemark og landet):")
print(df_combined)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "endring_i_arbeidsplasser_over_tid_vs_landet.csv"
task_name = "Arbeid og naeringsliv - Offentlig vs. privat næringsliv"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Næringsstruktur og arbeidsplasser"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_combined, file_name, github_folder, temp_folder, keepcsv=True)

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
