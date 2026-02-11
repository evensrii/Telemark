import os
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

#### Scriptet finner, for siste år, andel med ulike utdanningsnivåer i:

# 1) Telemark
# 2) Norge
# 3) Norge (utenom Oslo) <-- Må se på fordelingen av absolutte tall.

################# Spørring - Telemark #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:FylkerGjeldende",
        "values": [
          "40"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "PersonerProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_telemark = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Telemark",
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

print(df_telemark)

################# Spørring - Hele landet #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
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
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "PersonerProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_landet = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Hele landet",
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

print(df_landet)

################# Spørring - Hele landet (utenom Oslo) ################# <---- Absolutte tall

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:FylkerGjeldende",
        "values": [
          "31",
          "32",
          "33",
          "34",
          "39",
          "40",
          "42",
          "11",
          "46",
          "15",
          "50",
          "18",
          "55",
          "56"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Personer"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_landet_uten_oslo = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Hele landet uten Oslo",
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

print(df_landet_uten_oslo)

### DATA CLEANING

# Process df_landet_uten_oslo to match structure of other dataframes
# Group by 'nivå' and sum all values to get totals for each education level
df_landet_uten_oslo_grouped = df_landet_uten_oslo.groupby('nivå')['value'].sum().reset_index()

# Calculate total across all education levels
total_personer_uten_oslo = df_landet_uten_oslo_grouped['value'].sum()

# Calculate percentage for each education level
df_landet_uten_oslo_grouped['value'] = (df_landet_uten_oslo_grouped['value'] / total_personer_uten_oslo * 100).round(1)

# Add the other columns to match structure of df_telemark and df_landet
df_landet_uten_oslo_grouped['region'] = 'Hele landet utenom Oslo'
df_landet_uten_oslo_grouped['kjønn'] = 'Begge kjønn'
df_landet_uten_oslo_grouped['statistikkvariabel'] = 'Personer 16 år og over (prosent)'
df_landet_uten_oslo_grouped['år'] = df_landet_uten_oslo['år'].iloc[0]  # Get year from original data

# Reorder columns to match other dataframes
df_landet_uten_oslo = df_landet_uten_oslo_grouped[['region', 'nivå', 'kjønn', 'statistikkvariabel', 'år', 'value']].copy()

print("\nProcessed df_landet_uten_oslo:")
print(df_landet_uten_oslo)

# Process all three dataframes: move year to column header and remove unnecessary columns
# Get the year value from each dataframe
year_telemark = df_telemark['år'].iloc[0]
year_landet = df_landet['år'].iloc[0]
year_landet_uten_oslo = df_landet_uten_oslo['år'].iloc[0]

# Process df_telemark
df_telemark_final = df_telemark[['region', 'nivå', 'value']].copy()
df_telemark_final.columns = ['region', 'nivå', f'Andel ({year_telemark})']

# Process df_landet
df_landet_final = df_landet[['region', 'nivå', 'value']].copy()
df_landet_final.columns = ['region', 'nivå', f'Andel ({year_landet})']

# Process df_landet_uten_oslo
df_landet_uten_oslo_final = df_landet_uten_oslo[['region', 'nivå', 'value']].copy()
df_landet_uten_oslo_final.columns = ['region', 'nivå', f'Andel ({year_landet_uten_oslo})']

print("\nFinal processed dataframes:")
print("\ndf_telemark_final:")
print(df_telemark_final)
print("\ndf_landet_final:")
print(df_landet_final)
print("\ndf_landet_uten_oslo_final:")
print(df_landet_uten_oslo_final)

# Concatenate the three datasets
df_combined = pd.concat([df_telemark_final, df_landet_final, df_landet_uten_oslo_final], ignore_index=True)

# Combine the two university education categories into "Høyere utdanning"
# First, identify rows with university education
university_mask = df_combined['nivå'].isin(['Universitets- og høgskolenivå, kort', 'Universitets- og høgskolenivå, lang'])

# Group by region and sum the university education values
df_hoyere_utdanning = df_combined[university_mask].groupby('region')[f'Andel ({year_telemark})'].sum().reset_index()
df_hoyere_utdanning['nivå'] = 'Høyere utdanning'

# Remove the original university categories from the combined dataframe
df_combined_clean = df_combined[~university_mask].copy()

# Add the new "Høyere utdanning" category
df_combined_clean = pd.concat([df_combined_clean, df_hoyere_utdanning], ignore_index=True)

# Transform the dataframe to have regions as columns
df_final = df_combined_clean.pivot(index='nivå', columns='region', values=f'Andel ({year_telemark})').reset_index()

# Rename the index column and reorder columns
df_final.columns.name = None  # Remove the column name from pivot
df_final = df_final.rename(columns={'nivå': 'Utdanningsnivå'})

# Reorder columns to match requested order
column_order = ['Utdanningsnivå', 'Telemark', 'Hele landet', 'Hele landet utenom Oslo']
df_final = df_final[column_order]

# Round values to 1 decimal place
for col in ['Telemark', 'Hele landet', 'Hele landet utenom Oslo']:
    df_final[col] = df_final[col].round(1)

# Rename education levels to shorter names
education_mapping = {
    'Grunnskolenivå': 'Grunnskole',
    'Videregående skolenivå': 'Videregående skole',
    'Fagskolenivå': 'Fagskole',
    'Høyere utdanning': 'Høyere utdanning'
}

df_final['Utdanningsnivå'] = df_final['Utdanningsnivå'].map(education_mapping)

# Define the desired order and sort accordingly
desired_order = ['Grunnskole', 'Videregående skole', 'Fagskole', 'Høyere utdanning']
df_final['sort_order'] = df_final['Utdanningsnivå'].map({level: i for i, level in enumerate(desired_order)})
df_final = df_final.sort_values('sort_order').drop('sort_order', axis=1).reset_index(drop=True)

print("\nFinal transformed dataframe:")
print(df_final)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "utdanningsniva_telemark_og_landet.csv"
task_name = "Opplaering og kompetanse - Utdanningsnivaa Telemark og landet"
github_folder = "Data/02_Opplæring og kompetanse/Utdanningsnivå"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_final, file_name, github_folder, temp_folder, keepcsv=True)

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