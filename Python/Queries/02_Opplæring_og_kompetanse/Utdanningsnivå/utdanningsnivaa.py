import os
import pandas as pd
from datetime import datetime
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors

from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Alle fylker, siste år

# Antall 2024 - NÅ
# Antall 2020 - 2023
# Antall 2004 - 2019

# Andel 2024 - NÅ
# Andel 2020 - 2023
# Andel 2004 - 2019

################# Spørring #################

################################ Antall 2024 - NÅ

# Endepunkt for SSB API
# Generate a list of years from 2024 to the current year
current_year = datetime.now().year
years_to_query = [str(year) for year in range(2024, current_year + 1)]

# If the list is empty (i.e., current year is before 2024), default to 2024
if not years_to_query:
    years_to_query = ["2024"]

POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:KommGjeldende",
        "values": [
          "4001",
          "4003",
          "4005",
          "4010",
          "4012",
          "4014",
          "4016",
          "4018",
          "4020",
          "4022",
          "4024",
          "4026",
          "4028",
          "4030",
          "4032",
          "4034",
          "4036"
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
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",                              
          "2",
          "1"
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
        "filter": "item",
        "values": years_to_query
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

df_antall_2024_naa = None
while df_antall_2024_naa is None and years_to_query:
    try:
        # Update payload with the current list of years to try
        payload['query'][-1]['selection']['values'] = years_to_query

        df_antall_2024_naa = fetch_data(
            url=POST_URL,
            payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
            error_messages=error_messages,
            query_name="Antall 2024 - NÅ",
            response_type="json",  # The expected response type, either 'json' or 'csv'.
        )
        print(f"Successfully fetched data for years: {years_to_query}")

    except Exception as e:
        # Check if the error is a 400 Bad Request and there are years to remove
        if "400 Client Error" in str(e) and len(years_to_query) > 1:
            removed_year = years_to_query.pop()
            print(f"Query failed for years up to {removed_year}. Retrying with years up to {years_to_query[-1]}.")
            # If we successfully fall back from current year, explain this is normal
            if removed_year == str(current_year):
                print(f"NOTE: This error is perfectly normal - data for {removed_year} is not yet available in SSB. The script will continue with data up to {years_to_query[-1]}.")
        else:
            print(f"An unrecoverable error occurred: {e}")
            notify_errors(error_messages, script_name=script_name)
            raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

# If df is still None after the loop, it means all attempts failed
if df_antall_2024_naa is None:
    error_msg = "Could not fetch data from SSB after multiple attempts."
    print(error_msg)
    error_messages.append(error_msg)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(error_msg)



################################ Antall 2020 - 2023


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:Komm2020",
        "values": [
          "3806",
          "3807",
          "3808",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825"
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
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
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
        "filter": "item",
        "values": [
          "2020",
          "2021",
          "2022",
          "2023"
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
    df_antall_2020_2023 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Antall 2020 - 2023",
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


################################ Antall 2004 - 2019


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Kommune",
        "values": [
          "0805",
          "0806",
          "0807",
          "0811",
          "0814",
          "0815",
          "0817",
          "0819",
          "0821",
          "0822",
          "0826",
          "0827",
          "0828",
          "0829",
          "0830",
          "0831",
          "0833",
          "0834"
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
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
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
        "filter": "item",
        "values": [
          "2004",
          "2005",
          "2006",
          "2007",
          "2008",
          "2009",
          "2010",
          "2011",
          "2012",
          "2013",
          "2014",
          "2015",
          "2016",
          "2017",
          "2018",
          "2019"
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
    df_antall_2004_2019 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Antall 2004 - 2019",
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



################################ Andel 2024 - NÅ

# Endepunkt for SSB API
# Generate a list of years from 2024 to the current year
current_year = datetime.now().year
years_to_query = [str(year) for year in range(2024, current_year + 1)]

# If the list is empty (i.e., current year is before 2024), default to 2024
if not years_to_query:
    years_to_query = ["2024"]

POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:KommGjeldende",
        "values": [
          "4001",
          "4003",
          "4005",
          "4010",
          "4012",
          "4014",
          "4016",
          "4018",
          "4020",
          "4022",
          "4024",
          "4026",
          "4028",
          "4030",
          "4032",
          "4034",
          "4036"
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
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",                              
          "2",
          "1"
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
        "filter": "item",
        "values": years_to_query
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

df_andel_2024_naa = None
while df_andel_2024_naa is None and years_to_query:
    try:
        # Update payload with the current list of years to try
        payload['query'][-1]['selection']['values'] = years_to_query

        df_andel_2024_naa = fetch_data(
            url=POST_URL,
            payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
            error_messages=error_messages,
            query_name="Andel 2024 - NÅ",
            response_type="json",  # The expected response type, either 'json' or 'csv'.
        )
        print(f"Successfully fetched data for years: {years_to_query}")

    except Exception as e:
        # Check if the error is a 400 Bad Request and there are years to remove
        if "400 Client Error" in str(e) and len(years_to_query) > 1:
            removed_year = years_to_query.pop()
            print(f"Query failed for years up to {removed_year}. Retrying with years up to {years_to_query[-1]}.")
            # If we successfully fall back from current year, explain this is normal
            if removed_year == str(current_year):
                print(f"NOTE: This error is perfectly normal - data for {removed_year} is not yet available in SSB. The script will continue with data up to {years_to_query[-1]}.")
        else:
            print(f"An unrecoverable error occurred: {e}")
            notify_errors(error_messages, script_name=script_name)
            raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

# If df is still None after the loop, it means all attempts failed
if df_andel_2024_naa is None:
    error_msg = "Could not fetch data from SSB after multiple attempts."
    print(error_msg)
    error_messages.append(error_msg)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(error_msg)

# Process df_andel_2024_naa: filter most recent year and kjønn = "Begge kjønn"
# Get the most recent year
latest_year_2024 = df_andel_2024_naa['år'].max()

# Filter for most recent year and "Begge kjønn"
df_filtered = df_andel_2024_naa[
    (df_andel_2024_naa['år'] == latest_year_2024) & 
    (df_andel_2024_naa['kjønn'] == 'Begge kjønn')
].copy()

# Create "Høyere utdanning, kort eller lang" by summing the two university levels
# First, get the data for the two university levels
df_kort = df_filtered[df_filtered['nivå'] == 'Universitets- og høgskolenivå, kort'].copy()
df_lang = df_filtered[df_filtered['nivå'] == 'Universitets- og høgskolenivå, lang'].copy()

# Sum the values by region
df_hoyere_utdanning = df_kort.groupby('region')['value'].sum().reset_index()
df_lang_grouped = df_lang.groupby('region')['value'].sum().reset_index()

# Merge and sum the values
df_hoyere_utdanning = df_hoyere_utdanning.merge(df_lang_grouped, on='region', how='outer', suffixes=('_kort', '_lang'))
df_hoyere_utdanning['value'] = df_hoyere_utdanning['value_kort'].fillna(0) + df_hoyere_utdanning['value_lang'].fillna(0)

# Keep only region and the combined value
df_hoyere_utdanning = df_hoyere_utdanning[['region', 'value']].copy()
df_hoyere_utdanning['nivå'] = 'Høyere utdanning, kort eller lang'

# Remove the original university levels from the filtered dataframe
df_filtered_clean = df_filtered[
    ~df_filtered['nivå'].isin(['Universitets- og høgskolenivå, kort', 'Universitets- og høgskolenivå, lang'])
].copy()

# Add the new combined education level
df_hoyere_utdanning_full = df_hoyere_utdanning.copy()
# Add the other columns to match the structure
if len(df_filtered_clean) > 0:
    sample_row = df_filtered_clean.iloc[0]
    for col in df_filtered_clean.columns:
        if col not in df_hoyere_utdanning_full.columns:
            df_hoyere_utdanning_full[col] = sample_row[col]

# Combine the dataframes
df_final = pd.concat([df_filtered_clean, df_hoyere_utdanning_full], ignore_index=True)

# Create the final table with region and "Høyere utdanning, kort eller lang" values
df_hoyere_utdanning_table = df_hoyere_utdanning[['region', 'value']].copy()
df_hoyere_utdanning_table.columns = ['Kommune', 'Høyere utdanning, kort eller lang']

# Sort by value descending
df_hoyere_utdanning_table = df_hoyere_utdanning_table.sort_values('Høyere utdanning, kort eller lang', ascending=False).reset_index(drop=True)

print(f"\nHøyere utdanning (kort eller lang) per region for {latest_year_2024}:")
print(df_hoyere_utdanning_table.to_string(index=False))



################################ Andel 2020 - 2023


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:Komm2020",
        "values": [
          "3806",
          "3807",
          "3808",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825"
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
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
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
        "filter": "item",
        "values": [
          "2020",
          "2021",
          "2022",
          "2023"
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
    df_andel_2020_2023 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Andel 2020 - 2023",
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



################################ Andel 2004 - 2019


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Kommune",
        "values": [
          "0805",
          "0806",
          "0807",
          "0811",
          "0814",
          "0815",
          "0817",
          "0819",
          "0821",
          "0822",
          "0826",
          "0827",
          "0828",
          "0829",
          "0830",
          "0831",
          "0833",
          "0834"
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
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
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
        "filter": "item",
        "values": [
          "2004",
          "2005",
          "2006",
          "2007",
          "2008",
          "2009",
          "2010",
          "2011",
          "2012",
          "2013",
          "2014",
          "2015",
          "2016",
          "2017",
          "2018",
          "2019"
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
    df_andel_2004_2019 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Andel 2004 - 2019",
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

### DATA CLEANING

# Combine the dataframes
df_antall = pd.concat([df_antall_2024_naa, df_antall_2020_2023, df_antall_2004_2019], ignore_index=True)
df_andel = pd.concat([df_andel_2024_naa, df_andel_2020_2023, df_andel_2004_2019], ignore_index=True)

df_combined = pd.concat([df_antall, df_andel], ignore_index=True)

#Rename the columnss to "Kommune", "Utdanningsnivå", "Kjønn", "Variabel", "År" and "Verdi"
df_combined.columns = ["Kommune", "Utdanningsnivå", "Kjønn", "Variabel", "År", "Verdi"]

# Standardize data types to match what github_functions.py expects
# Convert 'År' to string to match GitHub format (github_functions.py keeps all columns as strings)
df_combined['År'] = df_combined['År'].astype(str)

# Keep 'Verdi' as string and handle NaN properly
# Convert numeric values to strings, and NaN/empty values to empty strings to match github_functions.py
df_combined['Verdi'] = df_combined['Verdi'].apply(lambda x: str(float(x)) if pd.notna(x) and str(x).strip() != '' else '')
# Clean up any 'nan' strings that might have been created
df_combined['Verdi'] = df_combined['Verdi'].replace('nan', '')

# Ensure other columns are strings
for col in ['Kommune', 'Utdanningsnivå', 'Kjønn', 'Variabel']:
    df_combined[col] = df_combined[col].astype(str)

# Sort the dataframe to ensure consistent order
df_combined = df_combined.sort_values(by=list(df_combined.columns)).reset_index(drop=True)

# Data is now properly formatted for comparison

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "utdanningsnivaa.csv"
task_name = "Opplaering og kompetanse - Utdanningsnivaa"
github_folder = "Data/02_Opplæring og kompetanse/Utdanningsnivå"
temp_folder = os.environ.get("TEMP_FOLDER")

# Temporary debugging: Let's see what the GitHub comparison is actually doing
from Helper_scripts.github_functions import download_github_file

# Save our file first
temp_csv_path = os.path.join(temp_folder, file_name)
df_combined.to_csv(temp_csv_path, index=False, encoding="utf-8")

# Download the GitHub version and compare directly
github_df = download_github_file(f"{github_folder}/{file_name}")

if github_df is not None:
    print(f"\nDirect comparison:")
    print(f"Local df shape: {df_combined.shape}")
    print(f"GitHub df shape: {github_df.shape}")
    print(f"Local df dtypes:\n{df_combined.dtypes}")
    print(f"GitHub df dtypes:\n{github_df.dtypes}")
    
    # Check if they're equal
    are_equal = df_combined.equals(github_df)
    print(f"DataFrames equal: {are_equal}")
    
    if not are_equal:
        # Check if shapes are different first
        if df_combined.shape != github_df.shape:
            print(f"Different shapes detected - this indicates new data has been added.")
            print(f"Difference: {df_combined.shape[0] - github_df.shape[0]} rows")
        else:
            # Only do detailed comparison if shapes are the same
            # Check each column
            for col in df_combined.columns:
                if col in github_df.columns:
                    col_equal = df_combined[col].equals(github_df[col])
                    print(f"Column '{col}' equal: {col_equal}")
                    if not col_equal:
                        # Show first few differences
                        diff_mask = df_combined[col] != github_df[col]
                        if diff_mask.any():
                            print(f"  First difference at index {diff_mask.idxmax()}: '{df_combined[col].iloc[diff_mask.idxmax()]}' vs '{github_df[col].iloc[diff_mask.idxmax()]}'")

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


################################ Landet utenom Oslo, siste år (for tekst på nettside)

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
          "04a",
          "09a"
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
    df_antall_alle_fylker = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Antall alle fylker siste år",
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

# Get the year from the data (since we use "top 1" filter)
latest_year = df_antall_alle_fylker['år'].iloc[0] if 'år' in df_antall_alle_fylker.columns else "ukjent år"

# Calculate percentage of each education level (nivå)
# Group by 'nivå' and sum the values to get total for each education level
df_nivaa_summary = df_antall_alle_fylker.groupby('nivå')['value'].sum().reset_index()
df_nivaa_summary.columns = ['nivå', 'antall']

# Calculate total across all education levels
total_personer = df_nivaa_summary['antall'].sum()

# Calculate percentage for each education level
df_nivaa_summary['prosent'] = (df_nivaa_summary['antall'] / total_personer * 100).round(2)

# Sort by percentage descending for better readability
df_nivaa_summary = df_nivaa_summary.sort_values('prosent', ascending=False).reset_index(drop=True)

print(f"\nUtdanningsnivå fordeling for {latest_year}:")
print(f"Total antall personer: {total_personer:,}")
print("\nFordeling per utdanningsnivå:")
print(df_nivaa_summary.to_string(index=False))

# Calculate the sum of "Universitets- og høgskolenivå, kort" og "Universitets- og høgskolenivå, lang"
universitets_prosent = df_nivaa_summary[df_nivaa_summary['nivå'].isin(['Universitets- og høgskolenivå, kort', 'Universitets- og høgskolenivå, lang'])]['prosent'].sum()

# Print a sentence "Andel personer i landet utenom Oslo som har høyere utdanning (kort eller lang) er {universitets_prosent:.1f} %"
print(f"Andel personer i landet utenom Oslo som har høyere utdanning (kort eller lang) er {universitets_prosent:.1f} %")


