import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
from datetime import datetime

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data


# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/13563/"


################## KOMMUNER

# Spørring for å hente ut data fra SSB
payload_kommuner = {
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
      "code": "HovArbStyrkStatus",
      "selection": {
        "filter": "vs:ArbStatus2018niva2a",
        "values": [
          "A.01",
          "A.09",
          "U.01",
          "U.03",
          "U.04-U.05",
          "U.06-U.07",
          "U.90A"
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
    df_kommuner = fetch_data(
        url=POST_URL,
        payload=payload_kommuner,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Arbeidsmarkedstilknytning, kommuner",
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

###################### TELEMARK FYLKE

# Spørring for å hente ut data fra SSB
payload_telemark = {
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
      "code": "HovArbStyrkStatus",
      "selection": {
        "filter": "vs:ArbStatus2018niva2a",
        "values": [
          "A.01",
          "A.09",
          "U.01",
          "U.03",
          "U.04-U.05",
          "U.06-U.07",
          "U.90A"
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
        payload=payload_telemark,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Arbeidsmarkedstilknytning, Telemark",
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

print("\nArbeidsmarkedstilknytning i Telemark:")
print(df_telemark)

###################### LANDET

# Spørring for å hente ut data fra SSB
payload_landet = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet",
        "values": []
      }
    },
    {
      "code": "HovArbStyrkStatus",
      "selection": {
        "filter": "vs:ArbStatus2018niva2a",
        "values": [
          "A.01",
          "A.09",
          "U.01",
          "U.03",
          "U.04-U.05",
          "U.06-U.07",
          "U.90A"
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
        payload=payload_landet,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Arbeidsmarkedstilknytning, Landet",
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



###################### FYLKENE

# Spørring for å hente ut data fra SSB
payload_fylker = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommFylker",
        "values": [
          "F-31",
          "F-32",
          "F-03",
          "F-34",
          "F-33",
          "F-39",
          "F-40",
          "F-42",
          "F-11",
          "F-46",
          "F-15",
          "F-50",
          "F-18",
          "F-55",
          "F-56"
        ]
      }
    },
    {
      "code": "HovArbStyrkStatus",
      "selection": {
        "filter": "vs:ArbStatus2018niva2a",
        "values": [
          "A.01",
          "A.09",
          "U.01",
          "U.03",
          "U.04-U.05",
          "U.06-U.07",
          "U.90A"
        ]
      }
    },
    {
      "code": "InnvandrKat",
      "selection": {
        "filter": "item",
        "values": [
          "A-G"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
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
    df_fylker = fetch_data(
        url=POST_URL,
        payload=payload_fylker,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Arbeidsmarkedstilknytning, fylker",
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

###################### TABLE 05749 KOMMUNER

# Add a new query for table 05749 for kommuner
POST_URL_05749 = "https://data.ssb.no/api/v0/no/table/05749/"

payload_05749_kommuner = {
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
            "code": "ContentsCode",
            "selection": {
                "filter": "item",
                "values": [
                    "Verdi1",  # Replace with actual content codes for table 05749
                    "Verdi2"
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

try:
    df_05749_kommuner = fetch_data(
        url=POST_URL_05749,
        payload=payload_05749_kommuner,
        error_messages=error_messages,
        query_name="Table 05749, kommuner",
        response_type="json"
    )
    print("\nData from table 05749 for kommuner:")
    print(df_05749_kommuner)
except Exception as e:
    print(f"Error occurred while fetching data from table 05749: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching for table 05749, stopping execution."
    )

# Function to process each dataframe
def process_dataframe(df):
    # Get the year from the 'år' column
    year = df['år'].iloc[0]
    
    # Find the column that starts with 'Arbeidsstyrkestatus'
    old_col = [col for col in df.columns if col.startswith('prioritert arbeidsstyrkestatus')][0]
    
    # Create a mapping for column renaming
    rename_map = {
        old_col: f'Arbeidsstyrkestatus ({year})',
        df.columns[-1]: 'Antall'  # Rename the last column to 'Antall'
    }
    
    # Rename columns
    df = df.rename(columns=rename_map)
    
    # Drop unwanted columns
    df = df.drop(['statistikkvariabel', 'år'], axis=1)
    
    return df

# Process each dataframe
df_kommuner = process_dataframe(df_kommuner)
df_telemark = process_dataframe(df_telemark)
df_landet = process_dataframe(df_landet)
df_fylker = process_dataframe(df_fylker)

def calculate_percentage_share(df):
    """Calculate the percentage share of 'Antall' for each region."""
    # Group by region to get total
    region_totals = df.groupby('region')['Antall'].sum().reset_index()
    region_totals = region_totals.rename(columns={'Antall': 'Total'})
    
    # Merge the totals back to the original dataframe
    df = df.merge(region_totals, on='region')
    
    # Calculate percentage
    df['Andel'] = (df['Antall'] / df['Total'] * 100).round(1)
    
    # Drop the temporary Total column
    df = df.drop('Total', axis=1)
    
    return df

# Apply the percentage calculation to df_kommuner and df_telemark
df_kommuner = calculate_percentage_share(df_kommuner)
df_telemark = calculate_percentage_share(df_telemark)
df_fylker = calculate_percentage_share(df_fylker)

############ TILLEGGSINFO TIL TEKST PÅ NETTSIDER ###

# Find the column that starts with 'Arbeidsstyrkestatus'
arbeidsstatus_col = [col for col in df_fylker.columns if col.startswith('Arbeidsstyrkestatus')][0]

# Filter rows for AFP/alderspensjon recipients
df_pensjonister = df_fylker[df_fylker[arbeidsstatus_col].str.startswith('Mottakere av AFP/alderspensjon', na=False)]

# Sort by "Andel" in descending order
df_pensjonister = df_pensjonister.sort_values(by='Andel', ascending=False)

print("\nPensjonister per fylke:")
print(df_pensjonister)

###---


# Filter rows for AFP/alderspensjon recipients
df_uføre = df_fylker[df_fylker[arbeidsstatus_col].str.startswith('Mottakere av arbeidsavklaringspenger / uføretrygd', na=False)]

# Sort by "Andel" in descending order
df_uføre = df_uføre.sort_values(by='Andel', ascending=False)

print("\nUføre per fylke:")
print(df_uføre)


####

# Create column "region" in "df_landet", and fill with value "Hele landet"
df_landet['region'] = 'Hele landet'

# Make region the first column
df_landet = df_landet[['region'] + [col for col in df_landet.columns if col != 'region']]

df_landet = calculate_percentage_share(df_landet)


# Concatenate the three dataframes. Dont sort 
df = pd.concat([df_landet, df_telemark, df_kommuner], ignore_index=True)

# Drop the "Antall" colum from df
df = df.drop('Antall', axis=1)

# Get the year from the column name
year_col = [col for col in df.columns if col.startswith('Arbeidsstyrkestatus')][0]
year = year_col.split('(')[1].split(')')[0]

# Store the original order of regions before pivot
region_order = df['region'].unique()

# Pivot the DataFrame to get the desired format
df_pivoted = df.pivot(
    index='region',
    columns=f'Arbeidsstyrkestatus ({year})',
    values='Andel'
)

# Reset the index to make 'region' a regular column
df_pivoted.reset_index(inplace=True)

# Set the index to match the original order
df_pivoted['order'] = df_pivoted['region'].map({region: idx for idx, region in enumerate(region_order)})
df_pivoted = df_pivoted.sort_values('order').drop('order', axis=1)

# Rename columns
df_pivoted = df_pivoted.rename(columns={
    "region": "Region",
    "Deltakere på arbeidsmarkedstiltak": "Arbeidsmarkedstiltak",
    "Mottakere av AFP/alderspensjon": "AFP/Alderspensjon",
    "Mottakere av arbeidsavklaringspenger / uføretrygd": "AAP/Uføretrygd",
    "Registrerte arbeidsledige": "Arbeidsledige",
    "Sysselsatte": "Sysselsatte",
    "Under ordinær utdanning": "Utdanning"})

# Split the DataFrame into fixed rows (first two) and rows to sort
fixed_rows = df_pivoted.iloc[:2]
rows_to_sort = df_pivoted.iloc[2:]

# Sort the remaining rows by "Sysselsatte"
sorted_rows = rows_to_sort.sort_values(by=["Sysselsatte", "AFP/Alderspensjon"], ascending=False)

# Concatenate the fixed rows with the sorted rows
df_pivoted = pd.concat([fixed_rows, sorted_rows]).reset_index(drop=True)

# Reorder columns based on mean values (excluding 'Region', 'Sysselsatte', 'Arbeidsledige', and 'Andre')
value_columns = [col for col in df_pivoted.columns if col not in ['Region', 'Sysselsatte', 'Arbeidsledige', 'Andre']]
column_means = df_pivoted[value_columns].mean()
sorted_columns = column_means.sort_values(ascending=False).index.tolist()

# Create final column order: Region, Sysselsatte, Arbeidsledige first, then sorted columns by mean, Andre last
new_column_order = ['Region', 'Sysselsatte', 'Arbeidsledige'] + sorted_columns + ['Andre']
df_pivoted = df_pivoted[new_column_order]

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "arbeidsmarkedstilknytning_per_kommune.csv"
task_name = "Arbeid og naeringsliv - Arbeidsmarkedstilknytning per kommune"
github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/Sysselsetting"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_pivoted, file_name, github_folder, temp_folder, keepcsv=True)

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