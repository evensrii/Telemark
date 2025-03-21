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

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/07984/"


################## SYSSELSATTE I KOMMUNER (BOSATTE)

# Spørring for å hente ut data fra SSB
payload_kommuner = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommSummerS",
        "values": [
          "K.4001",
          "K.4003",
          "K.4005",
          "K.4010",
          "K.4012",
          "K.4014",
          "K.4016",
          "K.4018",
          "K.4020",
          "K.4022",
          "K.4024",
          "K.4026",
          "K.4028",
          "K.4030",
          "K.4032",
          "K.4034",
          "K.4036"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-19",
          "20-24",
          "25-39",
          "40-54",
          "55-66",
          "67-74"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Sysselsatte"
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
        query_name="Sysselsatte, kommuner",
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

# Fetch latest year from "år" column in "df_kommuner"

latest_year = df_kommuner['år'].max()

################## SYSSELSATTE I LANDET (FORDELT PÅ ALDERSGRUPPER)


# Spørring for å hente ut data fra SSB
payload_landet = {
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
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-19",
          "20-24",
          "25-39",
          "40-54",
          "55-66",
          "67-74"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Sysselsatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [latest_year]
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
        query_name="Sysselsatte, landet",
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


#### PERSONER I LANDET

# Endepunkt for SSB API
POST_URL_PERSONER = "https://data.ssb.no/api/v0/no/table/07459/"


# Spørring for å hente ut data fra SSB
payload_personer_landet = {
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
      "code": "Alder",
      "selection": {
        "filter": "vs:AlleAldre00B",
        "values": [
          "015",
          "016",
          "017",
          "018",
          "019",
          "020",
          "021",
          "022",
          "023",
          "024",
          "025",
          "026",
          "027",
          "028",
          "029",
          "030",
          "031",
          "032",
          "033",
          "034",
          "035",
          "036",
          "037",
          "038",
          "039",
          "040",
          "041",
          "042",
          "043",
          "044",
          "045",
          "046",
          "047",
          "048",
          "049",
          "050",
          "051",
          "052",
          "053",
          "054",
          "055",
          "056",
          "057",
          "058",
          "059",
          "060",
          "061",
          "062",
          "063",
          "064",
          "065",
          "066",
          "067",
          "068",
          "069",
          "070",
          "071",
          "072",
          "073",
          "074"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          f"{latest_year}"
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
    df_personer_landet = fetch_data(
        url=POST_URL_PERSONER,
        payload=payload_personer_landet,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Personer, landet",
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


##### PERSONER I KOMMUNENE

# Spørring for å hente ut data fra SSB
payload_personer_kommunene = {
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
      "code": "Alder",
      "selection": {
        "filter": "vs:AlleAldre00B",
        "values": [
          "015",
          "016",
          "017",
          "018",
          "019",
          "020",
          "021",
          "022",
          "023",
          "024",
          "025",
          "026",
          "027",
          "028",
          "029",
          "030",
          "031",
          "032",
          "033",
          "034",
          "035",
          "036",
          "037",
          "038",
          "039",
          "040",
          "041",
          "042",
          "043",
          "044",
          "045",
          "046",
          "047",
          "048",
          "049",
          "050",
          "051",
          "052",
          "053",
          "054",
          "055",
          "056",
          "057",
          "058",
          "059",
          "060",
          "061",
          "062",
          "063",
          "064",
          "065",
          "066",
          "067",
          "068",
          "069",
          "070",
          "071",
          "072",
          "073",
          "074"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          f"{latest_year}"
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
    df_personer_kommuner = fetch_data(
        url=POST_URL_PERSONER,
        payload=payload_personer_kommunene,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Personer, kommunene",
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



################## SYSSELSETTING I LANDET (15-74)

sysselsatte_15_74_landet = df_landet['value'].sum()

personer_15_74_landet = df_personer_landet['value'].sum()

sysselsetting_15_74_landet = sysselsatte_15_74_landet / personer_15_74_landet * 100

print(f"\nSysselsetting for 15-74 i landet i {latest_year}: {sysselsetting_15_74_landet:.1f}%")


################## SYSSELSETTING I TELEMARK (15-74)

sysselsatte_15_74_telemark = df_kommuner['value'].sum()

personer_15_74_telemark = df_personer_kommuner['value'].sum()

sysselsetting_15_74_telemark = sysselsatte_15_74_telemark / personer_15_74_telemark * 100

print(f"Sysselsetting for 15-74 i Telemark i {latest_year}: {sysselsetting_15_74_telemark:.1f}%")


##### DATABEHANDLING

# Create age groups for df_personer_landet and df_personer_kommuner
def get_age_group(age):
    # Extract the number from the string (e.g., "15 år" -> 15)
    age = int(age.split()[0])
    if 15 <= age <= 19:
        return '15-19'
    elif 20 <= age <= 24:
        return '20-24'
    elif 25 <= age <= 39:
        return '25-39'
    elif 40 <= age <= 54:
        return '40-54'
    elif 55 <= age <= 66:
        return '55-66'
    elif 67 <= age <= 74:
        return '67-74'
    else:
        return 'Other'

# Define age group order
age_group_order = ['15-19', '20-24', '25-39', '40-54', '55-66', '67-74']

# Process landet data
df_personer_landet['age_group'] = df_personer_landet['alder'].apply(get_age_group)
df_personer_landet_grouped = df_personer_landet.groupby('age_group')['value'].sum().reset_index()
df_personer_landet_grouped = df_personer_landet_grouped.set_index('age_group').reindex(age_group_order).reset_index()

# Process kommune data
df_personer_kommuner['age_group'] = df_personer_kommuner['alder'].apply(get_age_group)
df_personer_kommuner_grouped = df_personer_kommuner.groupby(['region', 'age_group'])['value'].sum().reset_index()
df_personer_kommuner_grouped = df_personer_kommuner_grouped.pivot(index='region', columns='age_group', values='value')
df_personer_kommuner_grouped = df_personer_kommuner_grouped.reindex(columns=age_group_order)

# Display results
print("\nLandet age groups:")
print(df_personer_landet_grouped)
print("\nKommuner age groups:")
print(df_personer_kommuner_grouped)

# 1. Create Telemark aggregate for personer (population data)
telemark_sums = df_personer_kommuner_grouped.sum().to_frame().T
telemark_sums.index = ['Telemark']
df_personer_telemark = telemark_sums

# 2. Create Telemark aggregate for sysselsatte (employment data)
df_sysselsatte_telemark = df_kommuner.copy()
df_sysselsatte_telemark['region'] = 'Telemark'
df_sysselsatte_telemark = df_sysselsatte_telemark.groupby(['region', 'alder'])['value'].sum().reset_index()

print("\nTelemark population by age groups:")
print(df_personer_telemark)
print("\nTelemark employment by age groups:")
print(df_sysselsatte_telemark)

# Calculate employment percentages
def calculate_employment_rate(employed_df, population_df):
    rates = {}
    # Map age categories to the groups we want
    age_mapping = {
        '20-24 år': '20-24',
        '25-39 år': '25-39',
        '40-54 år': '40-54',
        '55-66 år': '55-66',
        '67-74 år': '67-74'
    }
    
    for age_cat, age_group in age_mapping.items():
        if age_cat in employed_df['alder'].values:
            employed = employed_df[employed_df['alder'] == age_cat]['value'].iloc[0]
            
            # Get population value based on DataFrame type
            if isinstance(population_df, pd.DataFrame) and 'age_group' in population_df.columns:
                # For landet data
                population = population_df[population_df['age_group'] == age_group]['value'].iloc[0]
            else:
                # For Telemark data (which is a DataFrame with age groups as columns)
                population = population_df.loc['Telemark', age_group]
            
            rate = (employed / population) * 100
            rates[age_cat] = round(rate, 1)
    
    return rates

# Calculate rates for Telemark
telemark_rates = calculate_employment_rate(df_sysselsatte_telemark, df_personer_telemark)

# Calculate rates for landet
landet_rates = calculate_employment_rate(df_landet, df_personer_landet_grouped)

# Create final comparison DataFrame
df_comparison = pd.DataFrame({
    'Aldersgruppe': list(telemark_rates.keys()),
    f'Telemark ({latest_year})': list(telemark_rates.values()),
    f'Hele landet ({latest_year})': list(landet_rates.values())
})

# Reorder columns
df_comparison = df_comparison[['Aldersgruppe', f'Telemark ({latest_year})', f'Hele landet ({latest_year})']]

print("\nEmployment rates by age group (%):")
print(df_comparison)

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "sysselsatte_etter_aldersgruppe.csv"
task_name = "Arbeid og naeringsliv - Sysselsatte etter aldersgruppe"
github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/Sysselsetting"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_comparison, file_name, github_folder, temp_folder, keepcsv=True)

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