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

# Example list of error messages to collect errors during execution
error_messages = []

# Endepunkt for SSB API
POST_URL_sysselsatte = "https://data.ssb.no/api/v0/no/table/13472/"


##### Query for å finne siste år i datasettet

# Spørring for å hente ut data fra SSB
payload_siste_aar = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg:KommSummer",
        "values": [
          "K-4001"
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
        "values": ["1"]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_siste_aar = fetch_data(
        url=POST_URL_sysselsatte,
        payload=payload_siste_aar,
        error_messages=error_messages,
        query_name="Sysselsatte, siste år",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Get most recent year
most_recent_year = df_siste_aar['år'].iloc[0]

# Create a list of years from 2016 until most_recent_year, years as strings enclosed in ""
years = [str(year) for year in range(2016, int(most_recent_year) + 1)]


################## ARBEIDSPLASSER OVER TID I FYLKET OG LANDET (15-74 ÅR) (Sysselsatte etter arbeidssted)

####### FYLKET

# Spørring for å hente ut data fra SSB
payload_fylket = {
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
        "filter": "item",
        "values": years
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_fylket = fetch_data(
        url=POST_URL_sysselsatte,
        payload=payload_fylket,
        error_messages=error_messages,
        query_name="Sysselsatte over tid, fylket",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Create a mask for private sector in df_fylket
private_sector_mask_fylket = df_fylket['sektor'] == 'Privat sektor'

# Split df_fylket into private and public sectors
private_sector_fylket = df_fylket[private_sector_mask_fylket].copy()
public_sector_fylket = df_fylket[~private_sector_mask_fylket].copy()

# Sum up all public sector values for each region in df_fylket
public_sector_sum_fylket = public_sector_fylket.groupby(['region', 'statistikkvariabel', 'år'])['value'].sum().reset_index()
public_sector_sum_fylket['sektor'] = 'Offentlig sektor'

# Combine private and public sector data for df_fylket
df_fylket = pd.concat([public_sector_sum_fylket, private_sector_fylket], ignore_index=True)

##### LANDET

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
        "filter": "item",
        "values": years
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

try:
    df_landet = fetch_data(
        url=POST_URL_sysselsatte,
        payload=payload_landet,
        error_messages=error_messages,
        query_name="Sysselsatte over tid, landet",
        response_type="json"
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Create a mask for private sector in df_landet
private_sector_mask_landet = df_landet['sektor'] == 'Privat sektor'

# Split df_landet into private and public sectors
private_sector_landet = df_landet[private_sector_mask_landet].copy()
public_sector_landet = df_landet[~private_sector_mask_landet].copy()

# Sum up all public sector values for each region in df_landet
public_sector_sum_landet = public_sector_landet.groupby(['region', 'statistikkvariabel', 'år'])['value'].sum().reset_index()
public_sector_sum_landet['sektor'] = 'Offentlig sektor'

# Combine private and public sector data for df_landet
df_landet = pd.concat([public_sector_sum_landet, private_sector_landet], ignore_index=True)

### Print to log

print("\nArbeidsplasser over tid, fylket:")
print(df_fylket)

print("\nArbeidsplasser over tid, landet:")
print(df_landet)

# Print percentage change in employees for public and private sectors in fylket
private_fylket = df_fylket[df_fylket['sektor'] == 'Privat sektor']
public_fylket = df_fylket[df_fylket['sektor'] == 'Offentlig sektor']

private_fylket_change = ((private_fylket['value'].iloc[-1] - private_fylket['value'].iloc[0]) / private_fylket['value'].iloc[0] * 100).round(1)
public_fylket_change = ((public_fylket['value'].iloc[-1] - public_fylket['value'].iloc[0]) / public_fylket['value'].iloc[0] * 100).round(1)

print(f"\nEndring i antall arbeidsplasser i fylket fra {years[0]} til {most_recent_year}:")
print(f"Offentlig sektor: {public_fylket_change:.1f}% ({public_fylket['value'].iloc[0]:.0f} -> {public_fylket['value'].iloc[-1]:.0f})")
print(f"Privat sektor: {private_fylket_change:.1f}% ({private_fylket['value'].iloc[0]:.0f} -> {private_fylket['value'].iloc[-1]:.0f})")

# Print percentage change in employees for public and private sectors in landet

private_landet = df_landet[df_landet['sektor'] == 'Privat sektor']
public_landet = df_landet[df_landet['sektor'] == 'Offentlig sektor']

private_landet_change = ((private_landet['value'].iloc[-1] - private_landet['value'].iloc[0]) / private_landet['value'].iloc[0] * 100).round(1)
public_landet_change = ((public_landet['value'].iloc[-1] - public_landet['value'].iloc[0]) / public_landet['value'].iloc[0] * 100).round(1)

print(f"\nEndring i antall arbeidsplasser i landet fra {years[0]} til {most_recent_year}:")
print(f"Offentlig sektor: {public_landet_change:.1f}% ({public_landet['value'].iloc[0]:.0f} -> {public_landet['value'].iloc[-1]:.0f})")
print(f"Privat sektor: {private_landet_change:.1f}% ({private_landet['value'].iloc[0]:.0f} -> {private_landet['value'].iloc[-1]:.0f})")


################## ARBEIDSPLASSER I KOMMUNER (15-74 ÅR) SISTE ÅR!! (Dvs. sysselsatte etter arbeidssted)

#values = [str(year) for year in range(2016, datetime.now().year - 1)]

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
        "filter": "item",
        "values": years
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

# Create a mask for private sector in df_sysselsatte
private_sector_mask = df_sysselsatte['sektor'] == 'Privat sektor'

# Split df_sysselsatte into private and public sectors
private_sector = df_sysselsatte[private_sector_mask].copy()
public_sector = df_sysselsatte[~private_sector_mask].copy()

# Sum up all public sector values for each region in df_sysselsatte
public_sector_sum = public_sector.groupby(['region', 'statistikkvariabel', 'år'])['value'].sum().reset_index()
public_sector_sum['sektor'] = 'Offentlig sektor'

# Combine private and public sector data for df_sysselsatte
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

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "offentlige_vs_privat_naringsliv.csv"
task_name = "Arbeid og naeringsliv - Offentlig vs. privat naeringsliv"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Næringsstruktur og arbeidsplasser"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_processed, file_name, github_folder, temp_folder, keepcsv=True)

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
