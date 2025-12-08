import os
import pandas as pd
import requests
from pyjstat import pyjstat

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

################# Spørring #################

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/07459/"

# Spørring for å hente ut data fra SSB
payload = {
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
      "code": "Alder",
      "selection": {
        "filter": "vs:AlleAldre00B",
        "values": []
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2010",
          "2011",
          "2012",
          "2013",
          "2014",
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",
          "2022",
          "2023",
          "2024",
          "2025"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

################# Hent data #################

print("Fetching data from SSB API...")

# Send POST request to SSB API
response = requests.post(POST_URL, json=payload)

# Check if request was successful
if response.status_code == 200:
    # Convert JSON-stat format to pandas DataFrame
    dataset = pyjstat.Dataset.read(response.text)
    df = dataset.write('dataframe')
    
    print(f"Data fetched successfully! Shape: {df.shape}")
    print("\nFirst few rows:")
    print(df.head())
else:
    print(f"Error: Failed to fetch data. Status code: {response.status_code}")
    print(response.text)
    exit(1)

################# Save to CSV #################

# Define output file path (same folder as script)
output_file = os.path.join(script_dir, "befolkning.csv")

# Save DataFrame to CSV
df.to_csv(output_file, index=False, sep=";", encoding="utf-8-sig")

print(f"\nData saved to: {output_file}")
print("Script completed successfully!")
