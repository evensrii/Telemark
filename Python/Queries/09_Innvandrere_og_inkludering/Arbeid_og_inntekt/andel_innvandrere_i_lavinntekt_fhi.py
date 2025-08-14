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

###### Andel innvandrere og øvrig befolkning i lavinntekt ######

# Base URL for FHI API
base_url = "https://statistikk-data.fhi.no/api/open/v1/nokkel/Table/403"

# First get the query structure
query_url = f"{base_url}/query"

try:
    # Fetch the query structure
    query = fetch_data(
        url=query_url,
        error_messages=error_messages,
        query_name="Query Structure",
        payload=None,
        response_type="json"
    )
    
    # Data URL for actual data fetch
    data_url = f"{base_url}/data"

    # Create payload with specific dimensions we want
    payload = {
        "dimensions": [
            {
                "code": "GEO",
                "filter": "item",
                "values": ["38"]  # Vestfold og Telemark
            },
            {
                "code": "AAR",
                "filter": "item",
                "values": ["2023_2023"]  # Most recent year
            },
            {
                "code": "ALDER",
                "filter": "item",
                "values": ["0_120"]  # All ages
            },
            {
                "code": "INNVKAT",
                "filter": "item",
                "values": ["0", "2"]  # Specific immigrant categories
            },
            {
                "code": "MAAL",
                "filter": "item",
                "values": ["ant_eu60"]  # EU60 poverty line
            },
            {
                "code": "MEASURE_TYPE",
                "filter": "item",
                "values": ["RATE"]  # Get rates/percentages
            }
        ],
        "response": {
            "format": "json-stat2"
        }
    }

    # Use our constructed payload for the data request
    df = fetch_data(
        url=data_url,
        error_messages=error_messages,
        query_name="Lavinntekt",
        payload=payload,
        response_type="json",
    )

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# Process the data
# df.info()
df.head()

# Save the data to file if needed
# handle_output_data(df, "lavinntekt_fhi.csv")
