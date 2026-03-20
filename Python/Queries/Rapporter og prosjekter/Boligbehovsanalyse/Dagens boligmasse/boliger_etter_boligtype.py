import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring #################

# SSB API v2 GET URL (tabell 06265 - Boliger etter boligtype)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06265/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=*"
    "&valuecodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&codelist[Region]=agg_KommSummer"
    "&valuecodes[BygnType]=*"
    "&heading=ContentsCode,Tid,BygnType"
    "&stub=Region"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Boliger etter boligtype (06265)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(df.head())
print(df.columns.tolist())

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "boliger_etter_boligtype.csv"
github_folder = "Data/Boligbehovsanalyse_2026/Dagens boligmasse"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")
