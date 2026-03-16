import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring #################

# SSB API v2 GET URL (tabell 07459)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13556/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=BosatteProsent"
    "&valuecodes[Tid]=top(1)"
    "&valuecodes[Region]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036"
    "&codelist[Region]=agg_KommGjeldende"
    "&valuecodes[HovArbStyrkStatus]=NEET"
    "&codelist[HovArbStyrkStatus]=vs_ArbStatus2018niva4"
    "&valuecodes[Alder]=15-29"
    "&valuecodes[Kjonn]=0"
    "&heading=Tid,ContentsCode,HovArbStyrkStatus"
    "&stub=Region,Alder,Kjonn"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Befolkning etter alder og kommune (07459)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### DATA CLEANING

df.head()

# ... your cleaning steps here ...

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

# file_name = "xxx.csv"
# task_name = "Tema - Tittel"
# github_folder = "Data/01_Befolkning/..."
# temp_folder = os.environ.get("TEMP_FOLDER")
# ... etc (standard output block) ...