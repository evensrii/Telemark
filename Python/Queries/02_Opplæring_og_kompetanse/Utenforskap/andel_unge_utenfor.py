import os

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring #################

# SSB API v2 GET URL (tabell 13556 - Utenforskap)
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
        query_name="Andel unge utenfor arbeid og utdanning (13556)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### Spørring 2: Hele landet ###

GET_URL_LANDET = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13556/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=BosatteProsent"
    "&valuecodes[Tid]=top(1)"
    "&valuecodes[Region]=*"
    "&codelist[Region]=vs_Landet"
    "&valuecodes[HovArbStyrkStatus]=NEET"
    "&codelist[HovArbStyrkStatus]=vs_ArbStatus2018niva4"
    "&valuecodes[Alder]=15-29"
    "&valuecodes[Kjonn]=0"
    "&heading=Tid,ContentsCode,HovArbStyrkStatus"
    "&stub=Region,Alder,Kjonn"
)

try:
    df_landet = fetch_data(
        url=GET_URL_LANDET,
        payload=None,
        error_messages=error_messages,
        query_name="Andel unge utenfor - Hele landet (13556)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### Spørring 3: Telemark fylke ###

GET_URL_TELEMARK = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13556/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=BosatteProsent"
    "&valuecodes[Tid]=top(1)"
    "&valuecodes[Region]=40"
    "&codelist[Region]=agg_FylkerGjeldende"
    "&valuecodes[HovArbStyrkStatus]=NEET"
    "&codelist[HovArbStyrkStatus]=vs_ArbStatus2018niva4"
    "&valuecodes[Alder]=15-29"
    "&valuecodes[Kjonn]=0"
    "&heading=Tid,ContentsCode,HovArbStyrkStatus"
    "&stub=Alder,Kjonn,Region"
)

try:
    df_telemark = fetch_data(
        url=GET_URL_TELEMARK,
        payload=None,
        error_messages=error_messages,
        query_name="Andel unge utenfor - Telemark (13556)",
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

# Extract the year from the data (used in column name)
year = df.iloc[0]["år"]

# Keep only Region and value columns, rename
df = df[["region", "value"]].copy()
df.columns = ["Kommune", f"Andel ({year})"]

# Same for Telemark data
df_telemark = df_telemark[["region", "value"]].copy()
df_telemark.columns = ["Kommune", f"Andel ({year})"]

# Same for national data
df_landet = df_landet[["region", "value"]].copy()
df_landet.columns = ["Kommune", f"Andel ({year})"]

# Combine: municipalities, then Telemark, then Norge (bottom)
import pandas as pd
df = pd.concat([df, df_telemark, df_landet], ignore_index=True)

# Add Label column (same as Kommune)
df["Label"] = df["Kommune"]

df.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_unge_utenfor.csv"
task_name = "Opplaering - Andel unge utenfor"
github_folder = "Data/02_Opplæring og kompetanse/Utenforskap"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True)

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