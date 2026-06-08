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

# SSB API v2 GET URL (tabell 11607 - Sysselsatte etter innvandrerbakgrunn)
# Region: 40=Telemark (2024-), 38=Vestfold og Telemark (2020-2023), 08=Telemark (før 2020)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/11607/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[Tid]=from(2015)"
    "&valuecodes[Region]=40,38,08"
    "&codelist[Region]=vs_FylkerAlle"
    "&valuecodes[Landbakgrunn]=abc,ddd,eee"
    "&valuecodes[Alder]=15-74"
    "&valuecodes[Kjonn]=0"
    "&valuecodes[ContentsCode]=Sysselsatte2"
    "&heading=ContentsCode,Tid,Alder,Kjonn"
    "&stub=Region,Landbakgrunn"
)

## Kjøre spørring i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Sysselsatte etter innvandrerbakgrunn (11607)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df.head()
df.info()

################# Bearbeide data #################

# Unify region names: all variants of Telemark -> "Telemark"
df["region"] = df["region"].replace(
    {
        "Vestfold og Telemark (2020-2023)": "Telemark",
        "Telemark (-2019)": "Telemark",
    }
)

# Rename values in column "landbakgrunn"
df["landbakgrunn"] = df["landbakgrunn"].replace(
    {
        "Norden utenom Norge, EU/EFTA,  Storbritannia, USA, Canada, Australia, New Zealand": "Gruppe 1-land (EU, Storbritannia, USA ++)",
        "Europa utenom EU/EFTA og Storbritannia, Afrika, Asia, Amerika utenom USA og Canada, Oseania utenom Australia og NZ, polare områder": "Gruppe 2-land (Asia, Afrika, Latin-Amerika ++)",
    }
)

# Remove columns "alder", "kjønn" and "statistikkvariabel"
df = df.drop(columns=["alder", "kjønn", "statistikkvariabel"])

# Remove rows with NaN values
df = df.dropna()

# Keep only "Telemark" rows
df = df[df["region"] == "Telemark"].copy()

# Pivot to wide format: År as rows, landbakgrunn categories as columns
df = df.pivot_table(index="år", columns="landbakgrunn", values="value", aggfunc="first")
df = df.reset_index()
df.columns.name = None

# Rename "år" column
df = df.rename(columns={"år": "År"})

# Ensure correct data types
df["År"] = df["År"].astype(str)

df.head(20)
df.info()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_sysselsatte_innvandrere_etter_bakgrunn.csv"
task_name = "Innvandrerbefolkningen - Sysselsatte etter bakgrunn"
github_folder = "Data/09_Innvandrere og inkludering/Arbeid og inntekt"
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
