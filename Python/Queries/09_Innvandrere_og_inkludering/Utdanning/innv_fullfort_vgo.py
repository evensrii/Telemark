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

################# Fullført - Innvandrere #################

# SSB API v2 GET URL (tabell 14883 - Gjennomføring i videregående opplæring for innvandrere)
GET_URL_innv = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/14883/data?lang=no"
    "&valueCodes[ContentsCode]=Prosent"
    "&valueCodes[Tid]=*"
    "&valueCodes[Region]=08"
    "&valueCodes[FullforingVGO]=1a,2a,4b"
    "&valueCodes[UtdProgram]=00"
    "&valueCodes[Kjonn]=0"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_innvandrere = fetch_data(
        url=GET_URL_innv,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Innvandrere",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df_innvandrere.head()

# Keep only "intervall (år)" and "value" columns
df_innvandrere = df_innvandrere[["intervall (år)", "value"]]

# Sum the three FullforingVGO categories (1a, 2a, 4b) per intervall (år)
df_innvandrere = df_innvandrere.groupby("intervall (år)")["value"].sum().reset_index()

# Legge til kategori for innvandrere
df_innvandrere["gruppe"] = "Innvandrere"


################# Fullført - Hele befolkningen #################

# SSB API v2 GET URL (tabell 14867 - Gjennomføring i videregående opplæring, hele befolkningen)
GET_URL_hele_bef = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/14867/data?lang=no"
    "&valueCodes[ContentsCode]=Prosent"
    "&valueCodes[Tid]=*"
    "&valueCodes[Region]=08"
    "&valueCodes[FullforingVGO]=1a,2a,4b"
    "&valueCodes[UtdProgram]=00"
    "&valueCodes[Kjonn]=0"
)

try:
    df_befolkningen = fetch_data(
        url=GET_URL_hele_bef,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Hele befolkningen",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

df_befolkningen.head()

# Keep only "intervall (år)" and "value" columns
df_befolkningen = df_befolkningen[["intervall (år)", "value"]]

# Sum the three FullforingVGO categories (1a, 2a, 4b) per intervall (år)
df_befolkningen = df_befolkningen.groupby("intervall (år)")["value"].sum().reset_index()

# Legge til kategori for hele befolkningen
df_befolkningen["gruppe"] = "Hele befolkningen"


########## Merge datasett ##########

# Merge df_befolkningen and df_innvandrere
df_telemark = pd.concat([df_befolkningen, df_innvandrere], ignore_index=True)

# Create new column "År" containing the first year in the "intervall (år)" column
df_telemark["År"] = df_telemark["intervall (år)"].str.split("-").str[0]

# Remove column "intervall (år)"
df_telemark = df_telemark.drop(columns=["intervall (år)"])

# Rename columns to "Andel" and "Gruppe"
df_telemark = df_telemark.rename(columns={"value": "Andel", "gruppe": "Gruppe"})

# Reorder columns to "År", "Gruppe" and "Andel"
df_telemark = df_telemark[["År", "Gruppe", "Andel"]]

# Set the number of decimals in the "Andel" column to 1
df_telemark["Andel"] = df_telemark["Andel"].round(1)

# Pivotere til rett format (Everviz: År, Hele befolkningen, Innvandrere)
df_telemark_pivot = df_telemark.pivot_table(
    index=["År"], columns="Gruppe", values="Andel"
).reset_index()

# Flatten the MultiIndex columns
df_telemark_pivot.columns = [col for col in df_telemark_pivot.columns]

# Reorder columns to match Everviz legend order: År, Hele befolkningen, Innvandrere
df_telemark_pivot = df_telemark_pivot[["År", "Hele befolkningen", "Innvandrere"]]

# Sort by År
df_telemark_pivot = df_telemark_pivot.sort_values("År").reset_index(drop=True)

df_telemark_pivot.head()

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "fullført_vgo_innv_befolk.csv"
task_name = "Innvandrere - Fullfort VGO"
github_folder = "Data/09_Innvandrere og inkludering/Utdanningsnivå Telemark"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_telemark_pivot, file_name, github_folder, temp_folder, keepcsv=True)

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
