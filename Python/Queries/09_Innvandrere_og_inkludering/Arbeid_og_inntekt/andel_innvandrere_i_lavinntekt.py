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

# Telemark, siste år.

## Har kun andeler, og kun for Vestfold og Telemark samlet. Kan dermed ikke regne ut for Telemark samlet basert på absolutte tall.
## Utarbeider scriptet slik at det i framtiden kan hente ut tall for kun Telemark.


###### Andel innvandrere i lavinntekt ######

# Finner URL vha. "Inspiser side" og fane "Network" (F12)
url = "https://app-simapi-prod.azurewebsites.net/download_csv/f/vedvarende_lavinntekt"

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=url,
        error_messages=error_messages,
        query_name="Lavinntekt",
        payload=None,
        response_type="csv",
        delimiter=";",
        encoding="ISO-8859-1",
    )

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

# df.info()
df.head()

# Set data types
df["Fylkesnummer"] = df["Fylkesnummer"].astype(str).str.zfill(2)
df["År"] = pd.to_datetime(df["År"], format="%Y")

# Fange opp VTFK, og etterhvert også TFK-tallene.
df = df[df["Fylkesnummer"].isin(["38", "40"])]
# dtale.show(df, open_browser=True)

# Get the most recent year in the dataset
most_recent_year = df["År"].max()

# Add the year ({most_recent_year.year}) to the "Andel" column name
andel_column_name = f"Andel ({most_recent_year.year})"
df = df.rename(columns={"Antall": andel_column_name})

# Filter rows based on the most recent year
df = df[df["År"] == most_recent_year]

# Filter based on other criteria
df = df[df["Bakgrunn"] != "Hele befolkningen"]
df = df[~((df["Bakgrunn"] == "Innvandrere") & (df["Verdensregion"] == "Alle"))]

# Replace values
df["Verdensregion"] = df["Verdensregion"].replace(
    {
        "Øst-Europa utenom EU, Asia (inkl.Tyrkia), Afrika, Sør- og Mellom-Amerika og Oseania utenom Australia og New Zealand": "Gruppe 2-land (Asia, Afrika, Latin-Amerika osv.)",
        "EU/EFTA-land, Nord-Amerika, Australia og New Zealand": "Gruppe 1-land (EU, Storbritannia, USA osv.)",
        "Alle": "Befolkningen unntatt innvandrere",
    }
)

# Convert "Andel" to numeric and round to nearest integer
df[andel_column_name] = pd.to_numeric(
    df[andel_column_name].str.replace(",", "."), errors="coerce"
)
df[andel_column_name] = df[andel_column_name].round(0).astype(int)

# Rename columns
df = df.rename(columns={"Verdensregion": "Gruppe"})

# Remove columns
df = df.drop(columns=["Bakgrunn", "Fylkesnummer", "Fylke", "År", "Enhet"])

# Reset index
df = df.reset_index(drop=True)


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_innvandrere_i_lavinntekt.csv"
task_name = "Innvandrere - Lavinntekt"
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