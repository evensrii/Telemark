import os
import requests
import pandas as pd
import time
from datetime import datetime

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Hent data fra barnehagefakta.no API #################

# Telemark kommuner (fylkesnummer 40)
telemark_kommuner = [
    ("4001", "Porsgrunn"),
    ("4003", "Skien"),
    ("4005", "Notodden"),
    ("4010", "Siljan"),
    ("4012", "Bamble"),
    ("4014", "Kragerø"),
    ("4016", "Drangedal"),
    ("4018", "Nome"),
    ("4020", "Midt-Telemark"),
    ("4022", "Seljord"),
    ("4024", "Hjartdal"),
    ("4026", "Tinn"),
    ("4028", "Kviteseid"),
    ("4030", "Nissedal"),
    ("4032", "Fyresdal"),
    ("4034", "Tokke"),
    ("4036", "Vinje"),
]

base_url = "https://www.barnehagefakta.no/api/Kommune"

rows = []
nasjonalt = None

for knr, knavn in telemark_kommuner:
    try:
        response = requests.get(f"{base_url}/{knr}")
        response.raise_for_status()
        data = response.json()

        kommune_data = data.get("indikatorDataKommune", {})
        andel_oppfyller = kommune_data.get("andelBarnehagerSomOppfyllerPedagognormen")
        andel_ikke_oppfyller = kommune_data.get("andelBarnehagerSomIkkeOppfyllerPedagognormen")
        andel_med_dispensasjon = kommune_data.get("andelBarnehagerSomOppfyllerPedagognormenMedDispensasjon")
        antall_barn = kommune_data.get("antallBarn")

        rows.append({
            "Kommunenummer": knr,
            "Kommune": knavn,
            "Andel oppfyller pedagognormen": andel_oppfyller,
            "Andel oppfyller ikke pedagognormen": andel_ikke_oppfyller,
            "Andel oppfyller med dispensasjon": andel_med_dispensasjon,
            "Antall barn": antall_barn,
        })

        # Store national data from the first response (same for all)
        if nasjonalt is None:
            nasjonalt = data.get("indikatorDataNasjonalt", {})

        print(f"  {knavn} ({knr}): {andel_oppfyller} %")
        time.sleep(0.2)  # Be polite to the API

    except Exception as e:
        error_msg = f"Feil ved henting av data for {knavn} ({knr}): {e}"
        print(error_msg)
        error_messages.append(error_msg)

if not rows:
    print("Ingen data hentet. Avslutter.")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError("No data fetched from barnehagefakta.no API.")

df = pd.DataFrame(rows)

print(f"\nHentet data for {len(df)} kommuner.")
print(df)

################# Data cleaning #################

# Sort by Kommunenummer
df = df.sort_values("Kommunenummer").reset_index(drop=True)

# Add national row from API response
if nasjonalt:
    nasjonalt_row = {
        "Kommunenummer": "00",
        "Kommune": "Landet",
        "Andel oppfyller pedagognormen": nasjonalt.get("andelBarnehagerSomOppfyllerPedagognormen"),
        "Andel oppfyller ikke pedagognormen": nasjonalt.get("andelBarnehagerSomIkkeOppfyllerPedagognormen"),
        "Andel oppfyller med dispensasjon": nasjonalt.get("andelBarnehagerSomOppfyllerPedagognormenMedDispensasjon"),
        "Antall barn": nasjonalt.get("antallBarn"),
    }
    print(f"\n--- Nasjonale tall ---")
    print(f"Andel som oppfyller pedagognormen: {nasjonalt_row['Andel oppfyller pedagognormen']} %")
    print(f"Andel som ikke oppfyller: {nasjonalt_row['Andel oppfyller ikke pedagognormen']} %")
    print(f"Andel som oppfyller med dispensasjon: {nasjonalt_row['Andel oppfyller med dispensasjon']} %")

# Calculate Telemark aggregate (weighted average by antall barn)
telemark_total_barn = df["Antall barn"].sum()
telemark_fields = [
    "Andel oppfyller pedagognormen",
    "Andel oppfyller ikke pedagognormen",
    "Andel oppfyller med dispensasjon",
]
telemark_weighted = {}
for col in telemark_fields:
    telemark_weighted[col] = round((df[col] * df["Antall barn"]).sum() / telemark_total_barn, 1)

telemark_row = {
    "Kommunenummer": "40",
    "Kommune": "Telemark",
    "Andel oppfyller pedagognormen": telemark_weighted["Andel oppfyller pedagognormen"],
    "Andel oppfyller ikke pedagognormen": telemark_weighted["Andel oppfyller ikke pedagognormen"],
    "Andel oppfyller med dispensasjon": telemark_weighted["Andel oppfyller med dispensasjon"],
    "Antall barn": telemark_total_barn,
}

print(f"\n--- Telemark aggregert ---")
print(f"Andel som oppfyller pedagognormen: {telemark_row['Andel oppfyller pedagognormen']} %")
print(f"Antall barn totalt: {telemark_total_barn}")

# Add aggregate rows to DataFrame (Telemark and Landet at the bottom)
df = pd.concat([df, pd.DataFrame([telemark_row, nasjonalt_row])], ignore_index=True)

# Summary
print(f"\n--- Oppsummering ---")
print(f"Telemark: {telemark_row['Andel oppfyller pedagognormen']} %")
print(f"Landet: {nasjonalt_row['Andel oppfyller pedagognormen']} %")

# Kommuner under 50 %
df_kommuner = df[~df["Kommunenummer"].isin(["00", "40"])]
under_50 = df_kommuner[df_kommuner["Andel oppfyller pedagognormen"] < 50]
if len(under_50) > 0:
    print(f"\nKommuner under 50 %:")
    for _, row in under_50.iterrows():
        print(f"  {row['Kommune']}: {row['Andel oppfyller pedagognormen']} %")

# Kommuner med 100 %
full = df_kommuner[df_kommuner["Andel oppfyller pedagognormen"] == 100]
if len(full) > 0:
    print(f"\nKommuner med 100 %:")
    for _, row in full.iterrows():
        print(f"  {row['Kommune']}")

print("\n--- Ferdig DataFrame ---")
print(df.to_string(index=False))

# Prepare final output DataFrame
current_year = datetime.now().year
andel_col = f"Andel ({current_year})"

df_output = df[["Kommune", "Andel oppfyller pedagognormen"]].copy()
df_output["Andel oppfyller pedagognormen"] = df_output["Andel oppfyller pedagognormen"].round(0).astype(int)
df_output = df_output.rename(columns={"Andel oppfyller pedagognormen": andel_col})
df_output["Label"] = df_output["Kommune"]

print("\n--- Output DataFrame ---")
print(df_output.to_string(index=False))

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "pedagognorm_barnehager.csv"
task_name = "Folkehelse - Pedagognorm barnehager"
github_folder = "Data/08_Folkehelse og levekår/Oppvekst og levekår"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_output,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=[andel_col],
)

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
