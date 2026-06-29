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

# SSB API v2 GET URL (tabell 13502 - Utvalgte nøkkeltall for barnehager)
# Spørring 1: Kommunedata (med codelist for aggregering av historiske kommunekoder)
GET_URL_KOMMUNE = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13502/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=KOSandel150000,KOSandel350000"
    "&valueCodes[Tid]=from(2020)"
    "&valueCodes[KOKkommuneregion0000]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036,3806,3807,3808,3812,3813,3814,3815,3816,3817,3818,3819,3820,3821,3822,3823,3824,3825"
    "&codelist[KOKkommuneregion0000]=agg_KOGkommuneregion000005401"
    "&outputValues[KOKkommuneregion0000]=aggregated"
)

# Spørring 2: Landet og Telemark (fylkesnivå, uten codelist)
GET_URL_AGGREGERT = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13502/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=KOSandel150000,KOSandel350000"
    "&valueCodes[Tid]=from(2020)"
    "&valueCodes[KOKkommuneregion0000]=EAK,EKA40"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df = fetch_data(
        url=GET_URL_KOMMUNE,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Dekningsgrad barnehage - kommuner (13502)",
        response_type="json",
    )
    df_agg = fetch_data(
        url=GET_URL_AGGREGERT,
        payload=None,
        error_messages=error_messages,
        query_name="Dekningsgrad barnehage - landet/Telemark (13502)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(df.head(20))
print(df.columns.tolist())
print(df["region"].unique())

################# Data cleaning #################

# Strip parenthetical suffixes like " (2020-2023)" from kommune names
# This merges the two periods (38XX, 40XX) into a single kommune name
df["region"] = df["region"].str.replace(r"\s*\(.*?\)", "", regex=True).str.strip()

# Format year as datetime (YYYY-01-01)
df["år"] = pd.to_datetime(df["år"], format="%Y").dt.strftime("%Y-%m-%d")

# Drop rows where value is NaN (periods where kommune didn't exist)
df = df.dropna(subset=["value"])

# Shorten statistikkvariabel labels
var_map = {
    "Andel barn 1-5 år i barnehage, i forhold til innbyggere 1-5 år (prosent)": "Andel 1-5 år",
    "Andel barn 3-5 år i barnehage, i forhold til innbyggere 3-5 år (prosent)": "Andel 3-5 år",
}
df["statistikkvariabel"] = df["statistikkvariabel"].replace(var_map)

# Process aggregated data (Landet + Telemark from separate query)
df_agg["år"] = pd.to_datetime(df_agg["år"], format="%Y").dt.strftime("%Y-%m-%d")
df_agg = df_agg.dropna(subset=["value"])
df_agg["statistikkvariabel"] = df_agg["statistikkvariabel"].replace(var_map)

df_landet = df_agg[df_agg["region"] == "Landet"].copy()
df_telemark = df_agg[df_agg["region"] == "Telemark"].copy()

print("\n--- Nasjonale data ---")
print(df_landet)
print("\n--- Telemark aggregert ---")
print(df_telemark)

# Add Kommunenummer based on kommune name
kommunenummer_map = {
    "Porsgrunn": "4001",
    "Skien": "4003",
    "Notodden": "4005",
    "Siljan": "4010",
    "Bamble": "4012",
    "Kragerø": "4014",
    "Drangedal": "4016",
    "Nome": "4018",
    "Midt-Telemark": "4020",
    "Seljord": "4022",
    "Hjartdal": "4024",
    "Tinn": "4026",
    "Kviteseid": "4028",
    "Nissedal": "4030",
    "Fyresdal": "4032",
    "Tokke": "4034",
    "Vinje": "4036",
}
df["Kommunenummer"] = df["region"].map(kommunenummer_map)

# Rename columns
df = df.rename(columns={
    "region": "Kommune",
    "år": "År",
    "statistikkvariabel": "Statistikkvariabel",
    "value": "Andel",
})

# Reorder columns
df = df[["Kommunenummer", "Kommune", "År", "Statistikkvariabel", "Andel"]]

# Sort by Kommunenummer, Statistikkvariabel and År
df = df.sort_values(["Kommunenummer", "Statistikkvariabel", "År"]).reset_index(drop=True)

print("\n--- Kommunedata ---")
print(df.head(30))
print(f"\nAntall rader: {len(df)}")
print(f"Kommuner: {df['Kommune'].unique()}")
print(f"År: {sorted(df['År'].unique())}")

# --- Summary sentence (latest year only) ---
latest_year = df["År"].max()
df_latest = df[df["År"] == latest_year]

# Telemark aggregate values (EAK40) for latest year
df_telemark_latest = df_telemark[df_telemark["år"] == latest_year]
telemark_1_5 = df_telemark_latest[df_telemark_latest["statistikkvariabel"] == "Andel 1-5 år"]["value"].values[0]
telemark_3_5 = df_telemark_latest[df_telemark_latest["statistikkvariabel"] == "Andel 3-5 år"]["value"].values[0]

# National values (EAK) for latest year
df_landet_latest = df_landet[df_landet["år"] == latest_year]
landet_1_5 = df_landet_latest[df_landet_latest["statistikkvariabel"] == "Andel 1-5 år"]["value"].values[0]
landet_3_5 = df_landet_latest[df_landet_latest["statistikkvariabel"] == "Andel 3-5 år"]["value"].values[0]

# Print andel per kommune for latest year (both indicators)
print(f"\n--- Andel per kommune ({latest_year}) ---")
df_latest_pivot = df_latest.pivot_table(index=["Kommunenummer", "Kommune"], columns="Statistikkvariabel", values="Andel").reset_index()
df_latest_pivot = df_latest_pivot.sort_values("Kommunenummer").reset_index(drop=True)
print(df_latest_pivot.to_string(index=False))

print(f"\n--- Oppsummering ({latest_year}) ---")
print(
    f"Blant Telemarkskommunene går i snitt {telemark_1_5:.1f} % av alle barn mellom ett og fem år "
    f"i barnehagen, mens andelen blant tre-femåringene er {telemark_3_5:.1f} %. "
    f"Landsgjennomsnittet ligger på henholdsvis {landet_1_5:.1f} % og {landet_3_5:.1f} %."
)

# Filter CSV output to only include "Andel 1-5 år"
df = df[df["Statistikkvariabel"] == "Andel 1-5 år"].drop(columns=["Statistikkvariabel"]).reset_index(drop=True)

# Divide andel by 100 (convert from percent to decimal), cap at 1
df["Andel"] = (df["Andel"] / 100).clip(upper=1)

print("\n--- CSV-output ---")
print(df.head(20))

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "dekningsgrad_barnehage.csv"
task_name = "Folkehelse - Dekningsgrad barnehage"
github_folder = "Data/08_Folkehelse og levekår/Oppvekst og levekår"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=["Andel"],
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
