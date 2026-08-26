import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

################# Spørring #################

# SSB API v2 GET URL (tabell 12044 - Personer drept eller skadd i veitrafikkulykker)
# ContentsCode gir følgende statistikkvariabler: Ulykker (antall ulykker med personskade),
# Dod (dødsulykker), PersonerDrept (drepte), PersonerSkadd (skadde i alt),
# PersonerHardtSkadd (hardt skadde), PersonerLettSkadd (lettere skadde), Uoppgitt (uoppgitt skadegrad)
CONTENTS_CODE = "Ulykker,Dod,PersonerDrept,PersonerSkadd,PersonerHardtSkadd,PersonerLettSkadd,Uoppgitt"

YEARS = ",".join(str(year) for year in range(2000, 2026))

# Spørring 1: Telemark (fylkesnivå, med codelist for aggregering av historiske fylkesgrenser)
GET_URL_FYLKE = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/12044/data?lang=no"
    f"&valueCodes[ContentsCode]={CONTENTS_CODE}"
    f"&valueCodes[Tid]={YEARS}"
    "&valueCodes[Region]=F-40"
    "&codelist[Region]=agg_KommFylker"
    "&outputValues[Region]=aggregated"
)

# Spørring 2: Kommunene i Telemark (med codelist for aggregering av historiske kommunekoder)
GET_URL_KOMMUNE = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/12044/data?lang=no"
    f"&valueCodes[ContentsCode]={CONTENTS_CODE}"
    f"&valueCodes[Tid]={YEARS}"
    "&valueCodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&codelist[Region]=agg_KommSummer"
)

# Spørring 3: Hele landet
GET_URL_LANDET = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/12044/data?lang=no"
    f"&valueCodes[ContentsCode]={CONTENTS_CODE}"
    f"&valueCodes[Tid]={YEARS}"
    "&valueCodes[Region]=0"
    "&codelist[Region]=vs_Landet"
)

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_fylke = fetch_data(
        url=GET_URL_FYLKE,
        payload=None,  # None = GET request (SSB API v2)
        error_messages=error_messages,
        query_name="Trafikkulykker - Telemark (12044)",
        response_type="json",
    )
    df_kommune = fetch_data(
        url=GET_URL_KOMMUNE,
        payload=None,
        error_messages=error_messages,
        query_name="Trafikkulykker - kommuner (12044)",
        response_type="json",
    )
    df_landet = fetch_data(
        url=GET_URL_LANDET,
        payload=None,
        error_messages=error_messages,
        query_name="Trafikkulykker - hele landet (12044)",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(df_kommune.head(20))
print(df_kommune.columns.tolist())
print(df_kommune["region"].unique())

################# Data cleaning #################

# Kommunenummer per kommune i Telemark (samme oppsett som i dekningsgrad.py)
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
df_kommune["Kommunenummer"] = df_kommune["region"].map(kommunenummer_map)

df_fylke["Kommunenummer"] = "40"
df_landet["Kommunenummer"] = "00"

# Slå sammen kommune-, fylkes- og landsdata til én tabell
df = pd.concat([df_kommune, df_fylke, df_landet], ignore_index=True)

# Formater år som dato (YYYY-01-01), i tråd med resten av datasettene i repoet
df["år"] = pd.to_datetime(df["år"], format="%Y").dt.strftime("%Y-%m-%d")

# Fjern rader uten verdi (år hvor kommunen ikke fantes ennå)
df = df.dropna(subset=["value"])

# Rename columns
df = df.rename(columns={
    "region": "Kommune",
    "år": "År",
    "statistikkvariabel": "Statistikkvariabel",
    "value": "Antall",
})

# Reorder columns
df = df[["Kommunenummer", "Kommune", "År", "Statistikkvariabel", "Antall"]]

# Legg til aggregert kategori "Drepte eller hardt skadde" (DHS), summen av "Drepte" og "Hardt skadde"
dhs = (
    df[df["Statistikkvariabel"].isin(["Drepte", "Hardt skadde"])]
    .groupby(["Kommunenummer", "Kommune", "År"], as_index=False)["Antall"]
    .sum()
)
dhs["Statistikkvariabel"] = "Drepte eller hardt skadde"
df = pd.concat([df, dhs[["Kommunenummer", "Kommune", "År", "Statistikkvariabel", "Antall"]]], ignore_index=True)

# Sort by Kommunenummer, Statistikkvariabel og År
df = df.sort_values(["Kommunenummer", "Statistikkvariabel", "År"]).reset_index(drop=True)

print("\n--- CSV-output ---")
print(df.head(30))
print(f"\nAntall rader: {len(df)}")
print(f"Kommuner: {df['Kommune'].unique()}")
print(f"År: {sorted(df['År'].unique())}")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "trafikkulykker_ssb.csv"
task_name = "Folkehelse - Trafikkulykker"
github_folder = "Data/08_Folkehelse og levekår/Skader og ulykker/Trafikkulykker"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=["Antall"],
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
    print(f"New data detected in {file_name} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name}.")

print(f"New data status log written to {new_data_status_file}")
