import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query befolkningsendringer (table 01223) for all
#         Telemark municipalities, all quarters.
#         NB: The flow variables are cumulative year-to-date, so
#         the K4 value equals the whole year.
# ============================================================

GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/01223/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Folketallet1,Folketallet11,Fodte2,Dode3,Fodselsoverskudd4,Innvandring5,Utvandring6,Tilflytting7,Fraflytting8,Nettoflytting9,Folketilvekst10"
    "&valueCodes[Tid]=*"
    "&valueCodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&codelist[Region]=agg_KommSummer"
    "&outputValues[Region]=aggregated"
)

try:
    df_raw = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Befolkningsendringer",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Raw data: {len(df_raw)} rows")
print(df_raw.head(10))

# ============================================================
# Step 2: Keep only K4 (full year), pivot to one row per
#         kommune and year, and calculate net figures
# ============================================================

df = df_raw.copy()
df["value"] = pd.to_numeric(df["value"], errors="coerce")

df = df[df["kvartal"].str.endswith("K4")]

df_pivot = df.pivot_table(
    index=["region", "kvartal"], columns="statistikkvariabel", values="value", aggfunc="sum"
).reset_index()
df_pivot.columns.name = None

df_pivot["Ved utgangen av året"] = df_pivot["kvartal"].str[:4].astype(int)

df_final = pd.DataFrame(
    {
        "Ved utgangen av året": df_pivot["Ved utgangen av året"],
        "Kommune": df_pivot["region"],
        "År og kvartal": df_pivot["kvartal"],
        "Befolkning 1. januar": df_pivot["Befolkning 1. januar"],
        "Fødde": df_pivot["Fødde"],
        "Døde": df_pivot["Døde"],
        "Fødselsoverskudd": df_pivot["Fødselsoverskot"],
        "Innvandring": df_pivot["Innvandring"],
        "Utvandring": df_pivot["Utvandring"],
        "Nettoinnvandring": df_pivot["Innvandring"] - df_pivot["Utvandring"],
        "Innflytting, innalandsk": df_pivot["Innflytting, innalandsk"],
        "Utflytting, innalandsk": df_pivot["Utflytting, innalandsk"],
        "Netto innenlandsk innflytting": df_pivot["Innflytting, innalandsk"]
        - df_pivot["Utflytting, innalandsk"],
        "Nettoinnflytting inkl. inn- og utvandring": df_pivot[
            "Nettoinnflytting inkl. inn- og utvandring"
        ],
        "Folkevekst": df_pivot["Folkevekst"],
        "Befolkning ved utgangen av kvartalet": df_pivot[
            "Befolkning ved utgangen av kvartalet"
        ],
        # Date column for Power BI (1. januar of the year)
        "År": pd.to_datetime(df_pivot["Ved utgangen av året"].astype(str) + "-01-01").dt.strftime("%Y-%m-%d"),
    }
)

df_final = df_final.sort_values(["Kommune", "Ved utgangen av året"]).reset_index(drop=True)

print(df_final.head(20))
print(f"Years: {df_final['Ved utgangen av året'].min()}-{df_final['Ved utgangen av året'].max()}")

# ============================================================
# Step 3: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "befolkningsendringer.csv"
task_name = "Befolkning - Befolkningsendringer"
github_folder = "Data/01_Befolkning/Befolkningsutvikling"
temp_folder = os.environ.get("TEMP_FOLDER")

value_columns = [
    "Befolkning 1. januar",
    "Fødde",
    "Døde",
    "Fødselsoverskudd",
    "Innvandring",
    "Utvandring",
    "Nettoinnvandring",
    "Innflytting, innalandsk",
    "Utflytting, innalandsk",
    "Netto innenlandsk innflytting",
    "Nettoinnflytting inkl. inn- og utvandring",
    "Folkevekst",
    "Befolkning ved utgangen av kvartalet",
]

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_final, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
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
