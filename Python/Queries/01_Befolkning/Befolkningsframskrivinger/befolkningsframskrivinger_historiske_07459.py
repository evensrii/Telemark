import os
import pandas as pd

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query historical population data (table 07459)
#         using v2 GET API for three regions:
#         1) Telemark kommuner (from 2000)
#         2) Telemark fylke (from 2020)
#         3) Norge / Landet (from 2020)
# ============================================================

queries = [
    {
        "name": "Telemark kommuner",
        "url": (
            "https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"
            "&outputFormat=json-stat2"
            "&valuecodes[ContentsCode]=*"
            "&valuecodes[Tid]=from(2000)"
            "&valuecodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
            "&codelist[Region]=agg_KommSummer"
            "&valuecodes[Alder]=*"
            "&codelist[Alder]=agg_Funksjonell4"
            "&heading=ContentsCode,Tid"
            "&stub=Region,Alder"
        ),
    },
    {
        "name": "Telemark fylke",
        "url": (
            "https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"
            "&outputFormat=json-stat2"
            "&valuecodes[ContentsCode]=*"
            "&valuecodes[Tid]=from(2020)"
            "&valuecodes[Region]=F-40"
            "&codelist[Region]=agg_KommFylker"
            "&valuecodes[Alder]=*"
            "&codelist[Alder]=agg_Funksjonell4"
            "&heading=ContentsCode,Tid"
            "&stub=Region,Alder"
        ),
    },
    {
        "name": "Norge",
        "url": (
            "https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"
            "&outputFormat=json-stat2"
            "&valuecodes[ContentsCode]=*"
            "&valuecodes[Tid]=from(2020)"
            "&valuecodes[Region]=*"
            "&codelist[Region]=vs_Landet"
            "&valuecodes[Alder]=*"
            "&codelist[Alder]=agg_Funksjonell4"
            "&heading=ContentsCode,Tid"
            "&stub=Alder,Region"
        ),
    },
]

all_dfs = []

for q in queries:
    print(f"\nQuerying {q['name']}...")
    print(f"  URL: {q['url'][:120]}...")

    try:
        df_q = fetch_data(
            url=q["url"],
            payload=None,
            error_messages=error_messages,
            query_name=f"Historiske 07459 - {q['name']}",
            response_type="json",
        )
    except Exception as e:
        print(f"Error occurred: {e}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred during data fetching, stopping execution."
        )

    if df_q is not None and not df_q.empty:
        print(f"  -> Got {len(df_q)} rows")
        all_dfs.append(df_q)
    else:
        print(f"  -> No data returned for {q['name']}")

df = pd.concat(all_dfs, ignore_index=True)
print(f"\nCombined data: {df.shape[0]} rows")
print(f"  Columns: {list(df.columns)}")
print(f"  Regions: {sorted(df['region'].unique())}")
print(df.head())

### DATA CLEANING

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "befolkningsframskrivinger_historiske.csv"
task_name = "Befolkning - Befolkningsframskrivinger historiske_07459"
github_folder = "Data/01_Befolkning/Befolkningsframskrivinger"
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