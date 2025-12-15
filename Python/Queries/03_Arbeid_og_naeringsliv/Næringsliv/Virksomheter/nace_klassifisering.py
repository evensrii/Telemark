import os
import pandas as pd

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

script_name = os.path.basename(__file__)

error_messages = []


def download_standard_for_naeringsgruppering(
    classification_id: int = 6,
    from_date: str = "2009-01-01",
    language: str = "nb",
    csv_separator: str = ";",
) -> pd.DataFrame:
    base_url = f"https://data.ssb.no/api/klass/v1/classifications/{classification_id}/codes.csv"
    url = (
        f"{base_url}?from={from_date}"
        f"&language={language}"
        f"&csvSeparator={csv_separator}"
    )

    df = fetch_data(
        url=url,
        payload=None,
        error_messages=error_messages,
        query_name="SSB Klass - Standard for næringsgruppering",
        response_type="csv",
        delimiter=csv_separator,
        encoding="ISO-8859-1",
    )

    df = df.copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace("nan", "")

    return df


df_standard_for_naeringsgruppering = download_standard_for_naeringsgruppering()

file_name = "standard_for_naeringsgruppering.csv"
task_name = "NACE - Standard for naeringsgruppering"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter/Nace-tabell"
temp_folder = os.environ.get("TEMP_FOLDER")

is_new_data = handle_output_data(
    df_standard_for_naeringsgruppering,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
)

log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
