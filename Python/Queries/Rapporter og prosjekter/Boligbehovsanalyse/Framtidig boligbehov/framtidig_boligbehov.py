import os
import re
import requests
import pandas as pd
from io import BytesIO

from Helper_scripts.github_functions import handle_output_data, GITHUB_TOKEN

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# GitHub settings
REPO = "evensrii/Telemark"
BRANCH = "main"


##################### Hjelpefunksjoner #####################

def list_github_folder(folder_path):
    """List files in a GitHub repository folder via the API."""
    url = f"https://api.github.com/repos/{REPO}/contents/{folder_path}?ref={BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to list folder {folder_path}: {response.status_code}")
        return []


def find_most_recent_csv(folder_path, file_prefix):
    """Find the most recent CSV file in a GitHub folder based on date in filename.
    Excludes files ending in '_siste.csv'."""
    files = list_github_folder(folder_path)

    # Filter to CSV files matching the prefix, excluding _siste.csv and lesmeg.txt
    csv_files = [
        f for f in files
        if f["name"].endswith(".csv")
        and f["name"].startswith(file_prefix)
        and "_siste" not in f["name"]
    ]

    if not csv_files:
        print(f"No CSV files found in {folder_path} with prefix '{file_prefix}'")
        return None

    # Sort by filename (contains ISO date, so alphabetical = chronological)
    csv_files.sort(key=lambda f: f["name"], reverse=True)
    most_recent = csv_files[0]

    print(f"Most recent file in {folder_path}: {most_recent['name']}")
    return most_recent


def download_csv_from_github(file_info):
    """Download a CSV file from GitHub and return as DataFrame."""
    url = file_info["download_url"]
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        df = pd.read_csv(BytesIO(response.content))
        return df
    else:
        print(f"Failed to download {file_info['name']}: {response.status_code}")
        return None


##################### Konfigurasjon #####################

folders_config = [
    {
        "github_data_folder": "Data/Boligbehovsanalyse_2026/Framskrevet_husholdninger_alder",
        "output_file": "framskrevet_husholdninger_alder_siste.csv",
        "task_name": "Boligbehov - Framskrevet husholdninger alder",
        "file_prefix": "Boligetterspørsel",
    },
    {
        "github_data_folder": "Data/Boligbehovsanalyse_2026/Framskrevet_husholdninger_størrelse",
        "output_file": "framskrevet_husholdninger_størrelse_siste.csv",
        "task_name": "Boligbehov - Framskrevet husholdninger stoerrelse",
        "file_prefix": "Boligetterspørsel",
    },
    {
        "github_data_folder": "Data/Boligbehovsanalyse_2026/Framtidige_boligbehov",
        "output_file": "framtidige_boligbehov_siste.csv",
        "task_name": "Boligbehov - Framtidige boligbehov",
        "file_prefix": "Boligbalanse",
    },
]


##################### Prosessering #####################

for config in folders_config:
    print(f"\n{'='*60}")
    print(f"Processing: {config['task_name']}")
    print(f"{'='*60}")

    # Find and download the most recent CSV file from GitHub
    most_recent_file = find_most_recent_csv(
        config["github_data_folder"], config["file_prefix"]
    )

    if most_recent_file is None:
        error_messages.append(f"No CSV file found for {config['task_name']}")
        continue

    df = download_csv_from_github(most_recent_file)

    if df is None:
        error_messages.append(f"Failed to download CSV for {config['task_name']}")
        continue

    print(f"Downloaded {len(df)} rows from {most_recent_file['name']}")
    print(f"Columns: {list(df.columns)}")


    ##################### Transformering #####################

    output_file = config["output_file"]

    if output_file == "framskrevet_husholdninger_alder_siste.csv":
        # Rename columns
        df = df.rename(columns={
            "municipality_code": "Kommunenummer",
            "municipality_name": "Kommune",
            "year": "År",
            "age_interval": "Aldersgruppe",
            "housing_demand": "Etterspørsel",
        })

        # Convert year to datetime format (YYYY-01-01)
        df["År"] = pd.to_datetime(df["År"], format="%Y").dt.strftime("%Y-%m-%d")

        # Rename age intervals: "INTERVAL_25_29" -> "25-29", "INTERVAL_80_INF" -> "80+"
        def rename_age_interval(val):
            val = val.replace("INTERVAL_", "")
            if val.endswith("_INF"):
                return val.replace("_INF", "+")
            return val.replace("_", "-")

        df["Aldersgruppe"] = df["Aldersgruppe"].apply(rename_age_interval)

    elif output_file == "framskrevet_husholdninger_størrelse_siste.csv":
        # Rename columns
        df = df.rename(columns={
            "municipality_code": "Kommunenummer",
            "municipality_name": "Kommune",
            "year": "År",
            "household_size": "Husholdningsstørrelse",
            "housing_demand": "Etterspørsel",
        })

        # Convert year to datetime format (YYYY-01-01)
        df["År"] = pd.to_datetime(df["År"], format="%Y").dt.strftime("%Y-%m-%d")

        # Rename household sizes: "SIZE_1" -> "1", "SIZE_6_INF" -> "6+"
        def rename_household_size(val):
            val = val.replace("SIZE_", "")
            if val == "6_INF":
                return "6+"
            return val

        df["Husholdningsstørrelse"] = df["Husholdningsstørrelse"].apply(rename_household_size)

    elif output_file == "framtidige_boligbehov_siste.csv":
        # Rename columns
        df = df.rename(columns={
            "municipality_code": "Kommunenummer",
            "municipality_name": "Kommune",
            "year": "År",
            "housing_demand": "Etterspørsel",
            "housing_supply": "Tilbud",
            "new_housing_demand": "Ny etterspørsel",
        })

        # Convert year to datetime format (YYYY-01-01)
        df["År"] = pd.to_datetime(df["År"], format="%Y").dt.strftime("%Y-%m-%d")

    # Ensure Kommunenummer is string (for github_functions compatibility)
    df["Kommunenummer"] = df["Kommunenummer"].astype(str)

    # Ensure numeric columns are float64 for github_functions compatibility
    numeric_cols = [c for c in df.columns if c not in ["Kommunenummer", "Kommune", "År", "Aldersgruppe", "Husholdningsstørrelse"]]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"\nTransformed data preview:")
    print(df.head(10).to_string())


    ##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

    file_name = output_file
    task_name = config["task_name"]
    github_folder = config["github_data_folder"]
    temp_folder = os.environ.get("TEMP_FOLDER")

    # Call the function and get the "New Data" status
    is_new_data = handle_output_data(
        df,
        file_name,
        github_folder,
        temp_folder,
        keepcsv=True,
    )

    # Write the "New Data" status to a unique log file
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


##################### Oppsummering #####################

if error_messages:
    print(f"\n{'='*60}")
    print("ERRORS during execution:")
    for msg in error_messages:
        print(f"  - {msg}")
else:
    print(f"\n{'='*60}")
    print("All folders processed successfully.")
