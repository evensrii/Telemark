import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from dotenv import load_dotenv
import io

# Load GitHub token from the .env file
load_dotenv("../../../token.env", override=True)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    raise ValueError(
        "GitHub token not found. Please ensure that the token is set in '../../../token.env'"
    )

# GitHub Repository information
REPO = "evensrii/Telemark"
BRANCH = "main"
GITHUB_API_URL = "https://api.github.com"
DATA_PATH = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub/"

# Function to download a file from GitHub
def download_github_file(file_path):
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{file_path}?ref={BRANCH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.text
    elif response.status_code == 404:
        print(f"File not found: {file_path}")
        return None
    else:
        print(
            f"Failed to download file: {file_path}, Status Code: {response.status_code}"
        )
        return None

# Function to upload a file to GitHub
def upload_github_file(file_path, content, message="Updating data"):
    url = f"{GITHUB_API_URL}/repos/{REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.get(url, headers=headers)
    sha = response.json().get("sha") if response.status_code == 200 else None

    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha

    response = requests.put(url, json=payload, headers=headers)

    if response.status_code in [201, 200]:
        print(f"File uploaded successfully: {file_path}")
    else:
        print(f"Failed to upload file: {response.json()}")

# Function to query the Elhub API for a specific date
def query_elhub(date):
    url = f"https://api.elhub.no/energy-data/v0/municipalities?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR&startDate={date}"
    response = requests.get(url)
    data = response.json()

    df_data = pd.json_normalize(data["data"])
    df_data_expanded = df_data.explode("attributes.consumptionPerGroupMunicipalityHour")
    df_consumption = pd.json_normalize(
        df_data_expanded["attributes.consumptionPerGroupMunicipalityHour"]
    )

    df_data_expanded = df_data_expanded.reset_index(drop=True)
    df_consumption = df_consumption.reset_index(drop=True)

    df_combined = pd.concat(
        [
            df_data_expanded[["attri
