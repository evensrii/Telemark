import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import base64
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
DATA_FILE_PATH = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/transparencyplatform/strompriser.csv"

# ENTSO-E API Key
API_KEY = "5cf92fdd-a882-4158-a5a9-9b0b8e202786"
BASE_URL = "https://web-api.tp.entsoe.eu/api"


def download_github_file(file_path):
    """Download an existing file from GitHub."""
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


def upload_github_file(file_path, content, message="Updating data"):
    """Upload a new or updated file to GitHub."""
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


def get_latest_date_from_github(file_path):
    """Determine the latest date in the existing GitHub file."""
    file_content = download_github_file(file_path)
    if file_content is None:
        # No existing file; default to December 1, 2021
        return datetime(2021, 12, 1, 0, 0)

    # Read CSV and find the latest date
    df = pd.read_csv(io.StringIO(file_content), parse_dates=["time"])
    return df["time"].max() + timedelta(
        hours=1
    )  # Add one hour to start after the latest entry


def fetch_energy_prices(period_start, period_end):
    """Fetch energy prices from the ENTSO-E API."""
    params = {
        "securityToken": API_KEY,
        "documentType": "A44",
        "periodStart": period_start,
        "periodEnd": period_end,
        "out_Domain": "10YNO-2--------T",  # NO2
        "in_Domain": "10YNO-2--------T",
    }

    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
        root = ET.fromstring(response.text)

        # Parse data
        time_price_data = []
        for timeseries in root.findall("ns:TimeSeries", ns):
            for period in timeseries.findall("ns:Period", ns):
                start_time = datetime.fromisoformat(
                    period.find("ns:timeInterval/ns:start", ns).text[:-1]
                )
                for point in period.findall("ns:Point", ns):
                    position = int(point.find("ns:position", ns).text)
                    price = float(point.find("ns:price.amount", ns).text)
                    timestamp = start_time + timedelta(hours=position - 1)
                    time_price_data.append({"time": timestamp, "price": price})

        return pd.DataFrame(time_price_data)
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")
        return pd.DataFrame()


def main():
    # Determine the latest date in GitHub file
    latest_date = get_latest_date_from_github(DATA_FILE_PATH)
    print(f"Latest date found: {latest_date}")

    # Calculate the period end (today's midnight)
    period_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_period_end = period_end.strftime("%Y%m%d%H%M")

    # Format the period start
    formatted_period_start = latest_date.strftime("%Y%m%d%H%M")

    print(f"Querying data from {formatted_period_start} to {formatted_period_end}...")

    # Fetch data from the API
    new_data = fetch_energy_prices(formatted_period_start, formatted_period_end)

    if not new_data.empty:
        # Download existing data from GitHub
        existing_data_content = download_github_file(DATA_FILE_PATH)
        if existing_data_content:
            # Read the existing data using io.StringIO
            existing_data = pd.read_csv(io.StringIO(existing_data_content))

            # Ensure the 'time' column is in datetime format
            existing_data["time"] = pd.to_datetime(existing_data["time"])
            new_data["time"] = pd.to_datetime(new_data["time"])

            # Combine, remove duplicates, and sort by 'time'
            combined_data = (
                pd.concat([existing_data, new_data])
                .drop_duplicates()
                .sort_values("time")
            )
        else:
            # If no existing data, new_data becomes the combined dataset
            new_data["time"] = pd.to_datetime(
                new_data["time"]
            )  # Ensure 'time' is datetime
            combined_data = new_data

        # Upload updated data back to GitHub
        upload_github_file(
            DATA_FILE_PATH,
            combined_data.to_csv(index=False),
            message="Updated energy price data",
        )
    else:
        print("No new data fetched.")


if __name__ == "__main__":
    main()
