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
DATA_FILE_PATH = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e/strompriser.csv"

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
        return datetime(2021, 12, 1, 0, 0)

    df = pd.read_csv(io.StringIO(file_content), parse_dates=["time"])
    return df["time"].max() + timedelta(hours=1)


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


def fetch_exchange_rates(latest_date):
    """Fetch EUR to NOK exchange rates from the latest date."""
    base_url = "https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP"
    start_date = latest_date.strftime(
        "%Y-%m-%d"
    )  # Use the latest date as the start date
    end_date = datetime.now().strftime("%Y-%m-%d")  # Use today's date as the end date

    params = {
        "startPeriod": start_date,
        "endPeriod": end_date,
        "format": "csv",
        "bom": "include",
        "locale": "no",
    }
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        exchange_data = pd.read_csv(
            io.StringIO(response.text), encoding="utf-8", sep=";"
        )
        exchange_data = exchange_data[["TIME_PERIOD", "OBS_VALUE"]]
        exchange_data.columns = ["time", "kurs"]
        exchange_data["time"] = pd.to_datetime(exchange_data["time"])
        exchange_data["kurs"] = (
            exchange_data["kurs"].str.replace(",", ".").astype(float)
        )
        return exchange_data
    else:
        print(f"Failed to fetch exchange rates: {response.status_code}")
        return pd.DataFrame()


def main():
    # Determine the latest date in GitHub file
    existing_data_content = download_github_file(DATA_FILE_PATH)

    if existing_data_content:
        # Load the existing data from GitHub
        existing_data = pd.read_csv(io.StringIO(existing_data_content))
        existing_data["time"] = pd.to_datetime(existing_data["time"])
        latest_date = existing_data["time"].max()
        print(f"Latest date found in GitHub file: {latest_date}")
    else:
        # If no existing data, initialize empty DataFrame and start from a default date
        existing_data = pd.DataFrame(
            columns=["time", "EUR/MWh", "kurs", "NOK/MWh", "NOK/KWh"]
        )
        latest_date = datetime(2021, 12, 1)  # Default start date
        print(f"No existing data found. Starting from {latest_date}.")

    ### Exchange Rates
    # Fetch exchange rates from the latest date onward
    exchange_data = fetch_exchange_rates(latest_date)
    if exchange_data.empty:
        print("No new exchange rates fetched.")
        return  # Exit if no exchange rate data is available

    ### Energy Prices
    # Calculate the query start date as "latest date + 1"
    query_start_date = latest_date + timedelta(days=1)
    formatted_period_start = query_start_date.strftime("%Y%m%d%H%M")
    period_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_period_end = period_end.strftime("%Y%m%d%H%M")

    print(
        f"Querying energy prices from {formatted_period_start} to {formatted_period_end}..."
    )
    new_data = fetch_energy_prices(formatted_period_start, formatted_period_end)
    if new_data.empty:
        print("No new energy prices fetched.")
        return  # Exit if no energy price data is available

    ### Calculate Daily Average Prices
    # Calculate the average price per day
    new_data["date"] = new_data["time"].dt.date
    daily_avg_prices = new_data.groupby("date")["price"].mean().reset_index()
    daily_avg_prices.columns = ["time", "EUR/MWh"]  # Rename columns

    ### Merge Daily Average Prices with Exchange Rates
    # Convert the "time" column to datetime format
    daily_avg_prices["time"] = pd.to_datetime(daily_avg_prices["time"])
    exchange_data["time"] = pd.to_datetime(exchange_data["time"])

    # Perform a left join to keep all rows from daily_avg_prices
    merged_data = pd.merge(
        daily_avg_prices,
        exchange_data,
        on="time",  # Merge on the 'time' column
        how="left",  # Left join to keep all rows from daily_avg_prices
    )

    # Fill NaN values by propagating the previous value
    merged_data["kurs"] = merged_data["kurs"].fillna(method="ffill")

    ### Calculate Energy Prices in NOK
    # Calculate the price in NOK per MWh
    merged_data["NOK/MWh"] = merged_data["EUR/MWh"] * merged_data["kurs"]

    # Create a column named "NOK/KWh" by dividing "NOK/MWh" by 1000
    merged_data["NOK/KWh"] = merged_data["NOK/MWh"] / 1000

    ### Finalize Columns
    # Select relevant columns and ensure proper naming
    merged_data = merged_data[["time", "EUR/MWh", "kurs", "NOK/MWh", "NOK/KWh"]]
    merged_data.columns = ["time", "EUR/MWh", "kurs", "NOK/MWh", "NOK/KWh"]

    ### Combine with Existing Data
    # Append the new data to the existing data
    combined_data = (
        pd.concat([existing_data, merged_data])
        .drop_duplicates(subset="time")
        .sort_values("time")
    )

    ### Upload Combined Data to GitHub
    upload_github_file(
        DATA_FILE_PATH,
        combined_data.to_csv(index=False),
        message="Appended new energy price and exchange rate data",
    )
    print("Updated data successfully uploaded to GitHub.")


if __name__ == "__main__":
    main()
