import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from dotenv import load_dotenv
import io

# Dette er en test

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

# Define the starting point, price area, and API endpoint
start_date = datetime(
    2021, 12, 1
)  # Adjust this to the earliest known date or API limit
end_date = datetime.now()
price_area = "NO2"  # Adjust as needed, e.g., "NO2", "NO3", etc.
base_url = "https://www.hvakosterstrommen.no/api/v1/prices/{year}/{month}-{day}_{price_area}.json"

all_data = []
current_date = start_date

while current_date <= end_date:
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    url = base_url.format(year=year, month=month, day=day, price_area=price_area)

    try:
        response = requests.get(url)
        if response.status_code == 200:
            try:
                json_data = response.json()
                if json_data:
                    df = pd.DataFrame(json_data)
                    df["price_area"] = price_area
                    all_data.append(df)
                else:
                    print(f"No data available for {current_date.strftime('%Y-%m-%d')}")
            except ValueError:
                print(
                    f"Invalid JSON for {current_date.strftime('%Y-%m-%d')}. Skipping..."
                )
        else:
            print(
                f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')}: {response.status_code}"
            )
    except Exception as e:
        print(f"Error fetching data for {current_date.strftime('%Y-%m-%d')}: {e}")

    current_date += timedelta(days=1)

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv("electricity_prices.csv", index=False)
    print("Data saved to 'electricity_prices.csv'.")
else:
    print("No data was retrieved.")
