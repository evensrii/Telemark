import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import io

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import download_github_file, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# ENTSO-E API Key
API_KEY = "5cf92fdd-a882-4158-a5a9-9b0b8e202786"
BASE_URL = "https://web-api.tp.entsoe.eu/api"

def fetch_energy_prices(start_date, end_date, api_key, error_messages):
    """Fetches energy prices from ENTSO-E for a given date range."""
    print("Fetching energy prices from ENTSO-E...")
    
    domain = "10YNO-2--------T"  # NO2 bidding zone (Southern Norway)
    params = {
        "documentType": "A44",
        "in_Domain": domain,
        "out_Domain": domain,
        "periodStart": start_date.strftime("%Y%m%d") + "0000",
        "periodEnd": end_date.strftime("%Y%m%d") + "0000",
        "securityToken": api_key,
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        print("Successfully received response from ENTSO-E")
    except requests.exceptions.RequestException as e:
        error_msg = f"Error connecting to ENTSO-E API: {str(e)}"
        print(error_msg)
        error_messages.append(error_msg)
        return pd.DataFrame() # Return empty DataFrame on failure

    print("Parsing XML response...")
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
    root = ET.fromstring(response.content)
    price_data = []

    timeseries = root.findall(".//ns:TimeSeries", ns)
    print(f"Found {len(timeseries)} TimeSeries elements")

    for ts in timeseries:
        periods = ts.findall(".//ns:Period", ns)
        for period in periods:
            start = period.find(".//ns:start", ns).text
            points = period.findall(".//ns:Point", ns)
            for point in points:
                position = int(point.find("ns:position", ns).text)
                price = float(point.find("ns:price.amount", ns).text)
                time = (datetime.fromisoformat(start[:-1]) + timedelta(hours=position-1))
                price_data.append({"time": time, "price_eur": price})

    if not price_data:
        error_msg = "No price data found in the API response"
        print(error_msg)
        error_messages.append(error_msg)
        return pd.DataFrame()

    print(f"Total price data points collected: {len(price_data)}")
    return pd.DataFrame(price_data)

def fetch_exchange_rates(start_date, end_date, existing_df, error_messages):
    """Fetches EUR to NOK exchange rates from Norges Bank API."""
    print("Fetching exchange rates...")
    try:
        base_url = "https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP"
        params = {
            "startPeriod": start_date.strftime("%Y-%m-%d"),
            "endPeriod": end_date.strftime("%Y-%m-%d"),
            "format": "csv", "bom": "include", "locale": "no",
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        df_rates = pd.read_csv(io.StringIO(response.text), sep=";")
        
        if df_rates.empty:
            raise Exception("Exchange rate dataframe is empty")

        df_rates["TIME_PERIOD"] = pd.to_datetime(df_rates["TIME_PERIOD"])
        df_rates = df_rates.rename(columns={"TIME_PERIOD": "time", "OBS_VALUE": "eur_nok_rate"})
        df_rates = df_rates[["time", "eur_nok_rate"]]
        if df_rates["eur_nok_rate"].dtype == object:
            df_rates["eur_nok_rate"] = df_rates["eur_nok_rate"].str.replace(',', '.').astype(float)
        
        print(f"Successfully fetched {len(df_rates)} exchange rate records")
        return df_rates
            
    except Exception as e:
        print(f"Could not fetch new exchange rates: {str(e)}, trying fallback...")
        if not existing_df.empty and 'kurs' in existing_df.columns:
            latest_rate = existing_df.iloc[-1]["kurs"]
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            df_rates = pd.DataFrame({'time': dates, 'eur_nok_rate': [latest_rate] * len(dates)})
            print(f"Using latest available rate from existing data: {latest_rate}")
            return df_rates
        else:
            error_msg = "No existing data to get latest exchange rate from."
            print(error_msg)
            error_messages.append(error_msg)
            return pd.DataFrame()

# --- Main script execution ---
print("Script starting...")

# 1. Setup paths
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
file_name = "strompriser.csv"
print(f"Working with file: {github_folder}/{file_name}")

# 2. Download and process existing data
existing_df = download_github_file(f"{github_folder}/{file_name}")
latest_date = datetime(2024, 1, 1)  # Default start date
if existing_df is not None and not existing_df.empty:
    try:
        existing_df["time"] = pd.to_datetime(existing_df["time"])
        latest_date = existing_df["time"].max()
        print(f"Latest date in existing data: {latest_date.date()}")
    except Exception as e:
        print(f"Error processing dates from existing data: {e}")
        existing_df = pd.DataFrame()
else:
    print("No existing data found.")
    existing_df = pd.DataFrame()

# 3. Set up date range for new data
start_date = latest_date + timedelta(days=1)
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
end_date = yesterday

if start_date.date() > end_date.date():
    print("All prices are already up to date.")
    daily_avg = existing_df.copy()
else:
    print(f"Will fetch data from {start_date.date()} to {end_date.date()}")
    
    # 4. Fetch new data
    df_prices = fetch_energy_prices(start_date, end_date, API_KEY, error_messages)
    
    if df_prices.empty:
        print("No new price data to process.")
        daily_avg = existing_df.copy()
    else:
        # Filter out dates that are already in the existing dataset
        if not existing_df.empty:
            existing_dates = pd.to_datetime(existing_df["time"]).dt.date
            df_prices["date"] = pd.to_datetime(df_prices["time"]).dt.date
            df_prices = df_prices[~df_prices["date"].isin(existing_dates)]
            df_prices = df_prices.drop(columns=["date"])

        if df_prices.empty:
            print("No new price data after filtering for existing dates.")
            daily_avg = existing_df.copy()
        else:
            df_rates = fetch_exchange_rates(start_date, end_date, existing_df, error_messages)
            
            if df_rates.empty:
                print("Could not retrieve exchange rates. Aborting further processing.")
                notify_errors(error_messages, script_name)
                daily_avg = existing_df.copy()
            else:
                # 5. Merge and process data
                df_prices["date_for_merge"] = pd.to_datetime(df_prices["time"]).dt.floor('D')
                df = pd.merge(df_prices, df_rates, left_on="date_for_merge", right_on="time", suffixes=('', '_exchange'))
                df = df.drop(columns=["time_exchange", "date_for_merge"])
                df["eur_nok_rate"] = df["eur_nok_rate"].fillna(method="ffill")
                df["price_nok"] = df["price_eur"] * df["eur_nok_rate"]
                df["date"] = pd.to_datetime(df["time"]).dt.date

                daily_avg_new = df.groupby("date").agg(
                    price_eur=("price_eur", "mean"),
                    eur_nok_rate=("eur_nok_rate", "mean"),
                    price_nok=("price_nok", "mean")
                ).reset_index()

                daily_avg_new = daily_avg_new.rename(columns={
                    "date": "time", "price_eur": "EUR/MWh",
                    "eur_nok_rate": "kurs", "price_nok": "NOK/MWh"
                })
                daily_avg_new["NOK/KWh"] = daily_avg_new["NOK/MWh"] / 1000
                daily_avg_new["time"] = pd.to_datetime(daily_avg_new["time"])

                # 6. Combine with existing data
                daily_avg = pd.concat([existing_df, daily_avg_new], ignore_index=True)
                daily_avg = daily_avg.sort_values("time").reset_index(drop=True)

# 7. Save and upload to GitHub
task_name = "Klima og energi - Strompriser"
temp_folder = os.environ.get("TEMP_FOLDER")
is_new_data = handle_output_data(daily_avg, file_name, github_folder, temp_folder, keepcsv=True)

# 8. Log status
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
print("Script completed successfully!")