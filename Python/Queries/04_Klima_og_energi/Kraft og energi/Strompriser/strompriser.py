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
    """
    Fetches energy prices from ENTSO-E for a given date range,
    handling requests by splitting them into monthly chunks for reliability.
    """
    print(f"Fetching energy prices from {start_date.date()} to {end_date.date()}...")
    
    all_price_data = []
    current_start = start_date.replace(day=1)
    
    while current_start <= end_date:
        # Determine the end of the current month
        if current_start.month == 12:
            next_month_start = current_start.replace(year=current_start.year + 1, month=1, day=1)
        else:
            next_month_start = current_start.replace(month=current_start.month + 1, day=1)
        
        current_end = min(next_month_start - timedelta(days=1), end_date)
        
        # Ensure the loop doesn't go past the original start_date for the first iteration
        effective_start = max(current_start, start_date)

        print(f"  Fetching chunk from {effective_start.date()} to {current_end.date()}...")

        domain = "10YNO-2--------T"  # NO2 bidding zone (Southern Norway)
        params = {
            "documentType": "A44",
            "in_Domain": domain,
            "out_Domain": domain,
            "periodStart": effective_start.strftime("%Y%m%d") + "0000",
            "periodEnd": current_end.strftime("%Y%m%d") + "2300",
            "securityToken": api_key,
        }

        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error_msg = f"Error connecting to ENTSO-E API for chunk {effective_start.date()}-{current_end.date()}: {str(e)}"
            print(error_msg)
            error_messages.append(error_msg)
            current_start = next_month_start
            continue # Move to next chunk

        # Parse XML response
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
        root = ET.fromstring(response.content)
        
        timeseries = root.findall(".//ns:TimeSeries", ns)
        if not timeseries:
            print(f"  No TimeSeries found for chunk {effective_start.date()}-{current_end.date()}.")
            current_start = next_month_start
            continue

        for ts in timeseries:
            periods = ts.findall(".//ns:Period", ns)
            for period in periods:
                p_start_str = period.find(".//ns:start", ns).text
                p_start_dt = datetime.fromisoformat(p_start_str.replace('Z', '+00:00'))
                
                points = period.findall(".//ns:Point", ns)
                for point in points:
                    position = int(point.find("ns:position", ns).text)
                    price = float(point.find("ns:price.amount", ns).text)
                    time = p_start_dt + timedelta(hours=position - 1)
                    all_price_data.append({"time": time, "price_eur": price})
        
        current_start = next_month_start

    if not all_price_data:
        error_msg = "No price data found in the API response for the entire period."
        print(error_msg)
        error_messages.append(error_msg)
        return pd.DataFrame()

    print(f"Total price data points collected: {len(all_price_data)}")
    return pd.DataFrame(all_price_data)

def find_missing_date_ranges(df, start_date, end_date):
    """Finds gaps in the data and returns a list of (start, end) tuples for missing ranges."""
    if df is None or df.empty:
        return [(start_date, end_date)]
    
    # Ensure the 'time' column is timezone-naive datetime objects at date level
    existing_dates = pd.to_datetime(df['time']).dt.normalize().unique()
    
    # Create a full date range from the desired start to the effective end date
    full_range = pd.date_range(start=start_date, end=end_date, freq='D', tz=None)
    
    # Identify missing dates
    missing_dates = sorted(list(set(full_range) - set(existing_dates)))
    
    if not missing_dates:
        return []

    # Group missing dates into contiguous ranges
    gaps = []
    if missing_dates:
        start_gap = missing_dates[0]
        for i in range(1, len(missing_dates)):
            if (missing_dates[i] - missing_dates[i-1]).days > 1:
                gaps.append((start_gap, missing_dates[i-1]))
                start_gap = missing_dates[i]
        gaps.append((start_gap, missing_dates[-1])) # Add the last gap
    
    print(f"Found missing date ranges: {gaps}")
    return gaps

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

# 1. Setup paths and dates
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
file_name = "strompriser.csv"
desired_start_date = datetime(2021, 1, 1)
yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
all_processed_data = []

# 2. Download existing data
existing_df = download_github_file(f"{github_folder}/{file_name}")
if existing_df is not None and not existing_df.empty:
    existing_df["time"] = pd.to_datetime(existing_df["time"])
    all_processed_data.append(existing_df)

# 3. Find and fetch data for missing date ranges (gaps)
# Use yesterday as end date to ensure we have exchange rates available
missing_ranges = find_missing_date_ranges(existing_df, desired_start_date, yesterday)

if missing_ranges:
    print("Fetching data for missing periods...")
    for start_gap, end_gap in missing_ranges:
        print(f"-- Processing gap: {start_gap.date()} to {end_gap.date()} --")
        df_prices = fetch_energy_prices(start_gap, end_gap, API_KEY, error_messages)
        if df_prices.empty:
            continue
        
        df_rates = fetch_exchange_rates(start_gap, end_gap, existing_df, error_messages)
        if df_rates.empty:
            continue
        
        # Merge, process, and append gap data
        # Remove timezone from price data to allow merging with naive exchange rate dates
        df_prices["date_for_merge"] = pd.to_datetime(df_prices["time"]).dt.tz_localize(None).dt.floor('D')
        
        # Use a left merge to keep all energy price entries, even if exchange rate is missing
        df = pd.merge(df_prices, df_rates, how='left', left_on="date_for_merge", right_on="time", suffixes=('', '_exchange'))
        df = df.drop(columns=["time_exchange", "date_for_merge"])
        
        # Forward-fill missing exchange rates (for weekends/holidays)
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
        all_processed_data.append(daily_avg_new)

# 4. Combine all dataframes
if not all_processed_data:
    print("No data available to process.")
    daily_avg = pd.DataFrame()
else:
    daily_avg = pd.concat(all_processed_data, ignore_index=True)
    # Remove duplicates and sort
    daily_avg = daily_avg.drop_duplicates(subset=['time'])
    daily_avg = daily_avg.sort_values("time").reset_index(drop=True)

# 5. Save and upload to GitHub
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