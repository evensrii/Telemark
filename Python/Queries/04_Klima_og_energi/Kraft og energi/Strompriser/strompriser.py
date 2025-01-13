import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import io

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data, delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

# ENTSO-E API Key
API_KEY = "5cf92fdd-a882-4158-a5a9-9b0b8e202786"
BASE_URL = "https://web-api.tp.entsoe.eu/api"

def fetch_energy_prices(period_start, period_end):
    """
    Fetch energy prices from the ENTSO-E API.
    
    Args:
        period_start (str): Start date in YYYYMMDD format
        period_end (str): End date in YYYYMMDD format
        
    Returns:
        pandas.DataFrame: DataFrame with energy prices
    """
    # NO2 bidding zone (Southern Norway)
    domain = "10YNO-2--------T"
    
    params = {
        "documentType": "A44",
        "in_Domain": domain,
        "out_Domain": domain,
        "periodStart": period_start + "0000",
        "periodEnd": period_end + "0000",
        "securityToken": API_KEY,
    }
    
    try:
        print(f"Fetching energy prices from {period_start} to {period_end}")
        
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        
        # Define XML namespace
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
        
        # Parse XML response
        root = ET.fromstring(response.content)
        
        data = []
        timeseries = root.findall(".//ns:TimeSeries", ns)
        print(f"Found {len(timeseries)} TimeSeries elements")
        
        for ts in timeseries:
            try:
                periods = ts.findall(".//ns:Period", ns)
                for period in periods:
                    start = period.find(".//ns:start", ns).text
                    print(f"Processing period starting at {start}")
                    
                    points = period.findall(".//ns:Point", ns)
                    print(f"Found {len(points)} Point elements")
                    
                    for point in points:
                        position = int(point.find("ns:position", ns).text)
                        price = float(point.find("ns:price.amount", ns).text)
                        
                        # Convert time (remove Z and parse)
                        time = (datetime.fromisoformat(start[:-1]) + 
                               timedelta(hours=position-1))
                        
                        data.append({
                            "time": time,
                            "price_eur": price
                        })
            except Exception as e:
                print(f"Error processing TimeSeries element: {e}")
                continue
        
        print(f"Total data points collected: {len(data)}")
        
        if not data:
            raise ValueError("No price data found in the API response")
            
        df = pd.DataFrame(data)
        print("Energy prices data loaded successfully.")
        return df

    except requests.exceptions.RequestException as e:
        error_messages.append(f"Error connecting to ENTSO-E API: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred during data fetching, stopping execution."
        )
    except (ET.ParseError, ValueError, AttributeError) as e:
        error_messages.append(f"Error processing ENTSO-E API response: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred while processing the data, stopping execution."
        )
    except Exception as e:
        error_messages.append(f"Unexpected error in fetch_energy_prices: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "An unexpected error occurred, stopping execution."
        )

def fetch_exchange_rates(start_date):
    """
    Fetch EUR to NOK exchange rates.
    
    Args:
        start_date (datetime): Start date for exchange rates
        
    Returns:
        pandas.DataFrame: DataFrame with exchange rates
    """
    try:
        # Format date for Norges Bank API
        date_str = start_date.strftime("%Y-%m-%d")
        
        # Fetch exchange rates from Norges Bank
        url = f"https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP?format=csv&startPeriod={date_str}"
        df_rates = pd.read_csv(url, sep=";")
        
        if df_rates.empty:
            raise ValueError("No exchange rate data found in the API response")
        
        # Clean and prepare exchange rates data
        df_rates["TIME_PERIOD"] = pd.to_datetime(df_rates["TIME_PERIOD"])
        df_rates = df_rates.rename(columns={
            "TIME_PERIOD": "time",
            "OBS_VALUE": "eur_nok_rate"
        })
        
        print("Exchange rates data loaded successfully.")
        return df_rates[["time", "eur_nok_rate"]]
        
    except requests.exceptions.RequestException as e:
        error_messages.append(f"Error connecting to Norges Bank API: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred during data fetching, stopping execution."
        )
    except ValueError as e:
        error_messages.append(f"Error processing exchange rate data: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "A critical error occurred while processing the data, stopping execution."
        )
    except Exception as e:
        error_messages.append(f"Unexpected error in fetch_exchange_rates: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError(
            "An unexpected error occurred, stopping execution."
        )

def get_latest_date_from_github(file_path):
    """
    Get the latest date from existing data in GitHub.
    
    Args:
        file_path (str): Path to the file in GitHub
        
    Returns:
        datetime: Latest date in the data, or None if no data exists
    """
    try:
        content = download_github_file(file_path)
        if content is not None and content.strip():  # Check if content exists and is not empty
            df = pd.read_csv(io.StringIO(content))
            if not df.empty:
                df["time"] = pd.to_datetime(df["time"])
                return df["time"].max()
        return None
    except Exception as e:
        error_messages.append(f"Error getting latest date from GitHub: {str(e)}")
        notify_errors(error_messages, script_name=script_name)
        return None

# Main execution
try:
    # Get the latest date from GitHub data
    github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
    file_name = "strompriser.csv"
    latest_date = get_latest_date_from_github(f"{github_folder}/{file_name}")
    
    # Set date range for new data
    if latest_date is not None:
        start_date = latest_date + timedelta(days=1)
    else:
        start_date = datetime.now() - timedelta(days=30)  # Default to last 30 days
    
    end_date = datetime.now()
    
    # Fetch energy prices
    df_prices = fetch_energy_prices(
        start_date.strftime("%Y%m%d"),
        end_date.strftime("%Y%m%d")
    )
    
    # Fetch exchange rates
    df_rates = fetch_exchange_rates(start_date)
    
    # Merge prices with exchange rates
    df = pd.merge(df_prices, df_rates, on="time", how="left")
    
    # Fill missing exchange rates with the last known value
    df["eur_nok_rate"] = df["eur_nok_rate"].fillna(method="ffill")
    
    # Calculate NOK prices
    df["price_nok"] = df["price_eur"] * df["eur_nok_rate"]
    
    # Calculate daily averages
    df["date"] = pd.to_datetime(df["time"]).dt.date
    daily_avg = df.groupby("date").agg({
        "price_eur": "mean",
        "eur_nok_rate": "mean",
        "price_nok": "mean"
    }).reset_index()
    
    # Rename columns to match existing format
    daily_avg.columns = ["time", "EUR/MWh", "kurs", "NOK/MWh"]
    
    # Add NOK/KWh column
    daily_avg["NOK/KWh"] = (daily_avg["NOK/MWh"] / 1000).apply(lambda x: format(x, ".2f"))
    
    # Convert date to datetime string format - date only
    daily_avg["time"] = pd.to_datetime(daily_avg["time"]).dt.strftime("%Y-%m-%d")
    
    # Sort by date
    daily_avg = daily_avg.sort_values("time")
    
    ##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################
    
    file_name = "strompriser.csv"
    task_name = "Klima og energi - Strompriser"
    github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
    temp_folder = os.environ.get("TEMP_FOLDER")
    
    # Call the function and get the "New Data" status
    is_new_data = handle_output_data(daily_avg, file_name, github_folder, temp_folder, keepcsv=True)
    
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
        print("No new data to push to GitHub.")

except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during execution, stopping script."
    )
