import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta

# Define the base URL and common parameters
base_url = "https://web-api.tp.entsoe.eu/api"
security_token = (
    "5cf92fdd-a882-4158-a5a9-9b0b8e202786"  # Replace with your actual token
)

# Define the start and end dates
start_date = datetime(2021, 12, 1)
end_date = datetime(2024, 10, 31)

# Initialize an empty DataFrame to store all data
all_data = pd.DataFrame()

# Loop through each month from start_date to end_date
current_date = start_date
while current_date <= end_date:
    # Calculate the periodStart and periodEnd for the current month
    period_start = current_date.strftime("%Y%m%d%H%M")
    next_month = current_date.replace(day=28) + timedelta(days=4)  # Jump to next month
    period_end_date = next_month.replace(day=1) - timedelta(seconds=1)
    period_end = period_end_date.strftime("%Y%m%d%H%M")

    # Update parameters for the request
    params = {
        "securityToken": security_token,
        "documentType": "A44",  # Price document
        "periodStart": period_start,
        "periodEnd": period_end,
        "out_Domain": "10YNO-2--------T",  # NO2
        "in_Domain": "10YNO-2--------T",
    }

    # Make the GET request
    response = requests.get(base_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the XML response
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
        root = ET.fromstring(response.text)

        # Extract data into lists
        time_price_data = []
        for timeseries in root.findall("ns:TimeSeries", ns):
            for period in timeseries.findall("ns:Period", ns):
                # Get the start time and resolution
                start_time = datetime.fromisoformat(
                    period.find("ns:timeInterval/ns:start", ns).text[:-1]
                )

                # Parse points (hourly prices)
                for point in period.findall("ns:Point", ns):
                    position = int(point.find("ns:position", ns).text)
                    price = float(point.find("ns:price.amount", ns).text)

                    # Calculate the timestamp for this point
                    timestamp = start_time + timedelta(hours=position - 1)
                    time_price_data.append({"time": timestamp, "price": price})

        # Convert to DataFrame and append to all_data
        if time_price_data:  # Check if there is data
            month_df = pd.DataFrame(time_price_data)
            all_data = pd.concat([all_data, month_df], ignore_index=True)
        else:
            print(f"No data for period {period_start} to {period_end}.")
    else:
        print(
            f"Failed to fetch data for period {period_start} to {period_end}: {response.status_code}, {response.text}"
        )

    # Move to the next month
    current_date = next_month.replace(day=1)

# Save or display the final DataFrame
print(all_data)

# Save the data to a CSV file
all_data.to_csv("strompriser.csv", index=False)
