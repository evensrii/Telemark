import requests
import pandas as pd

def get_elhub_data():
    """
    Fetches energy consumption data from the Elhub API and returns it as a pandas DataFrame.
    """
    # Define the API endpoint
    url = "https://api.elhub.no/energy-data/v0/municipalities?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR&startDate=2025-08-08&endDate=2025-08-09"

    try:
        # Make the GET request
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        # Load the JSON response
        data = response.json()

        if not data.get("data"):
            print("No data available in the API response.")
            return pd.DataFrame()

        # Normalize the main data structure
        df_data = pd.json_normalize(data["data"])

        # Check if consumption data is empty and expand it
        if 'attributes.consumptionPerGroupMunicipalityHour' not in df_data.columns or df_data['attributes.consumptionPerGroupMunicipalityHour'].isnull().all():
            print("No consumption data available.")
            return pd.DataFrame()

        df_data_expanded = df_data.explode("attributes.consumptionPerGroupMunicipalityHour")
        
        # Normalize the nested consumption data
        df_consumption = pd.json_normalize(
            df_data_expanded["attributes.consumptionPerGroupMunicipalityHour"]
        )

        # Reset index to ensure proper alignment for concatenation
        df_data_expanded = df_data_expanded.reset_index(drop=True)
        df_consumption = df_consumption.reset_index(drop=True)

        # Combine the dataframes
        df_combined = pd.concat(
            [
                df_data_expanded[["attributes.municipalityNumber", "attributes.name"]],
                df_consumption,
            ],
            axis=1,
        )

        return df_combined

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"A request error occurred: {req_err}")
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
        
    return None

# To run this in a Jupyter interactive window, you can call the function directly:
elhub_df = get_elhub_data()
if elhub_df is not None:
    # First, filter out any non-numeric municipality numbers
    numeric_df = elhub_df[elhub_df["attributes.municipalityNumber"].str.isnumeric()].copy()

    # Convert the column to integer type for comparison
    numeric_df["attributes.municipalityNumber"] = numeric_df["attributes.municipalityNumber"].astype(int)

    # Now, filter for Telemark municipalities (number between 4000 and 4200)
    df_telemark = numeric_df[
        (numeric_df["attributes.municipalityNumber"] > 4000) & 
        (numeric_df["attributes.municipalityNumber"] < 4200)
    ]

    print("Filtered Telemark Data:")
    print(df_telemark.head())

    # Convert 'startTime' to datetime objects to extract the date
    df_telemark['startTime'] = pd.to_datetime(df_telemark['startTime'])

    # Group by date (extracted from startTime) and consumptionGroup, then sum the quantityKwh
    daily_summary = df_telemark.groupby([df_telemark['startTime'].dt.date, 'consumptionGroup'])['quantityKwh'].sum().reset_index()
    daily_summary = daily_summary.rename(columns={'startTime': 'date'})

    # Convert kWh to MWh
    daily_summary['quantityMwh'] = daily_summary['quantityKwh'] / 1000
    daily_summary = daily_summary.drop(columns=['quantityKwh'])

    print("\nDaily Consumption Summary (MWh) per Group:")
    print(daily_summary)

