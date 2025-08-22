import os
import pandas as pd
from datetime import datetime
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors

from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []


# Antall 2024 - NÅ
# Antall 2020 - 2023
# Antall 2004 - 2019

# Andel 2024 - NÅ
# Andel 2020 - 2023
# Andel 2004 - 2019

################# Spørring #################

################################ Antall 2024 - NÅ

# Endepunkt for SSB API
# Generate a list of years from 2024 to the current year
current_year = datetime.now().year
years_to_query = [str(year) for year in range(2024, current_year + 1)]

# If the list is empty (i.e., current year is before 2024), default to 2024
if not years_to_query:
    years_to_query = ["2024"]

POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:KommGjeldende",
        "values": [
          "4001",
          "4003",
          "4005",
          "4010",
          "4012",
          "4014",
          "4016",
          "4018",
          "4020",
          "4022",
          "4024",
          "4026",
          "4028",
          "4030",
          "4032",
          "4034",
          "4036"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",                              
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Personer"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": years_to_query
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

df_antall_2024_naa = None
while df_antall_2024_naa is None and years_to_query:
    try:
        # Update payload with the current list of years to try
        payload['query'][-1]['selection']['values'] = years_to_query

        df_antall_2024_naa = fetch_data(
            url=POST_URL,
            payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
            error_messages=error_messages,
            query_name="Antall 2024 - NÅ",
            response_type="json",  # The expected response type, either 'json' or 'csv'.
        )
        print(f"Successfully fetched data for years: {years_to_query}")

    except Exception as e:
        # Check if the error is a 400 Bad Request and there are years to remove
        if "400 Client Error" in str(e) and len(years_to_query) > 1:
            removed_year = years_to_query.pop()
            print(f"Query failed for years up to {removed_year}. Retrying with years up to {years_to_query[-1]}.")
        else:
            print(f"An unrecoverable error occurred: {e}")
            notify_errors(error_messages, script_name=script_name)
            raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

# If df is still None after the loop, it means all attempts failed
if df_antall_2024_naa is None:
    error_msg = "Could not fetch data from SSB after multiple attempts."
    print(error_msg)
    error_messages.append(error_msg)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(error_msg)



################################ Antall 2020 - 2023


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:Komm2020",
        "values": [
          "3806",
          "3807",
          "3808",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Personer"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2020",
          "2021",
          "2022",
          "2023"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_antall_2020_2023 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Antall 2020 - 2023",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )


################################ Antall 2004 - 2019


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Kommune",
        "values": [
          "0805",
          "0806",
          "0807",
          "0811",
          "0814",
          "0815",
          "0817",
          "0819",
          "0821",
          "0822",
          "0826",
          "0827",
          "0828",
          "0829",
          "0830",
          "0831",
          "0833",
          "0834"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Personer"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2004",
          "2005",
          "2006",
          "2007",
          "2008",
          "2009",
          "2010",
          "2011",
          "2012",
          "2013",
          "2014",
          "2015",
          "2016",
          "2017",
          "2018",
          "2019"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_antall_2004_2019 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Antall 2004 - 2019",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )



################################ Andel 2024 - NÅ

# Endepunkt for SSB API
# Generate a list of years from 2024 to the current year
current_year = datetime.now().year
years_to_query = [str(year) for year in range(2024, current_year + 1)]

# If the list is empty (i.e., current year is before 2024), default to 2024
if not years_to_query:
    years_to_query = ["2024"]

POST_URL = "https://data.ssb.no/api/v0/no/table/09429/"

# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:KommGjeldende",
        "values": [
          "4001",
          "4003",
          "4005",
          "4010",
          "4012",
          "4014",
          "4016",
          "4018",
          "4020",
          "4022",
          "4024",
          "4026",
          "4028",
          "4030",
          "4032",
          "4034",
          "4036"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",                              
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "PersonerProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": years_to_query
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

df_andel_2024_naa = None
while df_andel_2024_naa is None and years_to_query:
    try:
        # Update payload with the current list of years to try
        payload['query'][-1]['selection']['values'] = years_to_query

        df_andel_2024_naa = fetch_data(
            url=POST_URL,
            payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
            error_messages=error_messages,
            query_name="Andel 2024 - NÅ",
            response_type="json",  # The expected response type, either 'json' or 'csv'.
        )
        print(f"Successfully fetched data for years: {years_to_query}")

    except Exception as e:
        # Check if the error is a 400 Bad Request and there are years to remove
        if "400 Client Error" in str(e) and len(years_to_query) > 1:
            removed_year = years_to_query.pop()
            print(f"Query failed for years up to {removed_year}. Retrying with years up to {years_to_query[-1]}.")
        else:
            print(f"An unrecoverable error occurred: {e}")
            notify_errors(error_messages, script_name=script_name)
            raise RuntimeError("A critical error occurred during data fetching, stopping execution.")

# If df is still None after the loop, it means all attempts failed
if df_andel_2024_naa is None:
    error_msg = "Could not fetch data from SSB after multiple attempts."
    print(error_msg)
    error_messages.append(error_msg)
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(error_msg)



################################ Andel 2020 - 2023


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:Komm2020",
        "values": [
          "3806",
          "3807",
          "3808",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "PersonerProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2020",
          "2021",
          "2022",
          "2023"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}

## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_andel_2020_2023 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Andel 2020 - 2023",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )



################################ Andel 2004 - 2019


# Spørring for å hente ut data fra SSB
payload = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Kommune",
        "values": [
          "0805",
          "0806",
          "0807",
          "0811",
          "0814",
          "0815",
          "0817",
          "0819",
          "0821",
          "0822",
          "0826",
          "0827",
          "0828",
          "0829",
          "0830",
          "0831",
          "0833",
          "0834"
        ]
      }
    },
    {
      "code": "Nivaa",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02a",
          "11",
          "03a",
          "04a",
          "09a"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "2",
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "PersonerProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2004",
          "2005",
          "2006",
          "2007",
          "2008",
          "2009",
          "2010",
          "2011",
          "2012",
          "2013",
          "2014",
          "2015",
          "2016",
          "2017",
          "2018",
          "2019"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}


## Kjøre spørringer i try-except for å fange opp feil. Quitter hvis feil.

try:
    df_andel_2004_2019 = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests. If None, a GET request is used.
        error_messages=error_messages,
        query_name="Andel 2004 - 2019",
        response_type="json",  # The expected response type, either 'json' or 'csv'.
        # delimiter=";", # The delimiter for CSV data (default: ';').
        # encoding="ISO-8859-1", # The encoding for CSV data (default: 'ISO-8859-1').
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### DATA CLEANING

# Combine the dataframes
df_antall = pd.concat([df_antall_2024_naa, df_antall_2020_2023, df_antall_2004_2019], ignore_index=True)
df_andel = pd.concat([df_andel_2024_naa, df_andel_2020_2023, df_andel_2004_2019], ignore_index=True)

df_combined = pd.concat([df_antall, df_andel], ignore_index=True)

#Rename the columnss to "Kommune", "Utdanningsnivå", "Kjønn", "Variabel", "År" and "Verdi"
df_combined.columns = ["Kommune", "Utdanningsnivå", "Kjønn", "Variabel", "År", "Verdi"]

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "utdanningsnivå.csv"
task_name = "Opplæring og kompetanse - Utdanningsnivå"
github_folder = "Data/02_Opplæring og kompetanse/Utdanningsnivå"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_combined, file_name, github_folder, temp_folder, keepcsv=True)

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
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")  