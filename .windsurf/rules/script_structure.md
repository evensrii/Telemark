---
trigger: model_decision
description: This rule should be activated when asked to create or modify a script for fetching data from APIs etc. NB: There is room for flexibility.
---

### Standard workflow for most scripts

- I mostly write scripts that trigger this process (in order):

1) Query data from an API and save the result as a pandas dataframe
2) Modify the original dataframe using pandas to produce a final dataframe
3) Compare the final dataframe with data in my Github repository
    - If the data don't already exist on Github, upload csv to Github.
    - If no new data, dont upload csv to Github.
    - If new (or different) data, upload updated csv to Github.
5) Always report the results of the comparison. If there were changes to the data, give a short summary (example) of the differences. Are there new headers, new lines, other numerical values etc.


#### General script info

- I use an environment.yml file to manage dependencies and standard folders (PYTHONPATH, TEMP_FOLDER, LOG_FOLDER) 
- I use a "token.env" file to store my API keys and other sensitive information
- I use the GitHub API to fetch and push data to GitHub (repository: https://github.com/evensrii/Telemark)

#### Standard script structure


1) Import packages, modules and libraries

- There are two key functions in the workflow:
   - fetch_data(): Query data and saves the results as a pandas dataframe.
   - handle_output_data(): Compares the final data to the data on Github, and pushes to Github if new data. Trigger many other important Github-related functions.

# Initial code:

import os
import pandas as pd
from pyjstat import pyjstat # <---- Used for both old and new SSB API (json-stat2 format)

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data


2) Other initial code

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- A list to collect errors during a run
error_messages = []


3) The query

- Run the query in a try-except block using "fetch_data()". This collects data and saves to a pandas dataframe.
- There are two SSB API patterns:

### Option A: Old SSB API (POST, v0) — used in older scripts

POST_URL = "https://data.ssb.no/api/v0/no/table/XXXXX/"
payload = {
    "query": [
        {"code": "Region", "selection": {"filter": "agg:KommSummer", "values": ["K-4001"]}},
        {"code": "ContentsCode", "selection": {"filter": "item", "values": ["Personer1"]}},
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"}
}

try:
    df = fetch_data(
        url=POST_URL,
        payload=payload,  # The JSON payload for POST requests.
        error_messages=error_messages,
        query_name="Tittel spørring",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### Option B: New SSB API (GET, v2) — preferred for new scripts

GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/XXXXX/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[ContentsCode]=*"
    "&valuecodes[Tid]=top(1)"
    "&valuecodes[Region]=4001,4003,..."
    "&codelist[Region]=agg_KommGjeldende"
)

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,  # None = GET request (new SSB API v2)
        error_messages=error_messages,
        query_name="Tittel spørring",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

### Notes on SSB API choice
- Both return json-stat2 and are parsed by pyjstat via fetch_data().
- New API (v2 GET): All query parameters are in the URL. Set payload=None.
- Old API (v0 POST): Query parameters are in a JSON payload dict.
- For non-SSB endpoints, use response_type="csv" with delimiter and encoding parameters.


4) Manual refinement of the dataset

- Add a code section to MANUALLY modify the pandas dataframe (df.head() etc).
- This section has to be OUTSIDE any functions, as I run this code line by line in the Jupyter interactive window and check the output.
- Important for testing and debugging.


5) Save dataframe as local temporary csv, compare final data to Github, upload if needed.

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "bosetting_enslige_mindreårige.csv" # <-- Example
task_name = "Innvandrere - Enslige mindreaarige" # <-- Example
github_folder = "Data/09_Innvandrere og inkludering/Bosetting av flyktninger" # <-- Example
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_telemark,  # <-- Example name of dataframe
    file_name, 
    github_folder, 
    temp_folder, 
    keepcsv=True,
    value_columns=['Anmodning om bosetting']  #  # <-- Example. Only compares this column
)

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