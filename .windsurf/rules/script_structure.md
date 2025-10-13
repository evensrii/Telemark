---
trigger: manual
---

#### Standard workflow for most scripts

- I mostly write scripts that trigger this process (in order):

1) Query data from APIs and databases
2) Modify the data using pandas to produce a final dataset
3) Compare the final dataset with data in my Github repository
    - If the data don't already exist on Github, upload to Github.
    - If no new data, dont upload to Github.
    - If new (or different) data, upload to Github.
5) Always report the results of the comparison. If there were changes to the data, give a short summary (example) of the differences.


#### General script info

- I use an environment.yml file to manage dependencies and standard folders (PYTHONPATH, TEMP_FOLDER, LOG_FOLDER) 
- I use a "token.env" file to store my API keys and other sensitive information
- I use the GitHub API to fetch and push data to GitHub


#### Standard script structure

A) Import packages, modules and libraries

import os
import pandas as pd
from pyjstat import pyjstat <---- If the endpoint is "https://data.ssb.no/api"

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data <---- If the endpoint is "https://data.ssb.no/api"
from Helper_scripts.github_functions import handle_output_data

B) Other initial code

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

C) The query

# Run in a try-except block
# If the query is against "data.ssb.no", use the "fetch_data" function




#### Executing scripts

- During script construction, testing, and debugging, I run the individual scripts in the Jupyter interactive window in Windsurf.
- Every night, all scripts in my master script are run on my server using Windows Task Scheduler.


# File structure
- GitHub repository: https://github.com/evensrii/Telemark
- Individual scripts: Telemark/Python/Queries/[Different folders for different scripts]
- Helper scripts: Telemark/Python/Helper_scripts
- Master script: Telemark/Python/Automatisering/Task scheduler/master_script.py
- Output folder: Telemark/Data/[Different folders for different scripts]




# Standard data handling (most cases)
- After query and pandas processing, the data are handled by the handle_output_data function in the github_functions.py in the Helper_scripts folder.
- This involves a comparison of the new vs. existing data using the compare_to_github function in the github_functions.py in the Helper_scripts folder.
- It also involves reporting details of the new data. I.e. if there are new headers, new lines, other numerical values etc.
- The handle_output_data function also handles the uploading of any new data to GitHub using the upload_github_file function in the github_functions.py in the Helper_scripts folder.