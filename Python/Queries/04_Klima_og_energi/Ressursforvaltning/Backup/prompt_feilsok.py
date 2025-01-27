I use a Python script, "okologisk_tilstand_server_versjon.py", to fetch water quality data in rivers, lakes and seawater from www.vann-nett.no. There is no good API, so I have to extract the data from the website using selenium. The script clicks and ticks the right buttons, before collecting the data from an html table on the website. The script is as follows:

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
import time

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file
from Helper_scripts.github_functions import download_github_file
from Helper_scripts.github_functions import compare_to_github
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

### Function to extract data from the surface water table


def collect_water_body_data(water_body_name):
    print(f"\nCollecting data for {water_body_name}...")
    
    # Add multiple retries for finding the table
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Force UTF-8 encoding in webdriver
            driver.execute_script("document.charset='utf-8';")
            
            # Locate the table element using its class attribute
            table = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//table[contains(@class, '_table_i0c5l_1')]")
                )
            )
            
            # Wait for table to be visible and interactable
            wait.until(EC.visibility_of(table))
            
            # Additional wait to ensure table is fully loaded
            driver.execute_script("return document.readyState") == "complete"
            
            # Wait until the table contains multiple rows (ensures the table is fully loaded)
            rows = wait.until(lambda d: table.find_elements(By.XPATH, ".//tbody/tr"))
            if len(rows) <= 3:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1}: Table not fully loaded (only {len(rows)} rows). Retrying...")
                    driver.refresh()
                    time.sleep(2)  # Add small delay before retry
                    continue
                else:
                    raise Exception(f"Table not fully loaded after {max_retries} attempts")
            
            # Extract header names from the second row of <thead>
            header_row = wait.until(
                EC.presence_of_element_located((By.XPATH, ".//thead/tr[2]"))
            )
            header_names = [th.text for th in header_row.find_elements(By.TAG_NAME, "th")[:3]]
            print(f"Headers found: {header_names}")
            
            # Extract data from the <tbody> section of the table
            data_rows = table.find_elements(By.XPATH, ".//tbody/tr")
            print(f"Number of rows found: {len(data_rows)}")
            
            data = []
            for row in data_rows:
                try:
                    # Wait for row elements to be present
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "th")))
                    wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "td")))
                    
                    # Get the header (state) of the row
                    row_header = row.find_element(By.TAG_NAME, "th").text.strip()
                    if not row_header:
                        print(f"Warning: Empty row header found, skipping row")
                        continue
                    
                    # Get the first two data points in the row
                    cells = row.find_elements(By.TAG_NAME, "td")[:2]
                    first_two_data = [td.text.strip() for td in cells]
                    
                    # Handle dash values
                    if first_two_data[1] == '-':
                        # Try to get the value from the first column if second column is dash
                        if first_two_data[0] != '-':
                            try:
                                count = int(first_two_data[0])
                                total_rows = sum(int(row.find_elements(By.TAG_NAME, "td")[0].text.strip()) 
                                               for row in data_rows 
                                               if row.find_elements(By.TAG_NAME, "td")[0].text.strip() != '-')
                                percentage = f"{(count/total_rows)*100:.1f} %"
                                first_two_data[1] = percentage
                            except (ValueError, ZeroDivisionError):
                                first_two_data[1] = '0,0 %'
                        else:
                            first_two_data[1] = '0,0 %'
                    
                    # Verify we have valid data
                    if len(first_two_data) < 2 or not all(x.strip() for x in first_two_data):
                        print(f"Warning: Invalid data in row: {first_two_data}")
                        continue
                    
                    print(f"Row data - Header: {row_header}, Values: {first_two_data}")
                    data.append([row_header] + first_two_data)
                except Exception as e:
                    print(f"Error processing row: {str(e)}")
                    continue
            
            if not data:
                if attempt < max_retries - 1:
                    print("No valid data collected. Retrying...")
                    driver.refresh()
                    time.sleep(2)  # Add small delay before retry
                    continue
                else:
                    raise Exception("No valid data collected after all retries")
            
            # Create a DataFrame from the extracted data
            df = pd.DataFrame(data, columns=["Tilstand"] + header_names[:2])
            print(f"\nRaw DataFrame for {water_body_name}:")
            print(df)
            
            # Drop the second column (optional step based on requirement)
            df.drop(df.columns[1], axis=1, inplace=True)
            
            # Rename columns to include the specific water body name
            df.columns = ["Tilstand", water_body_name]
            
            # Remove the last row (if necessary, for cleaning)
            df = df[:-1]
            
            print(f"\nFinal DataFrame for {water_body_name}:")
            print(df)
            
            return df
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print("Refreshing page and retrying...")
                driver.refresh()
                time.sleep(2)  # Add small delay before retry
                continue
            else:
                raise Exception(f"Failed to collect data for {water_body_name} after {max_retries} attempts")


def clean_percentage(value):
    """Clean percentage values by removing % symbol and converting comma to period."""
    print(f"Cleaning value: {value} (type: {type(value)})")
    if isinstance(value, str):
        # Remove % symbol and whitespace
        value = value.replace('%', '').strip()
        # Convert comma to period
        value = value.replace(',', '.')
        try:
            result = float(value)
            print(f"Converted to: {result}")
            return result
        except ValueError:
            print(f"Could not convert {value} to float, returning 0")
            return 0
    print(f"Value is not a string, returning as is: {value}")
    return value


try:
    ########################## SETTING UP SELENIUM AND OPENING THE WEBSITE

    options = Options()
    options.add_argument("--headless=new")  # Run in headless mode

    # Initialize the WebDriver
    driver = webdriver.Chrome(options=options)

    # Navigate to the webpage containing the water data
    url = "https://vann-nett.no/waterbodies/factsheet/environmental-status"
    driver.get(url)

    # Set up an explicit wait for elements to load
    wait = WebDriverWait(driver, 15)

    ########################## ACCEPT COOKIES

    # Attempt to click the "Aksepter alle" button for cookie consent
    try:
        accept_all_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Accept all']"))
        )
        accept_all_button.click()
    except:
        print("No cookie banner appeared, continuing...")

    ########################## GODTA DRIFTSMELDINGER

    try:
        driftsmeldinger_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Ok']"))
        )
        driftsmeldinger_button.click()
    except:
        print("Ingen driftsmeldinger dukket opp, continuing...")

    ########################## CLOSE WELCOME WINDOW

    try:
        close_welcome_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class, 'close')]")
            )
        )
        close_welcome_button.click()
    except:
        print("No welcome window appeared, continuing...")

    ########################## PRESS LANGUAGE BUTTON


    language_button = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class, '_menuButton_13182_1')]")
        )
    )
    language_button.click()


    ########################## CHANGE LANGUAGE TO NORWEGIAN
  
    norwegian_button = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//li[contains(text(), 'Norwegian (bokmål)')]")
        )
    )
    norwegian_button.click()


    ########################## SELECT TELEMARK COUNTY

    fylke = driver.find_element(By.XPATH, "//span[normalize-space()='Fylke']")
    fylke.click()

    telemark_label = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//label[contains(text(), 'Telemark')]")
        )
    )
    telemark_checkbox_id = telemark_label.get_attribute("for")
    telemark_checkbox = driver.find_element(By.ID, telemark_checkbox_id)
    driver.execute_script("arguments[0].click();", telemark_checkbox)

    ########################## SELECT WATER CATEGORIES (River, Lake, Coastal Water)

    #### Click on the "Vannkategori" filter to expand the options
    vannkategori = driver.find_element(
        By.XPATH, "//span[normalize-space()='Vannkategori']"
    )
    vannkategori.click()

    ########################## COLLECT DATA FOR EACH WATER CATEGORY

    data_frames = {}  # Dictionary to store collected data

    # Collect data for each water body category
    def safely_collect_data(water_body_name, label_xpath, error_messages):
        """
        Collects data for a given water body category and handles errors.

        Parameters:
            water_body_name (str): The name of the water body (e.g., "Elv", "Innsjø").
            label_xpath (str): The XPath of the checkbox label for the water body.
            error_messages (list): A list to append error messages.

        Returns:
            pd.DataFrame or None: The collected DataFrame, or None if an error occurred.
        """
        try:
            # Locate the label and checkbox for the water body
            label = wait.until(EC.presence_of_element_located((By.XPATH, label_xpath)))
            checkbox_id = label.get_attribute("for")
            checkbox = driver.find_element(By.ID, checkbox_id)

            # Select the checkbox if not already selected
            if not checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)

            # Collect data using the provided function
            df = collect_water_body_data(water_body_name)

            # Uncheck the checkbox after collecting data
            if checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)

            return df

        except Exception as e:
            # Log and append the error message
            error_message = f"Failed to collect data for {water_body_name}: {str(e)}"
            print(error_message)
            error_messages.append(error_message)
            return None

    # Collect data for each category
    data_frames = {}

    # Collect data for "Elv"
    data_frames["Elv"] = safely_collect_data(
        "Elv", "//label[contains(text(), 'Elv')]", error_messages
    )

    # Collect data for "Innsjø"
    data_frames["Innsjø"] = safely_collect_data(
        "Innsjø", "//label[contains(text(), 'Innsjø')]", error_messages
    )

    # Collect data for "Kystvann"
    data_frames["Kystvann"] = safely_collect_data(
        "Kystvann", "//label[contains(text(), 'Kystvann')]", error_messages
    )

    ########################## MERGE AND PROCESS DATAFRAMES

    # Ensure all data frames were created successfully
    if any(df is None for df in data_frames.values()):
        raise ValueError("One or more data frames failed to be created.")

    # Merge the data frames
    df = pd.merge(data_frames["Elv"], data_frames["Innsjø"], on="Tilstand").merge(
        data_frames["Kystvann"], on="Tilstand"
    )

    # Replace missing values (represented by "-") with 0
    df.replace("-", 0, inplace=True)

    # Clean percentage values in all numeric columns
    for col in df.columns:
        if col != "Tilstand":
            df[col] = df[col].apply(clean_percentage)

    # Melt the dataframe to create a long-form DataFrame
    df_melted = pd.melt(
        df, id_vars=["Tilstand"], var_name="Kategori", value_name="Andel"
    )

    # Clean the "Andel" column after melting
    df_melted["Andel"] = df_melted["Andel"].apply(clean_percentage)

    # Pivot the DataFrame to restructure it with "Kategori" as the index
    df_pivoted = df_melted.pivot(
        index="Kategori", columns="Tilstand", values="Andel"
    ).reset_index()

    # Remove the name of the columns index (optional, for cleaner output)
    df_pivoted.columns.name = None

    # Reorder the columns for a specific layout
    df_pivoted = df_pivoted.loc[:, ["Kategori", "Svært god", "God", "Moderat", "Dårlig", "Svært dårlig"]]

    # Clean up numeric columns
    for col in df_pivoted.columns[1:]:
        df_pivoted[col] = df_pivoted[col].apply(clean_percentage)

    # Fill any NaN values with 0
    df_pivoted.fillna(0, inplace=True)

    # Print the final processed DataFrame
    print(df_pivoted)

except Exception as e:
    error_message = f"An unexpected error occurred: {str(e)}"
    print(error_message)
    error_messages.append(error_message)

finally:
    driver.quit()
    # print("Skriver dette i stedenfor driver.quit()")

# Notify yourself of errors, if any
if error_messages:
    notify_errors(error_messages, script_name="Vannkvalitet")
else:
    print("All tasks completed successfully.")


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "økologisk_tilstand_vann.csv"
task_name = "Klima og energi - Okologisk tilstand vann"
github_folder = "Data/04_Klima og ressursforvaltning/Ressursforvaltning"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_pivoted, file_name, github_folder, temp_folder, keepcsv=True)

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

When I run this script manually, this is the resulting csv file (økologisk_tilstand_vann.csv):

Kategori,Svært god,God,Moderat,Dårlig,Svært dårlig
Elv,20.4,65.6,11.2,1.7,1.1
Innsjø,20.5,66.7,12.2,0.3,0.3
Kystvann,4.1,40.8,51.0,0.0,4.1

This is the correct data.

The log/standard output of the run is:

Loaded .env file from: D:\Scripts\analyse\Telemark\Python\token.env
X_FUNCTIONS_KEY loaded successfully.
Loaded .env file from: D:\Scripts\analyse\Telemark\Python\token.env
GITHUB_TOKEN loaded successfully.
Ingen driftsmeldinger dukket opp, continuing...

Collecting data for Elv...
Headers found: ['TILSTAND', 'ANTALL', 'PROSENT %']
Number of rows found: 6
Row data - Header: Svært god, Values: ['227', '20,4 %']
Row data - Header: God, Values: ['730', '65,6 %']
Row data - Header: Moderat, Values: ['125', '11,2 %']
Row data - Header: Dårlig, Values: ['19', '1,7 %']
Row data - Header: Svært dårlig, Values: ['12', '1,1 %']
Row data - Header: Udefinert, Values: ['-', '0,0 %']

Raw DataFrame for Elv:
       Tilstand TILSTAND  ANTALL
0     Svært god      227  20,4 %
1           God      730  65,6 %
2       Moderat      125  11,2 %
3        Dårlig       19   1,7 %
4  Svært dårlig       12   1,1 %
5     Udefinert        -   0,0 %

Final DataFrame for Elv:
       Tilstand     Elv
0     Svært god  20,4 %
1           God  65,6 %
2       Moderat  11,2 %
3        Dårlig   1,7 %
4  Svært dårlig   1,1 %

Collecting data for Innsjø...
Headers found: ['TILSTAND', 'ANTALL', 'PROSENT %']
Number of rows found: 6
Row data - Header: Svært god, Values: ['59', '20,5 %']
Row data - Header: God, Values: ['192', '66,7 %']
Row data - Header: Moderat, Values: ['35', '12,2 %']
Row data - Header: Dårlig, Values: ['1', '0,3 %']
Row data - Header: Svært dårlig, Values: ['1', '0,3 %']
Row data - Header: Udefinert, Values: ['-', '0,0 %']

Raw DataFrame for Innsjø:
       Tilstand TILSTAND  ANTALL
0     Svært god       59  20,5 %
1           God      192  66,7 %
2       Moderat       35  12,2 %
3        Dårlig        1   0,3 %
4  Svært dårlig        1   0,3 %
5     Udefinert        -   0,0 %

Final DataFrame for Innsjø:
       Tilstand  Innsjø
0     Svært god  20,5 %
1           God  66,7 %
2       Moderat  12,2 %
3        Dårlig   0,3 %
4  Svært dårlig   0,3 %

Collecting data for Kystvann...
Headers found: ['TILSTAND', 'ANTALL', 'PROSENT %']
Number of rows found: 6
Row data - Header: Svært god, Values: ['2', '4,1 %']
Row data - Header: God, Values: ['20', '40,8 %']
Row data - Header: Moderat, Values: ['25', '51,0 %']
Row data - Header: Dårlig, Values: ['-', '0,0 %']
Row data - Header: Svært dårlig, Values: ['2', '4,1 %']
Row data - Header: Udefinert, Values: ['-', '0,0 %']

Raw DataFrame for Kystvann:
       Tilstand TILSTAND  ANTALL
0     Svært god        2   4,1 %
1           God       20  40,8 %
2       Moderat       25  51,0 %
3        Dårlig        -   0,0 %
4  Svært dårlig        2   4,1 %
5     Udefinert        -   0,0 %

Final DataFrame for Kystvann:
       Tilstand Kystvann
0     Svært god    4,1 %
1           God   40,8 %
2       Moderat   51,0 %
3        Dårlig    0,0 %
4  Svært dårlig    4,1 %
Cleaning value: 20,4 % (type: <class 'str'>)
Converted to: 20.4
Cleaning value: 65,6 % (type: <class 'str'>)
Converted to: 65.6
Cleaning value: 11,2 % (type: <class 'str'>)
Converted to: 11.2
Cleaning value: 1,7 % (type: <class 'str'>)
Converted to: 1.7
Cleaning value: 1,1 % (type: <class 'str'>)
Converted to: 1.1
Cleaning value: 20,5 % (type: <class 'str'>)
Converted to: 20.5
Cleaning value: 66,7 % (type: <class 'str'>)
Converted to: 66.7
Cleaning value: 12,2 % (type: <class 'str'>)
Converted to: 12.2
Cleaning value: 0,3 % (type: <class 'str'>)
Converted to: 0.3
Cleaning value: 0,3 % (type: <class 'str'>)
Converted to: 0.3
Cleaning value: 4,1 % (type: <class 'str'>)
Converted to: 4.1
Cleaning value: 40,8 % (type: <class 'str'>)
Converted to: 40.8
Cleaning value: 51,0 % (type: <class 'str'>)
Converted to: 51.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 4,1 % (type: <class 'str'>)
Converted to: 4.1
Cleaning value: 20.4 (type: <class 'float'>)
Value is not a string, returning as is: 20.4
Cleaning value: 65.6 (type: <class 'float'>)
Value is not a string, returning as is: 65.6
Cleaning value: 11.2 (type: <class 'float'>)
Value is not a string, returning as is: 11.2
Cleaning value: 1.7 (type: <class 'float'>)
Value is not a string, returning as is: 1.7
Cleaning value: 1.1 (type: <class 'float'>)
Value is not a string, returning as is: 1.1
Cleaning value: 20.5 (type: <class 'float'>)
Value is not a string, returning as is: 20.5
Cleaning value: 66.7 (type: <class 'float'>)
Value is not a string, returning as is: 66.7
Cleaning value: 12.2 (type: <class 'float'>)
Value is not a string, returning as is: 12.2
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
Cleaning value: 40.8 (type: <class 'float'>)
Value is not a string, returning as is: 40.8
Cleaning value: 51.0 (type: <class 'float'>)
Value is not a string, returning as is: 51.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
Cleaning value: 20.4 (type: <class 'float'>)
Value is not a string, returning as is: 20.4
Cleaning value: 20.5 (type: <class 'float'>)
Value is not a string, returning as is: 20.5
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
Cleaning value: 65.6 (type: <class 'float'>)
Value is not a string, returning as is: 65.6
Cleaning value: 66.7 (type: <class 'float'>)
Value is not a string, returning as is: 66.7
Cleaning value: 40.8 (type: <class 'float'>)
Value is not a string, returning as is: 40.8
Cleaning value: 11.2 (type: <class 'float'>)
Value is not a string, returning as is: 11.2
Cleaning value: 12.2 (type: <class 'float'>)
Value is not a string, returning as is: 12.2
Cleaning value: 51.0 (type: <class 'float'>)
Value is not a string, returning as is: 51.0
Cleaning value: 1.7 (type: <class 'float'>)
Value is not a string, returning as is: 1.7
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 1.1 (type: <class 'float'>)
Value is not a string, returning as is: 1.1
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
   Kategori  Svært god   God  Moderat  Dårlig  Svært dårlig
0       Elv       20.4  65.6     11.2     1.7           1.1
1    Innsjø       20.5  66.7     12.2     0.3           0.3
2  Kystvann        4.1  40.8     51.0     0.0           4.1
All tasks completed successfully.
Saved file to D:\Scripts\analyse\Telemark\Python\Temp\økologisk_tilstand_vann.csv
[2025-01-23 08:30:49] Changes detected in the dataset.
File uploaded successfully: Data/04_Klima og ressursforvaltning/Ressursforvaltning/økologisk_tilstand_vann.csv

[2025-01-23 08:30:49] Showing up to 5 examples of changes:

Row: 
  Drlig: 0.0 -> 1.7

Row: 
  God: 0.0 -> 65.6

Row: 
  Moderat: 0.0 -> 11.2

Row: 
  Svrt drlig: 0.0 -> 1.1

Row: 
  Svrt god: 0.0 -> 20.4

Total changes found in examined rows: 6
(Showing first 5 changes only)
Email notifications disabled. Updates for økologisk_tilstand_vann.csv were not sent.
Keeping CSV file: D:\Scripts\analyse\Telemark\Python\Temp\økologisk_tilstand_vann.csv
New data detected and pushed to GitHub.
New data status log written to D:\Scripts\analyse\Telemark\Python\Log\new_data_status_Klima_og_energi_-_Okologisk_tilstand_vann.log

When I run the script through a master script using Windows Task Scheduler (master_script.py):

import subprocess
import os
from datetime import datetime
from Helper_scripts.github_functions import compare_to_github, get_last_commit_time
from Helper_scripts.utility_functions import delete_files_in_temp_folder
import re
import sys

# Paths and configurations
base_path = os.getenv("PYTHONPATH")
if base_path is None:
    raise ValueError("PYTHONPATH environment variable is not set")

LOG_DIR = os.path.join(base_path, "Automatisering", "Task scheduler", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

MASTER_LOG_FILE = os.path.join(LOG_DIR, "00_master_run.log")
EMAIL_LOG_FILE = os.path.join(LOG_DIR, "00_email.log")
CONDA_ENV = "analyse"
PYTHON_PATH = os.getenv("PYTHONPATH")
if PYTHON_PATH is None:
    raise ValueError("PYTHONPATH environment variable is not set")

TEMP_FOLDER = os.environ.get("TEMP_FOLDER")
if TEMP_FOLDER is None:
    raise ValueError("TEMP_FOLDER environment variable is not set")

SCRIPTS = [
    ## Klima og energi
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Klimagassutslipp/klimagassutslipp.py"), "Klima og energi - Sektorvise utslipp"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Klimagassutslipp/norskeutslipp.py"), "Klima og energi - Utslipp fra landbasert industri"),
    (os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Ressursforvaltning/okologisk_tilstand.py"), "Klima og energi - Okologisk tilstand vann"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Ressursforvaltning/antall_felt.py"), "Klima og energi - Felte hjortedyr"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Produksjon/NVE/vannkraft.py"), "Klima og energi - Vannkraft Telemark"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Strompriser/strompriser.py"), "Klima og energi - Strompriser"),
    #(os.path.join(PYTHON_PATH, "Queries/04_Klima_og_energi/Kraft og energi/Forbruk/Elhub/elhub.py"), "Klima og energi - Elhub"),

    ## Idrett, friluftsliv og frivillighet (husk, ingen komma i oppgavenavn)
    #(os.path.join(PYTHON_PATH, "Queries/07_Idrett_friluftsliv_og_frivillighet/Friluftsliv/andel_jegere.py"), "Idrett friluftsliv og frivillighet - Jegere"),

    ## Innvandrere og inkludering
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/andel_flyktninger_og_arbeidsinnvandrere.py"), "Innvandrere - Flyktninger og arbeidsinnvandrere"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/botid.py"), "Innvandrere - Botid"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandrere_bosatt.py"), "Innvandrere - Bosatt"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Innvandrerbefolkningen/innvandringsgrunn.py"), "Innvandrere - Innvandringsgrunn"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_innvandrere_i_lavinntekt.py"), "Innvandrere - Lavinntekt"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_innvandrere.py"), "Innvandrere - Sysselsatte"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_etter_botid_og_landbakgrunn.py"), "Innvandrere - Sysselsatte etter botid og bakgrunn"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/deltakere_introduksjonsprogram.py"), "Innvandrere - Deltakere introduksjonsprogrammet"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Introduksjonsprogrammet/etter_introduksjonsprogram.py"), "Innvandrere - Etter introduksjonsprogrammet"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/enslige_mindreaarige.py"), "Innvandrere - Enslige mindreaarige"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/anmodninger_og_faktisk_bosetting.py"), "Innvandrere - Anmodninger og faktisk bosetting"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Bosetting_av_flyktninger/sekundaerflytting.py"), "Innvandrere - Sekundaerflytting"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_fullfort_vgo.py"), "Innvandrere - Fullfort VGO"),
    #(os.path.join(PYTHON_PATH, "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_hoyeste_utdanning.py"), "Innvandrere - Hoyeste utdanning"),
    
    ## Areal og stedsutvikling
    #(os.path.join(PYTHON_PATH, "Queries/10_Areal_og_stedsutvikling/Areal_til_jordbruk/jordbruksareal_per_kommune.py"), "Areal - Jordbruksareal per kommune"),
    #(os.path.join(PYTHON_PATH, "Queries/10_Areal_og_stedsutvikling/Areal_til_jordbruk/fulldyrka_vs_ikke-fulldyrka.py"), "Areal - Fulldyrka vs ikke-fulldyrka"),
]

# Initialize master log
with open(MASTER_LOG_FILE, "w", encoding="utf-8") as log_file:
    log_file.write(f"[{datetime.now()}] Master script initialized\n")


def run_script(script_path, task_name):
    """Run a single script in the Conda environment and log its result."""
    script_name = os.path.basename(script_path) #Eks. "innvandrere_botid.py"
    # Use the task name as-is for the log file name
    script_log_file = os.path.join(LOG_DIR, f"{task_name}.log") #Eks. "Innvandrere - Botid.log"
    timestamp = datetime.now().strftime("%d.%m.%Y  %H:%M:%S")
    status = "Completed"
    new_data = "No"
    last_commit = None  # Initialize last_commit at the start

    try:
        # First read the script content to extract github_folder and file_name
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                script_content = f.read()
                
            # Extract github_folder and potential file names
            github_folder_match = re.search(r'github_folder\s*=\s*["\']([^"\']+)["\']', script_content)
            file_name_matches = re.findall(r'file_name\d*\s*=\s*["\']([^"\']+)["\']', script_content)
            
            github_folder = github_folder_match.group(1) if github_folder_match else None

            # Get the last commit time for the CSV files if we found the necessary info
            if github_folder and file_name_matches:
                from Helper_scripts.github_functions import get_last_commit_time
                # Get the most recent commit time among all related files
                commit_times = []
                for file_name in file_name_matches:
                    file_path = f"{github_folder}/{file_name}"
                    commit_time = get_last_commit_time(file_path)
                    if commit_time:
                        commit_times.append(commit_time)
                
                if commit_times:
                    # Use the most recent commit time
                    last_commit = max(commit_times)
        except Exception as e:
            print(f"Warning: Could not extract file information from {script_path}: {e}")
            # Continue execution even if we can't get the commit time
        
        # Run the script and capture its output
        result = subprocess.run(
            [
                "conda",
                "run",
                "-n",
                CONDA_ENV,
                "python",
                script_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        # Write script output to its log file
        with open(script_log_file, "w", encoding="utf-8") as log:
            log.write(result.stdout)
            if result.stderr:
                log.write("\nErrors/Warnings:\n")
                log.write(result.stderr)

        # Check if new data was detected
        if "New data detected" in result.stdout or "New data detected" in result.stderr:
            new_data = "Yes"

    except subprocess.CalledProcessError as e:
        status = "Failed"
        # Write error output to the script's log file
        with open(script_log_file, "w", encoding="utf-8") as log:
            if e.stdout:
                log.write(e.stdout)
            if e.stderr:
                log.write("\nErrors:\n")
                log.write(e.stderr)

    # Log to master log file
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as log:
        log_entry = f"[{timestamp}] {task_name}: {script_name}: {status}, {new_data}"
        if last_commit:
            log_entry += f", {last_commit}"
        log.write(log_entry + "\n")


def send_email():
    """Call the email script to format and send the email."""
    with open(EMAIL_LOG_FILE, "w", encoding="utf-8") as email_log:
        # Dynamically construct the path to the email script
        python_path = os.getenv("PYTHONPATH")
        if python_path is None:
            raise ValueError("PYTHONPATH environment variable is not set")
        
        email_script_path = os.path.join(
            python_path, "Automatisering", "Task scheduler", "email_when_run_completed.py"
        )
        
        # Quote the script path to handle spaces
        command = f'cmd.exe /c "conda activate {CONDA_ENV} && python \"{email_script_path}\""'
        subprocess.run(command, shell=True, stdout=email_log, stderr=subprocess.STDOUT)


def main():
    """Main function to execute all scripts and send the email."""
    # Clear all logs except readme.txt
    for file in os.listdir(LOG_DIR):
        if file != "readme.txt":
            os.remove(os.path.join(LOG_DIR, file))

    # Run each script
    for script, task_name in SCRIPTS:
        run_script(script, task_name)

    # Send email
    send_email()

    # Clean up individual status log files from the Python/Log folder
    status_log_dir = os.path.join(PYTHON_PATH, "Log")
    if os.path.exists(status_log_dir):
        for file in os.listdir(status_log_dir):
            if (file.startswith("new_data_status_") and file.endswith(".log")) or \
               (file.startswith("last_commit_") and file.endswith(".log")):
                try:
                    os.remove(os.path.join(status_log_dir, file))
                except Exception as e:
                    print(f"Warning: Could not remove status file {file}: {e}")

    # Final log entry
    with open(MASTER_LOG_FILE, "a", encoding="utf-8") as master_log:
        master_log.write(f"[{datetime.now()}] Master script completed\n")


if __name__ == "__main__":
    main()

The standard output, written to file "Klima og energi - Okologisk tilstand vann.log", is as follows:

Loaded .env file from: D:\Scripts\analyse\Telemark\Python\token.env
X_FUNCTIONS_KEY loaded successfully.
Loaded .env file from: D:\Scripts\analyse\Telemark\Python\token.env
GITHUB_TOKEN loaded successfully.
Ingen driftsmeldinger dukket opp, continuing...

Collecting data for Elv...
Headers found: ['TILSTAND', 'ANTALL', 'PROSENT %']
Number of rows found: 6
Row data - Header: SvÃ¦rt god, Values: ['-', '0,0 %']
Row data - Header: God, Values: ['-', '0,0 %']
Row data - Header: Moderat, Values: ['-', '0,0 %']
Row data - Header: DÃ¥rlig, Values: ['-', '0,0 %']
Row data - Header: SvÃ¦rt dÃ¥rlig, Values: ['-', '0,0 %']
Row data - Header: Udefinert, Values: ['-', '0,0 %']

Raw DataFrame for Elv:
       Tilstand TILSTAND ANTALL
0     SvÃ¦rt god        -  0,0 %
1           God        -  0,0 %
2       Moderat        -  0,0 %
3        DÃ¥rlig        -  0,0 %
4  SvÃ¦rt dÃ¥rlig        -  0,0 %
5     Udefinert        -  0,0 %

Final DataFrame for Elv:
       Tilstand    Elv
0     SvÃ¦rt god  0,0 %
1           God  0,0 %
2       Moderat  0,0 %
3        DÃ¥rlig  0,0 %
4  SvÃ¦rt dÃ¥rlig  0,0 %

Collecting data for InnsjÃ¸...
Headers found: ['TILSTAND', 'ANTALL', 'PROSENT %']
Number of rows found: 6
Row data - Header: SvÃ¦rt god, Values: ['-', '0,0 %']
Row data - Header: God, Values: ['192', '66,7 %']
Row data - Header: Moderat, Values: ['35', '12,2 %']
Row data - Header: DÃ¥rlig, Values: ['1', '0,3 %']
Row data - Header: SvÃ¦rt dÃ¥rlig, Values: ['1', '0,3 %']
Row data - Header: Udefinert, Values: ['-', '0,0 %']

Raw DataFrame for InnsjÃ¸:
       Tilstand TILSTAND  ANTALL
0     SvÃ¦rt god        -   0,0 %
1           God      192  66,7 %
2       Moderat       35  12,2 %
3        DÃ¥rlig        1   0,3 %
4  SvÃ¦rt dÃ¥rlig        1   0,3 %
5     Udefinert        -   0,0 %

Final DataFrame for InnsjÃ¸:
       Tilstand  InnsjÃ¸
0     SvÃ¦rt god   0,0 %
1           God  66,7 %
2       Moderat  12,2 %
3        DÃ¥rlig   0,3 %
4  SvÃ¦rt dÃ¥rlig   0,3 %

Collecting data for Kystvann...
Headers found: ['TILSTAND', 'ANTALL', 'PROSENT %']
Number of rows found: 6
Row data - Header: SvÃ¦rt god, Values: ['2', '4,1 %']
Row data - Header: God, Values: ['20', '40,8 %']
Row data - Header: Moderat, Values: ['25', '51,0 %']
Row data - Header: DÃ¥rlig, Values: ['-', '0,0 %']
Row data - Header: SvÃ¦rt dÃ¥rlig, Values: ['2', '4,1 %']
Row data - Header: Udefinert, Values: ['-', '0,0 %']

Raw DataFrame for Kystvann:
       Tilstand TILSTAND  ANTALL
0     SvÃ¦rt god        2   4,1 %
1           God       20  40,8 %
2       Moderat       25  51,0 %
3        DÃ¥rlig        -   0,0 %
4  SvÃ¦rt dÃ¥rlig        2   4,1 %
5     Udefinert        -   0,0 %

Final DataFrame for Kystvann:
       Tilstand Kystvann
0     SvÃ¦rt god    4,1 %
1           God   40,8 %
2       Moderat   51,0 %
3        DÃ¥rlig    0,0 %
4  SvÃ¦rt dÃ¥rlig    4,1 %
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 66,7 % (type: <class 'str'>)
Converted to: 66.7
Cleaning value: 12,2 % (type: <class 'str'>)
Converted to: 12.2
Cleaning value: 0,3 % (type: <class 'str'>)
Converted to: 0.3
Cleaning value: 0,3 % (type: <class 'str'>)
Converted to: 0.3
Cleaning value: 4,1 % (type: <class 'str'>)
Converted to: 4.1
Cleaning value: 40,8 % (type: <class 'str'>)
Converted to: 40.8
Cleaning value: 51,0 % (type: <class 'str'>)
Converted to: 51.0
Cleaning value: 0,0 % (type: <class 'str'>)
Converted to: 0.0
Cleaning value: 4,1 % (type: <class 'str'>)
Converted to: 4.1
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 66.7 (type: <class 'float'>)
Value is not a string, returning as is: 66.7
Cleaning value: 12.2 (type: <class 'float'>)
Value is not a string, returning as is: 12.2
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
Cleaning value: 40.8 (type: <class 'float'>)
Value is not a string, returning as is: 40.8
Cleaning value: 51.0 (type: <class 'float'>)
Value is not a string, returning as is: 51.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 66.7 (type: <class 'float'>)
Value is not a string, returning as is: 66.7
Cleaning value: 40.8 (type: <class 'float'>)
Value is not a string, returning as is: 40.8
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 12.2 (type: <class 'float'>)
Value is not a string, returning as is: 12.2
Cleaning value: 51.0 (type: <class 'float'>)
Value is not a string, returning as is: 51.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.0 (type: <class 'float'>)
Value is not a string, returning as is: 0.0
Cleaning value: 0.3 (type: <class 'float'>)
Value is not a string, returning as is: 0.3
Cleaning value: 4.1 (type: <class 'float'>)
Value is not a string, returning as is: 4.1
   Kategori  SvÃ¦rt god   God  Moderat  DÃ¥rlig  SvÃ¦rt dÃ¥rlig
0       Elv        0.0   0.0      0.0     0.0           0.0
1    InnsjÃ¸        0.0  66.7     12.2     0.3           0.3
2  Kystvann        4.1  40.8     51.0     0.0           4.1
All tasks completed successfully.
Saved file to D:\Scripts\analyse\Telemark\Python\Temp\Ã¸kologisk_tilstand_vann.csv
[2025-01-23 02:03:10] Changes detected in the dataset.
File uploaded successfully: Data/04_Klima og ressursforvaltning/Ressursforvaltning/Ã¸kologisk_tilstand_vann.csv

[2025-01-23 02:03:10] Showing up to 5 examples of changes:

Row: 
  Drlig: 1.7 -> 0.0

Row: 
  God: 65.6 -> 0.0

Row: 
  Moderat: 11.2 -> 0.0

Row: 
  Svrt drlig: 1.1 -> 0.0

Row: 
  Svrt god: 20.4 -> 0.0

Total changes found in examined rows: 6
(Showing first 5 changes only)
Email notifications disabled. Updates for Ã¸kologisk_tilstand_vann.csv were not sent.
Keeping CSV file: D:\Scripts\analyse\Telemark\Python\Temp\Ã¸kologisk_tilstand_vann.csv
New data detected and pushed to GitHub.
New data status log written to D:\Scripts\analyse\Telemark\Python\Log\new_data_status_Klima_og_energi_-_Okologisk_tilstand_vann.log


And the csv file is like this:

Kategori,Svært god,God,Moderat,Dårlig,Svært dårlig
Elv,0.0,0.0,0.0,0.0,0.0
Innsjø,0.0,66.7,12.2,0.3,0.3
Kystvann,4.1,40.8,51.0,0.0,4.1

This is incorrect, as the first row is all zeroes, and the first value in "Innsjø" is also zero.

Please help me understand and fix the problem, so that the nightly runs of the master script also gives the correct data in "økologisk_tilstand_vann.csv".