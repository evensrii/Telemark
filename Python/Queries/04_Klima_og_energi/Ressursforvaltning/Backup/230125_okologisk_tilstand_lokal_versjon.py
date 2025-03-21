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
    # Locate the table element using its class attribute
    table = wait.until(
        EC.presence_of_element_located(
            # Use XPath to find the table by class name
            (By.XPATH, "//table[contains(@class, '_table_i0c5l_1')]")
        )
    )

    # Wait until the table contains multiple rows (ensures the table is fully loaded)
    wait.until(lambda d: len(table.find_elements(By.XPATH, ".//tbody/tr")) > 3)

    # Extract header names from the second row of <thead> (skip the first empty header row)
    header_row = table.find_element(By.XPATH, ".//thead/tr[2]")
    header_names = [
        th.text for th in header_row.find_elements(By.TAG_NAME, "th")[:3]
    ]  # Extract the first three headers

    # Extract data from the <tbody> section of the table
    data_rows = table.find_elements(By.XPATH, ".//tbody/tr")
    data = []
    for row in data_rows:
        # Get the header (state) of the row (e.g., 'Svært god', 'God')
        row_header = row.find_element(By.TAG_NAME, "th").text
        # Get the first two data points in the row
        first_two_data = [td.text for td in row.find_elements(By.TAG_NAME, "td")[:2]]
        # Append the row header and data to the list
        data.append([row_header] + first_two_data)

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(
        data, columns=["Tilstand"] + header_names[:2]  # Only use relevant headers
    )

    # Drop the second column (optional step based on requirement)
    df.drop(df.columns[1], axis=1, inplace=True)

    # Rename columns to include the specific water body name
    df.columns = ["Tilstand", water_body_name]

    # Remove the last row (if necessary, for cleaning)
    df = df[:-1]

    # Clean percentage values
    df[water_body_name] = df[water_body_name].apply(clean_percentage)

    return df


def clean_percentage(value):
    """Clean percentage values by removing % symbol and converting comma to period."""
    if isinstance(value, str):
        # Remove % symbol and whitespace
        value = value.replace('%', '').strip()
        # Convert comma to period
        value = value.replace(',', '.')
        try:
            return float(value)
        except ValueError:
            return 0
    return value


try:
    ########################## SETTING UP SELENIUM AND OPENING THE WEBSITE

    options = Options()
    # options.add_argument("--headless=new")  # Run in headless mode

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
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Aksepter alle']"))
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
    df_pivoted = df_pivoted[
        ["Kategori", "Svært god", "God", "Moderat", "Dårlig", "Svært dårlig"]
    ]

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
