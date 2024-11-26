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

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution <--- Eksempel på liste for å samle feilmeldinger under kjøring
error_messages = []

try:
    ########################## SETTING UP SELENIUM AND OPENING THE WEBSITE

    options = Options()
    options.add_argument("--headless=new")  # Run in headless mode

    # Initialize the WebDriver
    driver = webdriver.Chrome(options=options)

    # Navigate to the webpage containing the water data
    url = "https://vann-nett-klient.miljodirektoratet.no/waterbodies/factsheet/environmental-status"
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

    ########################## COLLECT DATA FOR EACH WATER CATEGORY

    data_frames = {}

    def safely_collect_data(water_body_name, checkbox, error_messages):
        """
        Collects data for a given water body and handles errors.
        """
        try:
            if not checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)
            return collect_water_body_data(water_body_name)
        except Exception as e:
            error_message = f"Failed to collect data for {water_body_name}: {str(e)}"
            print(error_message)
            error_messages.append(error_message)
            return None

    # Elv (River)
    data_frames["Elv"] = safely_collect_data("Elv", telemark_checkbox, error_messages)

    # Innsjø (Lake)
    innsjo_label = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//label[contains(text(), 'Innsjø')]")
        )
    )
    innsjo_checkbox_id = innsjo_label.get_attribute("for")
    innsjo_checkbox = driver.find_element(By.ID, innsjo_checkbox_id)
    data_frames["Innsjø"] = safely_collect_data(
        "Innsjø", innsjo_checkbox, error_messages
    )

    # Kystvann (Coastal Water)
    kystvann_label = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//label[contains(text(), 'Kystvann')]")
        )
    )
    kystvann_checkbox_id = kystvann_label.get_attribute("for")
    kystvann_checkbox = driver.find_element(By.ID, kystvann_checkbox_id)
    data_frames["Kystvann"] = safely_collect_data(
        "Kystvann", kystvann_checkbox, error_messages
    )

    ########################## MERGE AND PROCESS DATAFRAMES

    # Ensure all data frames were created successfully
    if None in data_frames.values():
        raise ValueError("One or more data frames failed to be created.")

    # Merge the data frames
    df = pd.merge(data_frames["Elv"], data_frames["Innsjø"], on="Tilstand").merge(
        data_frames["Kystvann"], on="Tilstand"
    )

    # Replace missing values (represented by "-") with 0
    df.replace("-", 0, inplace=True)

    # Melt the dataframe to create a long-form DataFrame
    df_melted = pd.melt(
        df, id_vars=["Tilstand"], var_name="Kategori", value_name="Andel"
    )

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
        df_pivoted[col] = df_pivoted[col].str.replace(
            "%", "", regex=False
        )  # Remove "%"
        df_pivoted[col] = df_pivoted[col].str.replace(
            ",", ".", regex=False
        )  # Replace ","
        df_pivoted[col] = pd.to_numeric(df_pivoted[col])  # Convert to numeric type

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

# Notify yourself of errors, if any
if error_messages:
    notify_errors(error_messages, script_name="WaterBodyDataCollection")
else:
    print("All tasks completed successfully.")


############# SAVE DATAFRAME TO CSV FILE

# Define the CSV file path and save the DataFrame
csv = "økologisk_tilstand_vann.csv"
df_pivoted.to_csv((f"../../Temp/{csv}"), index=False)


##################### Opplasting til Github #####################

# Legge til directory hvor man finner github_functions.py i sys.path for å kunne importere denne
current_directory = os.path.dirname(os.path.abspath(__file__))
two_levels_up_directory = os.path.abspath(
    os.path.join(current_directory, os.pardir, os.pardir)
)
sys.path.append(two_levels_up_directory)

from github_functions import upload_file_to_github

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

csv_file = f"../../Temp/{csv}"
destination_folder = "Data/04_Klima og ressursforvaltning/Ressursforvaltning"  # Mapper som ikke eksisterer vil opprettes automatisk.
github_repo = "evensrii/Telemark"
git_branch = "main"

upload_file_to_github(csv_file, destination_folder, github_repo, git_branch)

##################### Remove temporary files #####################

# Delete files in folder using glob


def delete_files_in_folder(folder_path):
    # Construct the path pattern to match all files in the folder
    files = glob.glob(os.path.join(folder_path, "*"))

    # Iterate over the list of files and delete each one
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")


driver.quit()
