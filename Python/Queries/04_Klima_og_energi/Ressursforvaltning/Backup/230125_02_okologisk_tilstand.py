# Import required libraries
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
import io
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

# Set up environment variables
FILE_NAME = "økologisk_tilstand_vann.csv"
GITHUB_FOLDER = "Data/04_Klima og ressursforvaltning/Ressursforvaltning"

# Ensure environment variables are set
if not os.environ.get("TEMP_FOLDER"):
    raise ValueError("TEMP_FOLDER environment variable is not set")

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect error messages during execution
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


# Store the default total when the page first loads
default_total = None

def get_table_total():
    """
    Gets the current total from the "Alle" row in the table.
    Returns None if total cannot be retrieved.
    """
    try:
        # Wait for table to be present and visible
        table = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//table[contains(@class, '_table_i0c5l_1')]")
        ))
        
        # Wait specifically for the "Alle" row
        alle_row = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//table[contains(@class, '_table_i0c5l_1')]//tr[.//th[normalize-space()='Alle']]")
        ))
        
        # Get the first td element in the Alle row
        total_cell = alle_row.find_element(By.TAG_NAME, "td")
        total_text = total_cell.text.strip()
        
        # Remove any thousands separators (periods) and convert to integer
        try:
            # Remove periods used as thousand separators and any spaces
            cleaned_total = total_text.replace('.', '').replace(' ', '')
            print(f"Converting total from '{total_text}' (cleaned: '{cleaned_total}')")
            return int(cleaned_total)
        except ValueError:
            print(f"Could not convert total '{total_text}' (cleaned: '{cleaned_total}') to integer")
            return None
            
    except Exception as e:
        print(f"Error getting table total: {str(e)}")
        return None

def initialize_default_total():
    """
    Initialize the default total after Telemark has been selected.
    Should be called only once after Telemark selection.
    """
    global default_total
    default_total = get_table_total()
    if default_total is None:
        raise Exception("Could not establish default total after Telemark selection")
    print(f"Default total established as: {default_total} after Telemark selection")

def ensure_table_updated(expecting_default=False, max_retries=10, retry_delay=1):
    """
    Ensures the table has been fully updated after a category selection change.
    
    Args:
        expecting_default (bool): True if we expect to see the default total (after unchecking)
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay in seconds between retries
        
    Returns:
        bool: True if table is confirmed updated, False otherwise
    """
    global default_total
    
    # Verify default_total has been initialized
    if default_total is None:
        raise Exception("Default total not initialized. Must select Telemark first.")
    
    for attempt in range(max_retries):
        try:
            current_total = get_table_total()
            
            if current_total is None:
                print(f"Attempt {attempt + 1}/{max_retries}: Could not get current total")
                time.sleep(retry_delay)
                continue
                
            if expecting_default:
                # When unchecking, we expect to return to default total
                if current_total == default_total:
                    print(f"Table returned to default state (total: {current_total})")
                    return True
            else:
                # When checking a category, we expect a different total than default
                if current_total != default_total:
                    print(f"Table updated with new total: {current_total} (different from default: {default_total})")
                    return True
            
            print(f"Attempt {attempt + 1}/{max_retries}: Waiting for table update...")
            time.sleep(retry_delay)
            
        except Exception as e:
            print(f"Error checking table update (attempt {attempt + 1}): {str(e)}")
            time.sleep(retry_delay)
            
    return False

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
        print(f"\nCollecting data for {water_body_name}...")
        
        # Make sure Vannkategori is expanded
        try:
            vannkategori = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//span[normalize-space()='Vannkategori']")
            ))
            
            # Check if we need to expand by looking for visible checkboxes
            checkboxes_visible = driver.find_elements(By.XPATH, "//label[contains(text(), 'Elv')]")
            if not checkboxes_visible or not any(cb.is_displayed() for cb in checkboxes_visible):
                vannkategori.click()
                time.sleep(1)
        except Exception as e:
            print(f"Warning: Issue with Vannkategori section: {str(e)}")
        
        # Wait for and locate the label with retry
        max_retries = 3
        label = None
        for attempt in range(max_retries):
            try:
                label = wait.until(EC.presence_of_element_located((By.XPATH, label_xpath)))
                if label.is_displayed():
                    break
                time.sleep(1)
            except Exception:
                if attempt == max_retries - 1:
                    raise Exception(f"Could not locate visible label for {water_body_name}")
                time.sleep(1)
        
        # Get checkbox ID and locate checkbox
        checkbox_id = label.get_attribute("for")
        if not checkbox_id:
            raise Exception(f"Could not get checkbox ID for {water_body_name}")
        
        # Try to find the checkbox both by ID and by following the label
        try:
            checkbox = wait.until(EC.presence_of_element_located((By.ID, checkbox_id)))
        except:
            # If ID fails, try finding the input associated with the label
            checkbox = label.find_element(By.XPATH, "following-sibling::input[@type='checkbox']")
        
        # Ensure checkbox is not already selected
        if checkbox.is_selected():
            print(f"Warning: {water_body_name} checkbox was already selected, unchecking first")
            driver.execute_script("arguments[0].click();", checkbox)
            time.sleep(1)
        
        # Select the checkbox
        print(f"Clicking checkbox for {water_body_name}")
        driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(1)
        
        # Verify selection and retry if needed
        if not checkbox.is_selected():
            print(f"First click failed for {water_body_name}, retrying...")
            driver.execute_script("arguments[0].click();", checkbox)
            time.sleep(1)
            
            if not checkbox.is_selected():
                raise Exception(f"Failed to select checkbox for {water_body_name} after retry")
        
        # Wait for table to update
        print("Waiting for table to update...")
        time.sleep(2)
        
        # Collect data
        df = collect_water_body_data(water_body_name)
        
        # Uncheck after collection
        if checkbox.is_selected():
            driver.execute_script("arguments[0].click();", checkbox)
            time.sleep(1)
        
        return df
        
    except Exception as e:
        error_message = f"Failed to collect data for {water_body_name}: {str(e)}"
        print(error_message)
        error_messages.append(error_message)
        return None

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

    try:
        # First select Telemark
        fylke = driver.find_element(By.XPATH, "//span[normalize-space()='Fylke']")
        fylke.click()

        telemark_label = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//label[contains(text(), 'Telemark')]")
            )
        )
        telemark_checkbox_id = telemark_label.get_attribute("for")
        telemark_checkbox = driver.find_element(By.ID, telemark_checkbox_id)

        # Select Telemark if not already selected
        if not telemark_checkbox.is_selected():
            driver.execute_script("arguments[0].click();", telemark_checkbox)
            time.sleep(2)  # Give time for the table to update after Telemark selection

        ########################## EXPAND VANNKATEGORI SECTION

        # Click on the "Vannkategori" filter to expand the options
        try:
            vannkategori = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//span[normalize-space()='Vannkategori']")
                )
            )
            vannkategori.click()
            time.sleep(1)  # Give time for the section to expand
        except Exception as e:
            raise Exception(f"Failed to expand Vannkategori section: {str(e)}")
        
        # Initialize the default total after Telemark is selected and Vannkategori is expanded
        # Retry a few times if needed
        max_retries = 5
        for attempt in range(max_retries):
            try:
                initialize_default_total()
                print("Successfully initialized default total")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to initialize default total after {max_retries} attempts: {str(e)}")
                print(f"Attempt {attempt + 1}/{max_retries} to initialize default total failed, retrying...")
                time.sleep(2)

        ########################## COLLECT WATER CATEGORY DATA

        # Dictionary to store collected data
        data_frames = {}
        successful_collections = 0

        # Collect data for "Elv"
        df_elv = safely_collect_data(
            "Elv", "//label[contains(text(), 'Elv')]", error_messages
        )
        if df_elv is not None:
            data_frames["Elv"] = df_elv
            successful_collections += 1

        # Collect data for "Innsjø"
        df_innsjo = safely_collect_data(
            "Innsjø", "//label[contains(text(), 'Innsjø')]", error_messages
        )
        if df_innsjo is not None:
            data_frames["Innsjø"] = df_innsjo
            successful_collections += 1

        # Collect data for "Kystvann"
        df_kystvann = safely_collect_data(
            "Kystvann", "//label[contains(text(), 'Kystvann')]", error_messages
        )
        if df_kystvann is not None:
            data_frames["Kystvann"] = df_kystvann
            successful_collections += 1

        # Only proceed with data processing if we collected at least one dataset successfully
        if successful_collections > 0:
            # Combine all the collected dataframes
            df_combined = pd.concat(data_frames.values(), keys=data_frames.keys())
            df_combined.index.names = ['Vannkategori', 'Index']
            
            # Reset index to make 'Vannkategori' a column
            df_combined = df_combined.reset_index()
            df_combined = df_combined.drop('Index', axis=1)
            
            print("\nInitial combined DataFrame:")
            print(df_combined)
            
            # Create the pivoted dataframe
            df_pivoted = pd.DataFrame()
            
            # Process each water category
            categories = ['Elv', 'Innsjø', 'Kystvann']
            for category in categories:
                category_data = df_combined[df_combined['Vannkategori'] == category].copy()
                if not category_data.empty:
                    # Get the percentage values and clean them
                    values = category_data[category].apply(
                        lambda x: float(str(x).replace('%', '').replace(',', '.').strip()) if pd.notnull(x) else 0.0
                    )
                    # Create a dictionary with the values for each condition
                    row_data = {
                        'Kategori': category,
                        'Svært god': values[category_data['Tilstand'] == 'Svært god'].iloc[0],
                        'God': values[category_data['Tilstand'] == 'God'].iloc[0],
                        'Moderat': values[category_data['Tilstand'] == 'Moderat'].iloc[0],
                        'Dårlig': values[category_data['Tilstand'] == 'Dårlig'].iloc[0],
                        'Svært dårlig': values[category_data['Tilstand'] == 'Svært dårlig'].iloc[0]
                    }
                    df_pivoted = pd.concat([df_pivoted, pd.DataFrame([row_data])], ignore_index=True)

            # Ensure the DataFrame is in the exact format requested
            column_order = ['Kategori', 'Svært god', 'God', 'Moderat', 'Dårlig', 'Svært dårlig']
            df_pivoted = df_pivoted[column_order]

            # Round all numeric columns to 1 decimal place
            numeric_columns = ['Svært god', 'God', 'Moderat', 'Dårlig', 'Svært dårlig']
            df_pivoted[numeric_columns] = df_pivoted[numeric_columns].round(1)

            # Sort categories in the desired order
            category_order = {'Elv': 0, 'Innsjø': 1, 'Kystvann': 2}
            df_pivoted['sort_order'] = df_pivoted['Kategori'].map(category_order)
            df_pivoted = df_pivoted.sort_values('sort_order').drop('sort_order', axis=1).reset_index(drop=True)

            print(f"\nFinal pivoted DataFrame:")
            print(df_pivoted.to_string())

            # Call the function and get the "New Data" status
            is_new_data = handle_output_data(
                df_pivoted, 
                FILE_NAME, 
                GITHUB_FOLDER, 
                os.environ.get("TEMP_FOLDER"), 
                keepcsv=True
            )

            if is_new_data:
                print("New data detected and pushed to GitHub.")
            else:
                print("No new data detected.")
        else:
            raise Exception("No data was successfully collected from any water body category")

    except Exception as e:
        error_message = f"An unexpected error occurred during data collection: {str(e)}"
        print(error_message)
        error_messages.append(error_message)

except Exception as e:
    error_message = f"An unexpected error occurred during setup: {str(e)}"
    print(error_message)
    error_messages.append(error_message)

finally:
    try:
        driver.quit()
    except:
        print("Failed to quit driver, it may have already been closed")

    # Send error notifications if any errors occurred
    if error_messages:
        try:
            notify_errors(error_messages, "Vannkvalitet")
        except Exception as e:
            print("Email notifications disabled. Errors in Vannkvalitet were not sent.")