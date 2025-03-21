from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import sys
import os
import glob

### Function to extract data from the water table (kjøres 3 ganger (Elv, Innsjø, Kystvann))


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

    return df


########################## SETTING UP SELENIUM AND OPENING THE WEBSITE

# The selenium package is used to automate web browser interaction from Python.
# Based on html tags, css selectors, xpath, etc., selenium can interact with the elements on a webpage.
# Identify the elements on the webpage and interact with them to extract the required data.

options = Options()
# Uncomment the following line to run in headless mode (without a visible browser window)
# options.add_argument("--headless=new")

# Initialize the WebDriver (ensure the path to the WebDriver executable is correct)
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

# Eks. XPATH for et gitt element: //*[@id="root"]/div/aside/div[2]/div[2]/div/div[1]/span
# // is a syntax shortcut that represents a search for elements at any level in the document hierarchy, starting from the current node (or from the root if used at the beginning of the expression)

########################## CLOSE WELCOME WINDOW

# Attempt to close any welcome modal or window by clicking the "X" button
try:
    close_welcome_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'close')]"))
    )
    close_welcome_button.click()
except:
    print("No welcome window appeared, continuing...")

########################## SELECT TELEMARK COUNTY

#### Click on the "Fylke" filter to expand the options
fylke = driver.find_element(By.XPATH, "//span[normalize-space()='Fylke']")
fylke.click()

##### Select "Telemark" by finding its label and clicking the associated checkbox
telemark_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Telemark')]"))
)
telemark_checkbox_id = telemark_label.get_attribute("for")
telemark_checkbox = driver.find_element(By.ID, telemark_checkbox_id)
driver.execute_script("arguments[0].click();", telemark_checkbox)
# Filters span elements to find one that contains the text "Fylke" (with no leading or trailing whitespace)

########################## SELECT WATER CATEGORIES (River, Lake, Coastal Water)

#### Click on the "Vannkategori" filter to expand the options
vannkategori = driver.find_element(By.XPATH, "//span[normalize-space()='Vannkategori']")
vannkategori.click()

#### Select the checkbox for "Elv" (River)
elv_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Elv')]"))
)
elv_checkbox_id = elv_label.get_attribute("for")
elv_checkbox = driver.find_element(By.ID, elv_checkbox_id)
driver.execute_script("arguments[0].click();", elv_checkbox)

# Collect data for "Elv"
df_elv = collect_water_body_data("Elv")

#### Uncheck "Elv" and check "Innsjø" (Lake)
if elv_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", elv_checkbox)

innsjo_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Innsjø')]"))
)
innsjo_checkbox_id = innsjo_label.get_attribute("for")
innsjo_checkbox = driver.find_element(By.ID, innsjo_checkbox_id)
if not innsjo_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", innsjo_checkbox)

# Collect data for "Innsjø"
df_innsjo = collect_water_body_data("Innsjø")

#### Uncheck "Innsjø" and check "Kystvann" (Coastal Water)
if innsjo_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", innsjo_checkbox)

kystvann_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Kystvann')]"))
)
kystvann_checkbox_id = kystvann_label.get_attribute("for")
kystvann_checkbox = driver.find_element(By.ID, kystvann_checkbox_id)
if not kystvann_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", kystvann_checkbox)

# Collect data for "Kystvann"
df_kystvann = collect_water_body_data("Kystvann")

########################## MERGE AND PROCESS DATAFRAMES

# Merge dataframes for river, lake, and coastal water into a single DataFrame
df = pd.merge(df_elv, df_innsjo, on="Tilstand").merge(df_kystvann, on="Tilstand")

# Replace missing values (represented by "-") with 0
df.replace("-", 0, inplace=True)

# Melt the dataframe to create a long-form DataFrame
df_melted = pd.melt(df, id_vars=["Tilstand"], var_name="Kategori", value_name="Andel")

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

# Clean up numeric columns (remove "%" signs, replace "," with ".", and convert to numeric)
for col in df_pivoted.columns[1:]:
    df_pivoted[col] = df_pivoted[col].str.replace("%", "", regex=False)  # Remove "%"
    df_pivoted[col] = df_pivoted[col].str.replace(
        ",", ".", regex=False
    )  # Replace "," with "."
    df_pivoted[col] = pd.to_numeric(df_pivoted[col])  # Convert to numeric type

# Fill any NaN values with 0 (e.g., missing data points)
df_pivoted.fillna(0, inplace=True)

# Print the final processed DataFrame
print(df_pivoted)

############# SAVE DATAFRAME TO CSV FILE

# Define the CSV file path and save the DataFrame
csv = "økologisk_tilstand_vann.csv"
df_pivoted.to_csv((f"../../../Queries/Temp/{csv}"), index=False)

# Close the WebDriver
driver.quit()
