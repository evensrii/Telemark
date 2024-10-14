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

### Funksjon for å hente ut data fra tabellen for overflatevann


def collect_water_body_data(water_body_name):
    # Locate the table using the summary attribute
    table = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//table[@summary='Økologisk tilstand, overflatevann']")
        )
    )

    # Wait until multiple rows are detected (to ensure the table has been populated fully)
    wait.until(lambda d: len(table.find_elements(By.XPATH, ".//tbody/tr")) > 3)

    # Extract the header names from the second row of <thead> (skip the first row with empty headers)
    header_row = table.find_element(By.XPATH, ".//thead/tr[2]")
    header_names = [
        th.text for th in header_row.find_elements(By.TAG_NAME, "th")[:3]
    ]  # First three headers

    # Extract the data from <tbody> for each row
    data_rows = table.find_elements(By.XPATH, ".//tbody/tr")

    data = []
    for row in data_rows:
        row_header = row.find_element(
            By.TAG_NAME, "th"
        ).text  # Row header (e.g., 'Svært god', 'God')
        first_two_data = [
            td.text for td in row.find_elements(By.TAG_NAME, "td")[:2]
        ]  # First two <td> elements
        data.append(
            [row_header] + first_two_data
        )  # Combine row header with first two data points

    # Create a DataFrame
    df = pd.DataFrame(
        data, columns=["Tilstand"] + header_names[:2]
    )  # Use only relevant headers

    # Delete the second column (if necessary)
    df.drop(df.columns[1], axis=1, inplace=True)

    # Set column names as "Tilstand" and the given water_body_name
    df.columns = ["Tilstand", water_body_name]

    # Remove bottom two rows
    df = df[:-2]

    return df


########################## SETTE OPP SELENIUM OG ÅPNE NETTSIDE

options = Options()
options.add_argument("--headless=new")

# Set up WebDriver (Make sure you have the correct path to your WebDriver)
driver = webdriver.Chrome(options=options)
# driver = webdriver.Chrome()

# Open the webpage
url = "https://vann-nett-klient.miljodirektoratet.no/waterbodies/factsheet/environmental-status"
driver.get(url)

# Wait for the page to load and for the filters to be clickable
wait = WebDriverWait(driver, 15)

########################## AKSEPTERE COOKIES

# Try to click the "Aksepter alle" button if it appears
try:
    accept_all_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[text()='Aksepter alle']"))
    )
    accept_all_button.click()
except:
    print("No cookie banner appeared, continuing...")

########################## LUKKE VELKOMSTVINDU

# Try to close the welcome window by clicking the "X" in the top-right corner
try:
    close_welcome_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'close')]"))
    )
    close_welcome_button.click()
except:
    print("No welcome window appeared, continuing...")

########################## VELGE TELEMARK

#### Klikke på "Fylke"

fylke = driver.find_element(By.XPATH, "//span[normalize-space()='Fylke']")
fylke.click()

##### Klikke på "Telemark"
telemark_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Telemark')]"))
)
telemark_checkbox_id = telemark_label.get_attribute("for")
telemark_checkbox = driver.find_element(By.ID, telemark_checkbox_id)
driver.execute_script("arguments[0].click();", telemark_checkbox)

########################## VELGE VANNELEMENTER (Elv, Innsjø, Kystvann)

#### Klikke på "Vannkategori"

vannkategori = driver.find_element(By.XPATH, "//span[normalize-space()='Vannkategori']")
vannkategori.click()

#### Huke av checkbox "Elv"

# Wait for the "Elv" label and click the checkbox associated with it ("id" is dynamic and changes, but "for" is the same. Use "for" (Elv) to find the checkbox id.)
elv_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Elv')]"))
)
elv_checkbox_id = elv_label.get_attribute("for")
elv_checkbox = driver.find_element(By.ID, elv_checkbox_id)
driver.execute_script("arguments[0].click();", elv_checkbox)

df_elv = collect_water_body_data("Elv")  ### SAMLE DATA FOR ELV

#### Uncheck "Elv", check "Innsjø"
if elv_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", elv_checkbox)

innsjo_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Innsjø')]"))
)
innsjo_checkbox_id = innsjo_label.get_attribute("for")
innsjo_checkbox = driver.find_element(By.ID, innsjo_checkbox_id)
if not innsjo_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", innsjo_checkbox)

df_innsjo = collect_water_body_data("Innsjø")  ### SAMLE DATA FOR INNSJØ

#### Uncheck "Innsjø", check "Kystvann"
if innsjo_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", innsjo_checkbox)

kystvann_label = wait.until(
    EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Kystvann')]"))
)
kystvann_checkbox_id = kystvann_label.get_attribute("for")
kystvann_checkbox = driver.find_element(By.ID, kystvann_checkbox_id)
if not kystvann_checkbox.is_selected():
    driver.execute_script("arguments[0].click();", kystvann_checkbox)

df_kystvann = collect_water_body_data("Kystvann")  ### SAMLE DATA FOR KYSTVANN

########################## SLÅ SAMMEN OG BEHANDLE DATAFRAMENE

# Merge df_elv, df_innsjo, and df_kystvann into one DataFrame
df = pd.merge(df_elv, df_innsjo, on="Tilstand").merge(df_kystvann, on="Tilstand")

# Replace "-" with 0 in the DataFrame
df.replace("-", 0, inplace=True)

# First, melt the dataframe to unpivot the columns ("Elv", "Innsjø", "Kystvann")
df_melted = pd.melt(df, id_vars=["Tilstand"], var_name="Kategori", value_name="Andel")

# Then pivot the table to reorganize it
df_pivoted = df_melted.pivot(
    index="Kategori", columns="Tilstand", values="Andel"
).reset_index()

df_pivoted.columns.name = None  # Remove the multi-index name for columns

df_pivoted = df_pivoted[
    ["Kategori", "Svært god", "God", "Moderat", "Dårlig", "Svært dårlig"]
]

# Remove the "%" sign, replace "," with ".", and convert to numeric
for col in df_pivoted.columns[1:]:  # Skip the first column 'Kategori'
    df_pivoted[col] = df_pivoted[col].str.replace("%", "", regex=False)  # Remove '%'
    df_pivoted[col] = df_pivoted[col].str.replace(
        ",", ".", regex=False
    )  # Replace ',' with '.'
    df_pivoted[col] = pd.to_numeric(df_pivoted[col])  # Convert to numeric

# Replace NaN values with 0
df_pivoted.fillna(0, inplace=True)

############# Save dfs as a csv files

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
