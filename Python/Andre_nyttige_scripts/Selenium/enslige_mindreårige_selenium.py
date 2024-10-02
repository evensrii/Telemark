import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd


### DETTE SCRIPTET NAVIGERER SELV RUNDT PÅ NETTSIDEN OG HENTER UT DATA FRA GRAFEN ###


# Function to get the shadow root
def get_shadow_root(driver, element):
    return driver.execute_script("return arguments[0].shadowRoot", element)


# Function to find an element inside a shadow DOM
def find_element_in_shadow_dom(driver, shadow_host, selector):
    shadow_root = get_shadow_root(driver, shadow_host)
    return shadow_root.find_element(By.CSS_SELECTOR, selector)


# Function to find multiple elements inside a shadow DOM
def find_elements_in_shadow_dom(driver, shadow_host, selector):
    shadow_root = get_shadow_root(driver, shadow_host)
    return shadow_root.find_elements(By.CSS_SELECTOR, selector)


# Function to type in the input field with full event dispatching
def type_in_input_field(driver, input_element, text):
    for char in text:
        input_element.send_keys(char)
        time.sleep(0.2)  # Slight delay between keystrokes to mimic human typing


def main():
    print("Initializing Chrome Options...")
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1200,800")  # Smaller window size

    print("Starting WebDriver...")
    driver = webdriver.Chrome(options=chrome_options)

    # Define the lists for kommuner and years
    kommuner = ["Skien", "Porsgrunn"]
    years = ["2021", "2022"]

    all_data = []  # List to store all the data

    try:
        print("Navigating to the webpage...")
        driver.get(
            "https://www.imdi.no/om-integrering-i-norge/statistikk/F00/bosetting"
        )

        wait = WebDriverWait(driver, 20)

        # Step 1: Click the expand button to reveal the chart
        print("Waiting for expand button to be clickable...")
        expand_button = wait.until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "#toggle-list-chart_17320_15183_7 > div:nth-child(1) > button",
                )
            )
        )
        expand_button.click()
        print("Expand button clicked.")

        for selected_kommune in kommuner:
            try:
                print(f"\nProcessing data for {selected_kommune}")

                # Step 2: Locate the shadow host element
                print("Waiting for shadow host to be located...")
                shadow_host = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "#chart_17320_15183_7 > div > div > div:nth-child(1) > div > figure > div > div",
                        )
                    )
                )
                print("Shadow host located.")

                # Step 3: Access the 'Norge' button inside the shadow DOM and click it
                norge_button = find_element_in_shadow_dom(
                    driver, shadow_host, "button.radio-select__selected-container"
                )
                norge_button.click()
                print("'Norge' button clicked.")

                # Retry mechanism for accessing the input field
                retry_attempts = 3
                input_field = None
                for attempt in range(retry_attempts):
                    try:
                        # Re-locate the shadow host element before accessing the input field
                        shadow_host = wait.until(
                            EC.presence_of_element_located(
                                (
                                    By.CSS_SELECTOR,
                                    "#chart_17320_15183_7 > div > div > div:nth-child(1) > div > figure > div > div",
                                )
                            )
                        )

                        # Step 4: Access the input field inside the shadow DOM
                        input_field = find_element_in_shadow_dom(
                            driver, shadow_host, 'input[type="text"]'
                        )
                        input_field.click()
                        print("Input field focused and clicked.")
                        break
                    except Exception as e:
                        print(f"Attempt {attempt + 1} failed: {e}")
                        time.sleep(2)  # Wait before retrying

                if not input_field:
                    raise Exception("Failed to locate the input field after retries.")

                # Step 5: Type the selected kommune into the input field
                type_in_input_field(driver, input_field, selected_kommune)
                print(f"'{selected_kommune}' typed into input field.")

                # Step 6: Wait for the dropdown suggestions to appear
                suggestion_items = find_elements_in_shadow_dom(
                    driver, shadow_host, 'li[role="option"]'
                )
                print("Suggestion items located.")

                # Find the exact kommune option
                kommune_option = None
                for item in suggestion_items:
                    if item.text.strip() == f"{selected_kommune}, kommune":
                        kommune_option = item
                        break

                # If the kommune option was found, click it
                if kommune_option:
                    kommune_option.click()
                    print(f"'{selected_kommune}, kommune' option selected.")
                else:
                    print(f"'{selected_kommune}, kommune' option not found.")

                # Optional: Wait for the page to load data after selection
                time.sleep(3)

                # Now iterate over the years for the selected kommune
                for selected_year in years:
                    try:
                        print(
                            f"\nProcessing data for {selected_kommune} in {selected_year}"
                        )

                        # Step 7: Re-locate the shadow host element
                        shadow_host = wait.until(
                            EC.presence_of_element_located(
                                (
                                    By.CSS_SELECTOR,
                                    "#chart_17320_15183_7 > div > div > div:nth-child(1) > div > figure > div > div",
                                )
                            )
                        )

                        # Step 8: Access the 'År' dropdown inside the shadow DOM and click it
                        year_dropdown = find_element_in_shadow_dom(
                            driver, shadow_host, "button.search-select__input"
                        )
                        year_dropdown.click()
                        print("'År' dropdown clicked.")

                        # Step 9: Wait for the dropdown options to appear and select the year
                        dropdown_options = find_elements_in_shadow_dom(
                            driver,
                            shadow_host,
                            "button.search-select__option-label-wrapper",
                        )

                        for option in dropdown_options:
                            span_element = option.find_element(
                                By.CSS_SELECTOR, "span.search-select__option-label"
                            )
                            if span_element.text.strip() == selected_year:
                                option.click()
                                print(f"Year '{selected_year}' selected.")
                                break

                        time.sleep(3)  # Wait for the page to load data after selection

                        # Step 10: Re-locate the shadow host element before accessing the rect elements
                        shadow_host = wait.until(
                            EC.presence_of_element_located(
                                (
                                    By.CSS_SELECTOR,
                                    "#chart_17320_15183_7 > div > div > div:nth-child(1) > div > figure > div > div",
                                )
                            )
                        )

                        # Step 11: Locate the 'rect' elements inside the shadow DOM containing the data
                        rects = find_elements_in_shadow_dom(
                            driver, shadow_host, 'g.highcharts-series rect[role="img"]'
                        )
                        print(f"Found {len(rects)} rect elements.")

                        # Step 12: Extract numbers and their corresponding labels
                        labels_and_numbers = []
                        for rect in rects:
                            aria_label = rect.get_attribute("aria-label")
                            print(f"aria-label found: {aria_label}")  # Debugging line

                            if aria_label:
                                parts = aria_label.split(", ")

                                # Make sure there are enough parts and they are correctly structured
                                if len(parts) > 1:
                                    label = parts[0]
                                    number_part = parts[1].split()[0]

                                    # Remove any preceding "." or other non-numeric characters
                                    cleaned_number = number_part.lstrip(".").replace(
                                        ".", ""
                                    )

                                    # Check if the cleaned part is numeric
                                    if cleaned_number.isdigit():
                                        number = cleaned_number
                                    else:
                                        number = "0"
                                else:
                                    label = parts[0]
                                    number = "0"

                                labels_and_numbers.append((label, number))

                        # Create a dictionary with default values of "0" for the categories
                        data_dict = {
                            "Anmodning om bosetting": "0",
                            "Faktisk bosetting": "0",
                        }

                        # Update the dictionary with actual values found
                        for label, number in labels_and_numbers:
                            if label in data_dict:
                                data_dict[label] = number

                        # Append the results to the all_data list
                        all_data.extend(
                            [
                                (label, number, selected_year, selected_kommune)
                                for label, number in data_dict.items()
                            ]
                        )

                    except Exception as e:
                        print(
                            f"An error occurred for {selected_kommune} in {selected_year}: {e}"
                        )

            except Exception as e:
                print(f"An error occurred for {selected_kommune}: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Create a pandas DataFrame from the collected data
        df = pd.DataFrame(all_data, columns=["Kategori", "Antall", "År", "Kommune"])

        print("Final DataFrame created:")
        print(df)

        print("Script completed, the browser will remain open.")
        input("Press Enter to close the browser...")
        driver.quit()


if __name__ == "__main__":
    main()
