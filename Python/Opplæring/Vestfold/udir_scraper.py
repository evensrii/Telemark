from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import pandas as pd
import time
import os
from pathlib import Path

class UdirScraper:
    def __init__(self):
        self.url = "https://www.udir.no/tall-og-forskning/statistikk/statistikk-fag-og-yrkesopplaring/antall-larlinger/larekontrakter-utdanningsprogram/"
        self.driver = None
        self.download_dir = str(Path.home() / "Downloads")

    def start_browser(self):
        """Initialize the Chrome WebDriver"""
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        self.driver = webdriver.Chrome(options=options)
        self.driver.get(self.url)
        self.driver.set_window_size(1200, 800)
        time.sleep(2)  # Wait for page to load
        self.handle_popups()

    def safe_click(self, element, element_name=""):
        """Try multiple methods to click an element safely"""
        try:
            # Scroll element into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(1)
            
            # Try regular click
            try:
                element.click()
                print(f"Clicked {element_name} normally")
                return True
            except ElementClickInterceptedException:
                print(f"Regular click failed for {element_name}, trying JavaScript click...")
                
            # Try JavaScript click
            self.driver.execute_script("arguments[0].click();", element)
            print(f"Clicked {element_name} using JavaScript")
            return True
            
        except Exception as e:
            print(f"Failed to click {element_name}: {str(e)}")
            return False

    def handle_popups(self):
        """Handle any popup boxes that appear when the page loads"""
        try:
            # Wait for cookie banner to be present and visible
            cookie_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "submitAllCategoriesButton"))
            )
            print("Found cookie accept button...")
            
            if self.safe_click(cookie_button, "cookie accept button"):
                time.sleep(3)  # Wait longer for popup to fully disappear

            # Handle the collapse icon in the top right
            try:
                collapse_button = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="icon-collapse"]'))
                )
                print("Found collapse button...")
                if self.safe_click(collapse_button, "collapse button"):
                    time.sleep(2)
            except Exception as e:
                print(f"Note: Could not find or click collapse button: {str(e)}")
            
            # Check for any remaining overlays and try to close them
            try:
                overlays = self.driver.find_elements(By.CLASS_NAME, "close")
                for overlay in overlays:
                    if overlay.is_displayed():
                        self.safe_click(overlay, "close button")
                        time.sleep(1)
            except Exception as e:
                print(f"Note: Error handling overlays: {str(e)}")

        except Exception as e:
            print(f"Warning: Could not handle popups: {str(e)}")

    def close_browser(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()

    def click_kontraktstype(self):
        """Click the Kontraktstype button"""
        try:
            # Wait for any overlays to disappear
            time.sleep(3)
            
            # Try to find the Kontraktstype button
            print("Looking for Kontraktstype button...")
            xpath = "//button[normalize-space(.)='Kontraktstype']"
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            
            if self.safe_click(element, "Kontraktstype button"):
                time.sleep(1)  # Wait for any animations
                return True
            return False
            
        except TimeoutException:
            print("Could not find Kontraktstype button")
            return False

    def get_table_data(self):
        """Export table data and return as a pandas DataFrame"""
        try:
            print("Looking for export button...")
            # Try multiple selectors to find the export button
            selectors = [
                "//button[.//span[contains(text(), 'Eksporter')]]",  # Button with span containing text
                "//button[contains(@class, 'header-action')]//span[contains(text(), 'Eksporter')]/..",  # Button via span and class
                "//button[.//svg[@class='akselicon']/following-sibling::span[contains(text(), 'Eksporter')]]"  # Button with icon and text
            ]
            
            export_button = None
            for selector in selectors:
                try:
                    export_button = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    if export_button:
                        print(f"Found export button using selector: {selector}")
                        break
                except:
                    continue
            
            if not export_button:
                raise Exception("Could not find export button with any selector")

            # Get list of CSV files before clicking
            before_files = set(Path(self.download_dir).glob("*VGO.Laerekontrakter.csv"))
            
            # Click the export button
            if not self.safe_click(export_button, "export button"):
                raise Exception("Failed to click export button")

            # Wait for new file to appear (max 10 seconds)
            max_wait = 10
            start_time = time.time()
            new_file = None
            
            while time.time() - start_time < max_wait:
                # Get current files
                current_files = set(Path(self.download_dir).glob("*VGO.Laerekontrakter.csv"))
                # Find new files
                new_files = current_files - before_files
                
                if new_files:
                    new_file = new_files.pop()  # Get the first new file
                    break
                    
                time.sleep(0.5)
            
            if not new_file:
                raise Exception("No new CSV file was downloaded")
                
            # Read the CSV file
            df = pd.read_csv(str(new_file), sep='\t', encoding='utf-8')
            print("Successfully read exported data")
            print(f"Found {len(df)} rows")
            
            return df
            
        except Exception as e:
            print(f"Error getting exported data: {str(e)}")
            return None

def main():
    scraper = UdirScraper()
    try:
        scraper.start_browser()
        # Try to click the Kontraktstype button
        if scraper.click_kontraktstype():
            # Get the table data
            df = scraper.get_table_data()
            if df is not None:
                print("\nData Preview:")
                print(df.head())
                # Save to CSV
                df.to_csv('udir_data.csv', index=False, sep='\t')
                print("\nData saved to 'udir_data.csv'")
    finally:
        scraper.close_browser()

if __name__ == "__main__":
    main()
