from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Set up Selenium WebDriver (make sure chromedriver is installed and PATH is set)
driver = webdriver.Chrome()

# Open the NVE Solkraft page with the embedded Power BI report
driver.get("https://www.nve.no/energi/energisystem/solkraft/oversikt-over-solkraftanlegg-i-norge/")

time.sleep(2)  # Let the page load

# --- Accept cookies if present ---
try:
    # Try the most common selectors for cookie consent buttons
    cookie_selectors = [
        (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'tillat alle')]") ,
        (By.XPATH, "//button[contains(., 'Tillat alle')]") ,
        (By.XPATH, "//button[contains(., 'Godta')]") ,
        (By.XPATH, "//button[contains(., 'Aksepter')]") ,
        (By.CSS_SELECTOR, "button#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
    ]
    cookie_button = None
    for by, selector in cookie_selectors:
        try:
            cookie_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((by, selector))
            )
            if cookie_button:
                cookie_button.click()
                print("Accepted cookies.")
                time.sleep(1)
                break
        except Exception:
            continue
    if not cookie_button:
        print("No cookie consent button found or already accepted.")
except Exception as e:
    print("Error while accepting cookies:", e)

time.sleep(1)

# --- Switch to the Power BI iframe by index (iframes[1]) ---
WebDriverWait(driver, 30).until(lambda d: len(d.find_elements(By.TAG_NAME, "iframe")) > 1)
iframes = driver.find_elements(By.TAG_NAME, "iframe")

# Safety check: ensure the second iframe is the Power BI report
powerbi_iframe = None
for iframe in iframes:
    src = iframe.get_attribute('src')
    if src and 'reportId=661cbef9-69d1-4b3c-bdf6-42ea13e9986c' in src:
        powerbi_iframe = iframe
        break
if not powerbi_iframe:
    raise RuntimeError("Could not find Power BI iframe with correct reportId.")

# Switch to the Power BI iframe
try:
    driver.switch_to.frame(powerbi_iframe)
    print("Switched to Power BI iframe.")
except Exception as e:
    print("Could not switch to Power BI iframe:", e)
    driver.quit()
    raise

time.sleep(2)  # Let the Power BI report load

# --- Focus the Power BI report using JS and send keys to <body> or <html> ---
try:
    driver.execute_script("window.focus();")
    print("Focused Power BI iframe window via JS.")
except Exception as e:
    print("Could not focus Power BI iframe via JS:", e)

# --- Keyboard navigation: Press Tab 15 times, then Enter ---
from selenium.webdriver.common.keys import Keys

try:
    # Try to send keys to <body>, if not, try <html>
    try:
        elem = driver.find_element(By.TAG_NAME, "body")
    except Exception:
        elem = driver.find_element(By.TAG_NAME, "html")
    # First, send Ctrl+F6
    elem.send_keys(Keys.CONTROL, Keys.F6)
    print("Sent Ctrl+F6 to iframe.")
    time.sleep(0.5)
    # Then Tab 13 times
    for i in range(16):
        elem.send_keys(Keys.TAB)
        time.sleep(0.1)
    print("Sent 13 Tabs to iframe.")
    time.sleep(2)
except Exception as e:
    print("Could not send keyboard navigation:", e)

# ---
# Notes:
# - You may need to configure Chrome options to set the download directory and handle file downloads automatically.
# - Inspect the Power BI iframe for exact selectors, as they may change over time.
# - For more robust automation, consider using Playwright (supports more modern web features).
