import webbrowser
import pyautogui
import pytesseract
from PIL import Image
import pygetwindow as gw
import time

# Optional: Uncomment and update if tesseract is not in PATH
# pytesseract.pytesseract.tesseract_cmd = r'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'

import ctypes
ctypes.windll.user32.SetProcessDPIAware()
print(f"Screen size: {pyautogui.size()}")
print(f"Window box: {window.box}")  # should be (left, top, width, height)

border_left = window.left if window.left >= 0 else 0
adjusted_left = border_left + offset_x

# Step 1: Open the NVE Solkraft webpage
url = "https://www.nve.no/energi/energisystem/solkraft/oversikt-over-solkraftanlegg-i-norge/"
print(f"Opening {url} in your default browser...")
webbrowser.open(url)

# Step 2: Wait for the page to load and user to maximize browser
print("Please maximize your browser and ensure the Power BI report is visible.")
time.sleep(15)  # Increased to 10 seconds to allow Power BI to load

# Step 3: Try to find the browser window and take a region screenshot
window = None
for w in gw.getAllTitles():
    if "solkraftanlegg" in w.lower() or "nve" in w.lower():
        window = gw.getWindowsWithTitle(w)[0]
        break

if window is None:
    print("Could not find the browser window. Taking a full screenshot instead.")
    screenshot = pyautogui.screenshot()
    left, top = 0, 0  # for fallback click logic
else:
    window.activate()
    time.sleep(0.5)
    left, top, width, height = window.left, window.top, window.width, window.height
    print(f"Capturing region: left={left}, top={top}, width={width}, height={height}")
    screenshot = pyautogui.screenshot(region=(left, top, width, height))
# Do NOT crop for the first screenshot
crop_offset_y = 0

# Step 4: Preprocess the image for better OCR (grayscale, invert, enhance contrast)
gray = screenshot.convert('L')
inverted = Image.eval(gray, lambda x: 255 - x)
from PIL import ImageEnhance
inverted = ImageEnhance.Contrast(inverted).enhance(2.0)
inverted.save("ocr_debug_inverted.png")  # For debugging

# Use OCR on the preprocessed image
custom_config = r'--psm 6'
data = pytesseract.image_to_data(inverted, output_type=pytesseract.Output.DICT, config=custom_config)

# Step 5: Click sequence: Datagrunnlag -> Alle (Fylke, Kommune) -> Telemark

def ocr_and_click(target_keyword, screenshot, window, left=0, top=0, debug_prefix="", crop_offset_y=0):
    gray = screenshot.convert('L')
    inverted = Image.eval(gray, lambda x: 255 - x)
    from PIL import ImageEnhance
    inverted = ImageEnhance.Contrast(inverted).enhance(2.0)
    debug_img = f"ocr_debug_{debug_prefix}{target_keyword}.png"
    inverted.save(debug_img)
    custom_config = r'--psm 6'
    data = pytesseract.image_to_data(inverted, output_type=pytesseract.Output.DICT, config=custom_config)
    for i, word in enumerate(data['text']):
        if word.strip().lower() == target_keyword.lower():
            x = data['left'][i] + data['width'][i] // 2
            y = data['top'][i] + data['height'][i] // 2
            print(f"Found '{target_keyword}' at ({x}, {y}), clicking...")
            if window is not None:
                abs_x = left + x + crop_offset_x
                abs_y = top + y + crop_offset_y
            else:
                abs_x = x + crop_offset_x
                abs_y = y + crop_offset_y
            print(f"[DEBUG] Clicking at absolute position: ({abs_x}, {abs_y})")
            pyautogui.moveTo(abs_x, abs_y, duration=0.5)
            pyautogui.click()
            return True
    print(f"Keyword '{target_keyword}' not found. Debug image saved as {debug_img}")
    return False

def ocr_and_click_flexible(target_keyword, screenshot, window, left=0, top=0, debug_prefix="", crop_offset_y=0, crop_offset_x=0):
    # First try with preprocessing
    gray = screenshot.convert('L')
    inverted = Image.eval(gray, lambda x: 255 - x)
    from PIL import ImageEnhance
    inverted = ImageEnhance.Contrast(inverted).enhance(2.0)
    debug_img_inv = f"ocr_debug_{debug_prefix}{target_keyword}_inv.png"
    inverted.save(debug_img_inv)
    custom_config = r'--psm 6'
    data = pytesseract.image_to_data(inverted, output_type=pytesseract.Output.DICT, config=custom_config)
    for i, word in enumerate(data['text']):
        if word.strip().lower() == target_keyword.lower():
            x = data['left'][i] + data['width'][i] // 2
            y = data['top'][i] + data['height'][i] // 2
            print(f"[INV] Found '{target_keyword}' at ({x}, {y}), clicking...")
            if window is not None:
                abs_x = left + x + crop_offset_x
                abs_y = top + y + crop_offset_y
            else:
                abs_x = x + crop_offset_x
                abs_y = y + crop_offset_y
            print(f"[DEBUG] Clicking at absolute position: ({abs_x}, {abs_y})")
            pyautogui.moveTo(abs_x, abs_y, duration=0.5)
            pyautogui.click()
            return True
    # Then try raw image
    debug_img_raw = f"ocr_debug_{debug_prefix}{target_keyword}_raw.png"
    screenshot.save(debug_img_raw)
    data_raw = pytesseract.image_to_data(screenshot, output_type=pytesseract.Output.DICT, config=custom_config)
    for i, word in enumerate(data_raw['text']):
        if word.strip().lower() == target_keyword.lower():
            x = data_raw['left'][i] + data_raw['width'][i] // 2
            y = data_raw['top'][i] + data_raw['height'][i] // 2
            print(f"[RAW] Found '{target_keyword}' at ({x}, {y}), clicking...")
            if window is not None:
                abs_x = left + x + crop_offset_x
                abs_y = top + y + crop_offset_y
            else:
                abs_x = x + crop_offset_x
                abs_y = y + crop_offset_y
            print(f"[DEBUG] Clicking at absolute position: ({abs_x}, {abs_y})")
            pyautogui.moveTo(abs_x, abs_y, duration=0.5)
            pyautogui.click()
            return True
    print(f"Keyword '{target_keyword}' not found. Debug images saved as {debug_img_inv} and {debug_img_raw}")
    return False

# Crop to x=590:750, y=750:1100 before searching for 'Datagrunnlag'
x1, x2 = 590, 750
y1, y2 = 750, 1100
screenshot1_cropped = screenshot.crop((x1, y1, x2, y2))
screenshot1_cropped.save("ocr_debug_step1_Datagrunnlag_crop.png")

clicked = False
for attempt in range(5):
    print(f"Attempt {attempt+1} to find 'Datagrunnlag'...")
    clicked = ocr_and_click_flexible("Datagrunnlag", screenshot1_cropped, window, left, top, debug_prefix="step1_", crop_offset_x=x1, crop_offset_y=y1)
    if clicked:
        break
    time.sleep(0.7)
if not clicked:
    print("WARNING: Could not find 'Datagrunnlag' after 5 attempts!")
time.sleep(1)

# 2. Take a new screenshot and click 'Alle' (Fylke, Kommune) (crop top 200px, leftmost 875px)
if clicked:
    if window is not None:
        screenshot2 = pyautogui.screenshot(region=(left, top, width, height))
        screenshot2 = screenshot2.crop((0, 200, width, height))
    else:
        screenshot2 = pyautogui.screenshot()
        width2, height2 = screenshot2.size
        screenshot2 = screenshot2.crop((0, 200, width2, height2))
    # For the first 'Alle' (Fylke, Kommune), crop tightly to the region x=743:890, y=445:486 (after 200px top crop)
    alle_x1, alle_x2 = 743, 890
    alle_y1, alle_y2 = 445, 486  # 645-200, 686-200
    screenshot2_cropped = screenshot2.crop((alle_x1, alle_y1, alle_x2, alle_y2))
    screenshot2_cropped.save("ocr_debug_step2_alle_crop.png")
    def ocr_and_click_raw(target_keyword, screenshot, window, left=0, top=0, debug_prefix="", crop_offset_y=0, crop_offset_x=0):
        debug_img_raw = f"ocr_debug_{debug_prefix}{target_keyword}_raw.png"
        screenshot.save(debug_img_raw)
        custom_config = r'--psm 6'
        data_raw = pytesseract.image_to_data(screenshot, output_type=pytesseract.Output.DICT, config=custom_config)
        alle_positions = []
        for i, word in enumerate(data_raw['text']):
            if word.strip().lower() == target_keyword.lower():
                x = data_raw['left'][i] + data_raw['width'][i] // 2
                y = data_raw['top'][i] + data_raw['height'][i] // 2
                alle_positions.append((i, x, y))
        if alle_positions:
            print(f"[RAW] Found '{target_keyword}' at these positions (index, x, y): {alle_positions}")
            # Default: click the first one as before
            i, x, y = alle_positions[0]
            if window is not None:
                abs_x = left + x + crop_offset_x
                abs_y = top + y + crop_offset_y
            else:
                abs_x = x + crop_offset_x
                abs_y = y + crop_offset_y
            print(f"[DEBUG] Clicking at absolute position: ({abs_x}, {abs_y})")
            pyautogui.moveTo(abs_x, abs_y, duration=0.5)
            pyautogui.click()
            return (x, y)
        print(f"Keyword '{target_keyword}' not found. Debug image saved as {debug_img_raw}")
        return None
    # Find all 'Alle' positions and click the one at y=543 (Fylke, Kommune)
    def ocr_and_click_alle_at_y(target_keyword, screenshot, window, y_target, left=0, top=0, debug_prefix="", crop_offset_y=0, crop_offset_x=0, tol=10):
        debug_img_raw = f"ocr_debug_{debug_prefix}{target_keyword}_raw.png"
        screenshot.save(debug_img_raw)
        custom_config = r'--psm 6'
        data_raw = pytesseract.image_to_data(screenshot, output_type=pytesseract.Output.DICT, config=custom_config)
        alle_positions = []
        # Debug print of all OCR words and their coordinates
        all_words_debug = []
        for i, word in enumerate(data_raw['text']):
            x = data_raw['left'][i] + data_raw['width'][i] // 2
            y = data_raw['top'][i] + data_raw['height'][i] // 2
            all_words_debug.append((i, word.strip(), x, y))
            if word.strip().lower() == target_keyword.lower():
                alle_positions.append((i, x, y))
        print(f"[RAW][DEBUG] All OCR words in slicer crop:")
        for entry in all_words_debug:
            print(f"  idx={entry[0]}, word='{entry[1]}', x={entry[2]}, y={entry[3]}")
        print(f"[RAW] Found '{target_keyword}' at these positions (index, x, y): {alle_positions}")
        for i, x, y in alle_positions:
            if abs(y - y_target) <= tol:
                print(f"Clicking '{target_keyword}' at y={y} (target={y_target})")
                if window is not None:
                    pyautogui.moveTo(left + x + crop_offset_x, top + y + crop_offset_y, duration=0.5)
                else:
                    pyautogui.moveTo(x + crop_offset_x, y + crop_offset_y, duration=0.5)
                pyautogui.click()
                return (x, y)
        print(f"Keyword '{target_keyword}' not found at y~={y_target}. Debug image saved as {debug_img_raw}")
        return None
    alle_coords = ocr_and_click_alle_at_y("Alle", screenshot2_cropped, window, y_target=21, left=left, top=top, debug_prefix="step2_", crop_offset_y=645, crop_offset_x=743, tol=10)
    time.sleep(1)
else:
    alle_coords = None

# 3. Take a new screenshot and click 'Telemark' if 'Alle' was clicked (crop top 200px, leftmost 875px)
if alle_coords:
    if window is not None:
        screenshot3 = pyautogui.screenshot(region=(left, top, width, height))
        screenshot3 = screenshot3.crop((0, 200, width, height))
    else:
        screenshot3 = pyautogui.screenshot()
        width3, height3 = screenshot3.size
        screenshot3 = screenshot3.crop((0, 200, width3, height3))
    screenshot3_cropped = screenshot3.crop((0, 0, 875, screenshot3.size[1]))
    screenshot3_cropped.save("ocr_debug_step3_slicer_crop.png")
    def ocr_and_click_slicer(target_keyword, screenshot, window, left=0, top=0, debug_prefix="", crop_offset_y=0, crop_offset_x=0):
        debug_img = f"ocr_debug_{debug_prefix}{target_keyword}_cropped.png"
        screenshot.save(debug_img)
        custom_config = r'--psm 6'
        data = pytesseract.image_to_data(screenshot, output_type=pytesseract.Output.DICT, config=custom_config)
        for i, word in enumerate(data['text']):
            if word.strip().lower() == target_keyword.lower():
                x = data['left'][i] + data['width'][i] // 2
                y = data['top'][i] + data['height'][i] // 2
                print(f"[SLICER] Found '{target_keyword}' at ({x}, {y}), clicking...")
                if window is not None:
                    pyautogui.moveTo(left + x + crop_offset_x, top + y + crop_offset_y, duration=0.5)
                else:
                    pyautogui.moveTo(x + crop_offset_x, y + crop_offset_y, duration=0.5)
                pyautogui.click()
                return (x, y)
        print(f"Keyword '{target_keyword}' not found in slicer crop. Debug image saved as {debug_img}")
        return None
    telemark_coords = ocr_and_click_slicer("Telemark", screenshot3_cropped, window, left, top, debug_prefix="step3_", crop_offset_y=200, crop_offset_x=0)
    # 4. Exit the slicer by clicking 40 pixels further down from last click
    if telemark_coords:
        x, y = telemark_coords
        if window is not None:
            pyautogui.moveTo(left + x + 0, top + y + 40 + 200, duration=0.5)
        else:
            pyautogui.moveTo(x + 0, y + 40 + 200, duration=0.5)
        pyautogui.click()
        time.sleep(1)
        # 5. Take a new screenshot and click 'Alle' in the År slicer (by y=761)
        if window is not None:
            screenshot4 = pyautogui.screenshot(region=(left, top, width, height))
            screenshot4 = screenshot4.crop((0, 200, width, height))
        else:
            screenshot4 = pyautogui.screenshot()
            width4, height4 = screenshot4.size
            screenshot4 = screenshot4.crop((0, 200, width4, height4))
        screenshot4_cropped = screenshot4.crop((0, 0, 875, screenshot4.size[1]))
        screenshot4_cropped.save("ocr_debug_step4_slicer_crop.png")
        aar_coords = ocr_and_click_alle_at_y("Alle", screenshot4_cropped, window, y_target=761, left=left, top=top, debug_prefix="step4_", crop_offset_y=200, crop_offset_x=0, tol=10)
        time.sleep(1)
        # 6. Take a new screenshot and click the latest year
        if aar_coords:
            if window is not None:
                screenshot5 = pyautogui.screenshot(region=(left, top, width, height))
                screenshot5 = screenshot5.crop((0, 200, width, height))
            else:
                screenshot5 = pyautogui.screenshot()
                width5, height5 = screenshot5.size
                screenshot5 = screenshot5.crop((0, 200, width5, height5))
            # Find all year-like words (4 digits)
            debug_img_years = "ocr_debug_step5_years.png"
            screenshot5.save(debug_img_years)
            custom_config = r'--psm 6'
            data_years = pytesseract.image_to_data(screenshot5, output_type=pytesseract.Output.DICT, config=custom_config)
            years = []
            for i, word in enumerate(data_years['text']):
                if word.strip().isdigit() and len(word.strip()) == 4:
                    years.append((int(word.strip()), i))
            if years:
                years.sort(reverse=True)
                latest_year, idx = years[0]
                x = data_years['left'][idx] + data_years['width'][idx] // 2
                y = data_years['top'][idx] + data_years['height'][idx] // 2
                print(f"Clicking latest year: {latest_year} at ({x}, {y})")
                if window is not None:
                    pyautogui.moveTo(left + x, top + y + 200, duration=0.5)
                else:
                    pyautogui.moveTo(x, y + 200, duration=0.5)
                pyautogui.click()
                print(f"Selected year: {latest_year}")
                # Verification step: take new screenshot and check if latest_year is now shown in År dropdown
                time.sleep(1)
                if window is not None:
                    screenshot6 = pyautogui.screenshot(region=(left, top, width, height))
                    screenshot6 = screenshot6.crop((0, 200, width, height))
                else:
                    screenshot6 = pyautogui.screenshot()
                    width6, height6 = screenshot6.size
                    screenshot6 = screenshot6.crop((0, 200, width6, height6))
                debug_img_verify = "ocr_debug_step6_verify.png"
                screenshot6.save(debug_img_verify)
                data_verify = pytesseract.image_to_data(screenshot6, output_type=pytesseract.Output.DICT, config=custom_config)
                found_year = False
                for word in data_verify['text']:
                    if str(latest_year) == word.strip():
                        found_year = True
                        break
                if found_year:
                    print(f"Verification: Year {latest_year} is now selected in År dropdown.")
                else:
                    print(f"WARNING: Year {latest_year} was clicked but is not shown as selected in År dropdown!")
            else:
                print("No years found!")
        # 5. Take a new screenshot and click 'Alle' in the År slicer (use dual OCR)
        if window is not None:
            screenshot4 = pyautogui.screenshot(region=(left, top, width, height))
            screenshot4 = screenshot4.crop((0, 200, width, height))
        else:
            screenshot4 = pyautogui.screenshot()
            width4, height4 = screenshot4.size
            screenshot4 = screenshot4.crop((0, 200, width4, height4))
        # Use only RAW OCR for År slicer
        def ocr_and_click_raw(target_keyword, screenshot, window, left=0, top=0, debug_prefix="", crop_offset_y=0):
            debug_img_raw = f"ocr_debug_{debug_prefix}{target_keyword}_raw.png"
            screenshot.save(debug_img_raw)
            custom_config = r'--psm 6'
            data_raw = pytesseract.image_to_data(screenshot, output_type=pytesseract.Output.DICT, config=custom_config)
            for i, word in enumerate(data_raw['text']):
                if word.strip().lower() == target_keyword.lower():
                    x = data_raw['left'][i] + data_raw['width'][i] // 2
                    y = data_raw['top'][i] + data_raw['height'][i] // 2
                    print(f"[RAW] Found '{target_keyword}' at ({x}, {y}), clicking...")
                    if window is not None:
                        pyautogui.moveTo(left + x, top + y + crop_offset_y, duration=0.5)
                    else:
                        pyautogui.moveTo(x, y + crop_offset_y, duration=0.5)
                    pyautogui.click()
                    return (x, y)
            print(f"Keyword '{target_keyword}' not found. Debug image saved as {debug_img_raw}")
            return None
        aar_coords = ocr_and_click_raw("Alle", screenshot4, window, left, top, debug_prefix="step4_", crop_offset_y=200)
        time.sleep(1)
        
# 6. Take a new screenshot and click the latest year
if aar_coords:
    if window is not None:
        screenshot5 = pyautogui.screenshot(region=(left, top, width, height))
        screenshot5 = screenshot5.crop((0, 200, width, height))
    else:
        screenshot5 = pyautogui.screenshot()
        width5, height5 = screenshot5.size
        screenshot5 = screenshot5.crop((0, 200, width5, height5))

    # CROP to year dropdown region only (x=740:890, y=447:676)
    screenshot5 = screenshot5.crop((740, 447, 890, 676))
    print("[DEBUG] Cropped screenshot5 to year slicer region (740, 447, 890, 676)")

    # Find all year-like words (4 digits)
    debug_img_years = "ocr_debug_step5_years.png"
    screenshot5.save(debug_img_years)
    custom_config = r'--psm 6'
    data_years = pytesseract.image_to_data(screenshot5, output_type=pytesseract.Output.DICT, config=custom_config)
    years = []
    for i, word in enumerate(data_years['text']):
        if word.strip().isdigit() and len(word.strip()) == 4:
            years.append((int(word.strip()), i))
    if years:
        years.sort(reverse=True)
        latest_year, idx = years[0]
        x = data_years['left'][idx] + data_years['width'][idx] // 2
        y = data_years['top'][idx] + data_years['height'][idx] // 2
        print(f"Clicking latest year: {latest_year} at ({x}, {y})")
        if window is not None:
            pyautogui.moveTo(left + 740 + x, top + 200 + 447 + y, duration=0.5)
        else:
            pyautogui.moveTo(740 + x, 200 + 447 + y, duration=0.5)
        pyautogui.click()
        print(f"Selected year: {latest_year}")
        # Verification step: take new screenshot and check if latest_year is now shown in År dropdown
        time.sleep(1)
        if window is not None:
            screenshot6 = pyautogui.screenshot(region=(left, top, width, height))
            screenshot6 = screenshot6.crop((0, 200, width, height))
        else:
            screenshot6 = pyautogui.screenshot()
            width6, height6 = screenshot6.size
            screenshot6 = screenshot6.crop((0, 200, width6, height6))
        debug_img_verify = "ocr_debug_step6_verify.png"
        screenshot6.save(debug_img_verify)
        data_verify = pytesseract.image_to_data(screenshot6, output_type=pytesseract.Output.DICT, config=custom_config)
        found_year = False
        for word in data_verify['text']:
            if str(latest_year) == word.strip():
                found_year = True
                break
        if found_year:
            print(f"Verification: Year {latest_year} is now selected in År dropdown.")
        else:
            print(f"WARNING: Year {latest_year} was clicked but is not shown as selected in År dropdown!")
    else:
        print("No years found!")

    # Wait for UI to stabilize
    time.sleep(2)
    
    # Use exact coordinates for hovering over "Estimert produksjon over valgt periode (MWh)"
    target_x = 1750  # Center point between 1667 and 1834
    target_y = 642   # Center point between 605 and 679
    
    # Move mouse to hover over the text
    pyautogui.moveTo(target_x, target_y)
    time.sleep(1)
    
    # Click the breadcrumb button ("...") at exact coordinates
    breadcrumb_x = 1835
    breadcrumb_y = 595
    pyautogui.click(breadcrumb_x, breadcrumb_y)
    print("Hovered over 'Estimert produksjon' and clicked breadcrumb")
    
    # Wait for menu to appear
    time.sleep(1)
    
    # Click 'Eksporter data' at exact coordinates
    export_x = 1730
    export_y = 620
    pyautogui.click(export_x, export_y)
    print("Clicked 'Eksporter data' button")

    # Wait for menu to appear
    time.sleep(1)

    # Click 'Eksporter' at exact coordinates
    eksporter_x = 1420
    eksporter_y = 1200
    pyautogui.click(eksporter_x, eksporter_y)
    print("Clicked 'Eksporter' button")

    # --- NEW: Load latest Excel file from Downloads and print DataFrame preview ---
    import pandas as pd
    import os
    import time

    def load_latest_excel_from_downloads():
        downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
        print(f"Looking for Excel files in: {downloads_path}")
        excel_files = [
            os.path.join(downloads_path, f)
            for f in os.listdir(downloads_path)
            if f.lower().endswith('.xlsx')
        ]
        if not excel_files:
            print('No Excel files found in Downloads folder.')
            return None
        latest_file = max(excel_files, key=os.path.getmtime)
        print(f'Loading latest Excel file: {latest_file}')
        try:
            df = pd.read_excel(latest_file)
            print('Successfully loaded Excel file into DataFrame.')
            print('DataFrame preview:')
            print(df.head())
            return df
        except Exception as e:
            print(f'Error reading Excel file: {e}')
            return None

    # Wait a moment to ensure download is complete
    time.sleep(2)
    df = load_latest_excel_from_downloads()
    if df is not None:
        # Write DataFrame to CSV in Downloads folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, 'solkraft_export.csv')
        df.to_csv(csv_path, index=False)
        print(f"DataFrame written to CSV: {csv_path}")
    # --- END NEW ---
