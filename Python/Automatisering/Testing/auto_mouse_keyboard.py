# Installing essential libraries
# pip install pyautogui
# pip install keyboard

# Dette skriptet simulerer bruk av mus og tastatur for å utføre oppgaver på datamaskinen.

import pyautogui
import time

# Open a web browser
pyautogui.hotkey("win", "s")
pyautogui.write("vivaldi")
pyautogui.press("enter")
time.sleep(2)
pyautogui.hotkey("shift", "tab")
pyautogui.hotkey("shift", "tab")
pyautogui.write("https://www.nrk.no")
pyautogui.press("enter")
