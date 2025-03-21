import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import PyPDF2


# Specify the URL of the website you want to scrape
url = "https://www.miljodirektoratet.no/greenhousegas/api/excel/?areaId=10048"

# Send an HTTP request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Show content of the webpage
    print(response.content)
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

# Filter the links that include the word "desember"
december_links = [link["href"] for link in links if "desember" in link["href"]]


# Scrape a file with a certain url from the website and convert it to a pandas dataframe
url = "https://www.miljodirektoratet.no/greenhousegas/api/excel/?areaId=10048"
