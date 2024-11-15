import requests
import pandas as pd
from io import BytesIO

# URL til Excelfilen
url = "https://www.miljodirektoratet.no/globalassets/netttjenester/klimagassutslipp-kommuner/utslippsstatistikk_alle_kommuner.xlsx"

# Laste ned Excelfilen
response = requests.get(url)
response.raise_for_status()  # Sjekker om nedlastingen var vellykket

# Leser Excel-filen og spesifikt arket "Oversikt - detaljert"
excel_data = pd.ExcelFile(BytesIO(response.content))
df = excel_data.parse("Oversikt - detaljert")

# Viser de første radene i dataen
print(df.head())
