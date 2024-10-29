import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from dotenv import load_dotenv
import io

# Import files from the same directory as pandas dataframes. Create corresponding names.
df_2021 = pd.read_csv("2021.csv")
df_2022 = pd.read_csv("2022.csv")
df_2023 = pd.read_csv("2023.csv")
df_2024 = pd.read_csv("2024.csv")

# Show unique values in the column "Kommunenavn" in df_2021 and count the number of occurences per unique value.
print(df_2021["Kommunenavn"].value_counts())  # 10584 (mangler 01-01-2021)
print(df_2022["Kommunenavn"].value_counts())  # 10656
print(df_2023["Kommunenavn"].value_counts())  # 10656
print(df_2024["Kommunenavn"].value_counts())  # 21246

# Identify duplicate rows in the dataframes.
duplicateRowsDF_2021 = df_2021[df_2021.duplicated()]
duplicateRowsDF_2022 = df_2022[df_2022.duplicated()]
duplicateRowsDF_2023 = df_2023[df_2023.duplicated()]
duplicateRowsDF_2024 = df_2024[df_2024.duplicated()]

# Basic overview of dataset
df_2022.head()
df_2022.info()
df_2022[df_2022.duplicated()]  # Ingen duplikater
df_2022.isna().sum()  # No missing values (per feature)
df_2022.isna().sum().sum()  # No missing values
round(df_2022.isna().sum().sum() / df_2022.size * 100, 1)  # Percentage of missing cells
df_2022.describe()  # Mål for numeriske variabler
df_2022.describe(include="object")  # Inluderer også kategoriske variabler

df_2023.head()
df_2023.info()
df_2023[df_2023.duplicated()]  # Ingen duplikater
df_2023.isna().sum()  # No missing values (per feature)
df_2023.isna().sum().sum()  # No missing values
round(df_2023.isna().sum().sum() / df_2023.size * 100, 1)  # Percentage of missing cells
df_2023.describe()  # Mål for numeriske variabler
df_2023.describe(include="object")  # Inluderer også kategoriske variabler

df_2024.head()
df_2024.info()
df_2024[df_2024.duplicated()]  # Ingen duplikater
df_2024.isna().sum()  # No missing values (per feature)
df_2023.isna().sum().sum()  # No missing values
round(df_2024.isna().sum().sum() / df_2024.size * 100, 1)  # Percentage of missing cells
df_2024.describe()  # Mål for numeriske variabler
df_2024.describe(include="object")  # Inluderer også kategoriske variabler

# Count the number of values in the column "Year" in df_2021, df_2022, df_2023 and df_2024.
print(df_2023["Year"].value_counts())


# "Year" kun lagt til i 2023, ikke 2022...
