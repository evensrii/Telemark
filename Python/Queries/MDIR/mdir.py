import requests
from io import BytesIO
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib as mpl
import matplotlib.pyplot as plt

# from ydata_profiling import ProfileReport #ydata-profiling delivers an extended analysis of a DataFrame

# conda install xlrd


## Mdir gir ingen direkte url til .xlsx-fil, så jeg bruker requests for å simulere nedlasting av filen

url = "https://www.miljodirektoratet.no/greenhousegas/api/excel/?areaId=10048"

# Make a GET request to the URL to simulate downloading the file
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:

    # Use BytesIO to create a file-like object from the content (BytesIO lar deg jobbe med binære filer.)
    excel_file = BytesIO(response.content)

    # Use pandas ExcelFile to work with multiple sheets
    xls = pd.ExcelFile(excel_file)

    # Get the sheet names
    sheet_names = xls.sheet_names

    # Create a dictionary to store DataFrames for each sheet
    sheet_data = {}

    # Iterate through each sheet and read the data into a DataFrame
    for sheet_name in sheet_names:
        sheet_data[sheet_name] = xls.parse(sheet_name)

    # Now 'sheet_data' is a dictionary where keys are sheet names and values are DataFrames
    # You can access individual DataFrames like sheet_data['Sheet1']
    print(sheet_data)
else:
    print(f"Failed to download the file. Status code: {response.status_code}")

# Hente ut aktuelt ark
df = sheet_data["Oversikt - detaljert"]

# Basic overview of dataset
df.head()
df.shape
df.dtypes
df.info()
df[df.duplicated()]  # Ingen duplikater
df.isna().sum()  # No missing values (per feature)
df.isna().sum().sum()  # No missing values
round(df.isna().sum().sum() / df.size * 100, 1)  # Percentage of missing cells
df.describe()  # Mål for numeriske variabler
df.describe(include="object")  # Inluderer også kategoriske variabler

# Rename column name "Utslipp (tonn CO₂-ekvivalenter)" to "Utslipp"
df.columns = df.columns.str.replace("Utslipp (tonn CO₂-ekvivalenter)", "Utslipp")

# Filter out rows not matching "CO2" in the "Klimagass" column, and remove the "Klimagass" column
df = df[df["Klimagass"] == "CO2"].drop(columns="Klimagass")

# Group by 'Sektor', 'Klimagass', and sum 'Utslipp'
df = df.groupby(["År", "Sektor"]).agg({"Utslipp": "sum"}).reset_index()

# Convert column 'År' from integer to datetime format
df["År"] = pd.to_datetime(
    df["År"], format="%Y"
)  # %Y indicates the format of the input date

############## Plotting ##############

import matplotlib.dates as mdates  # "mdates.DateFormatter('%Y') is used to format the x-axis to display only the year."

##### Seaborn
import seaborn as sns
import seaborn.objects as so

sns.set_style("white")

testplt = sns.relplot(
    data=df, x="År", y="Utslipp", hue="Sektor", kind="line", marker="o", aspect=1.5
)
testplt.ax.set_xticks(df["År"])
testplt.ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

testplt = sns.lineplot(
    data=df, x="År", y="Utslipp", hue="Sektor", marker="o", dashes=(5, 5)
)
testplt.set_xticks(df["År"])
testplt.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
testplt.despine()

sns.axes_style()

sns.barplot(data=df, x="År", y="Utslipp", hue="Sektor")


testplt.ax.set_xticks(df["År"])

testplt

testplt = sns.relplot(
    data=df, x="År", y="Utslipp", hue="Sektor", kind="line", marker="o", aspect=1.5
)

plt.set_xticklabels(df["År"])

plt.ax.set_xticks([2009, 2011, 2013, 2015, 2017, 2019])

plt.ax.xaxis.set_major_locator(mdates.YearLocator())

#### Matplotlib
pivot_df.plot(kind="line", marker="o", figsize=(10, 6))


#### Highcharts for Python

# Import objects from the catch-all ".highcharts" module.
# from highcharts_core import highcharts

# Import classes using precise module indications. For example:
from highcharts_core.chart import Chart
from highcharts_core.global_options.shared_options import SharedOptions
from highcharts_core.options import HighchartsOptions
from highcharts_core.options.plot_options.bar import BarOptions
from highcharts_core.options.series.bar import BarSeries

# Initialize Highchart object
chart = Chart()

unique_sektors = grouped_df["Sektor"].unique()

for sektor in unique_sektors:
    sektor_data = grouped_df[grouped_df["Sektor"] == sektor]
    Chart.add_data_set(
        list(zip(sektor_data["År"], sektor_data["Utslipp"])), "line", name=sektor
    )

# Create the chart
my_chart = Chart.from_pandas(
    grouped_df,
    series_type="line",
    property_map={"x": "År", "y": "Utslipp", id: "Sektor"},
)

my_chart = Chart.from_pandas(pivot_df, series_type="line", series_in_rows=True)

# Display the chart
my_chart.display()
