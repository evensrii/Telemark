import requests
from io import BytesIO
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib as mpl
import matplotlib.pyplot as plt

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
