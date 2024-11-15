## Hensikten er å generere ulike syntetiske datasett, og visualisere disse vha. Highcharts.

# Se faker tutorial: https://www.datacamp.com/tutorial/creating-synthetic-data-with-python-faker-tutorial

# pip install Faker
from faker import Faker
import pandas as pd
import numpy as np
import random
from random import randrange, randint
from datetime import datetime
import random

## Tabell 1: Line chart med utslippstall for CO2 (var) per måned i 10 år (var) i nordiske land (var).

# Generate a fake dataset with 10 years of CO2 emissions for 3 nordic countries
faker = Faker("no_NO")
Faker.seed(4312)

## Generere en liste med de 3 skandinaviske landene


def generate_country_list(length):
    countries = [
        "Norway 2021",
        "Norway 2022",
        "Norway 2023",
        "Denmark 2021",
        "Denmark 2022",
        "Denmark 2023",
        "Sweden 2021",
        "Sweden 2022",
        "Sweden 2023",
    ]

    # Calculate repetitions to make each country appear the same number of times
    repetitions = length // len(countries)

    # Repeat each country the calculated number of times
    result_list = countries * repetitions

    # Shuffle the list to make the order random
    random.shuffle(result_list)

    # If there are remaining elements to reach the desired length, add them
    result_list += random.sample(countries, length % len(countries))

    return result_list


result = generate_country_list(9)
print(result)

## Generere resten av tabellen


def input_data(records):
    data = pd.DataFrame()

    for i in range(0, records):
        # data.loc[i, "id"] = randint(1, records)
        data.loc[i, "utslipp"] = faker.random_int(0, 1000000)

    data["country"] = generate_country_list(records)

    return data


df = input_data(9)

# Splitter og fjerner år fra landkolonne
df["year"] = df["country"].str.split(" ").str[1]
df["country"] = df["country"].str.split(" ").str[0]

# Konverterer til datetime
df["year"] = pd.to_datetime(df["year"])

# Sorterer etter land og år
df = df.sort_values(by=["country", "year"])

# Nullstill index
df = df.reset_index(drop=True)


#### Highcharts

# (pip install highcharts-core)

# Import objects from the catch-all ".highcharts" module.
from highcharts_core import highcharts

# Create the chart
my_chart = Chart.from_pandas(
    df,
    series_type="line",
    property_map={"x": "year", "y": "utslipp"},
)

# Display the chart
my_chart.display()
