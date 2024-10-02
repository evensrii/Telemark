import os
import pandas as pd
import numpy as np
import requests
import PyPDF2

# q: How to read a pdf file in python?
# a: https://www.geeksforgeeks.org/working-with-pdf-files-in-python/


def read_pdf_file(file_path, page_number):
    # creating a pdf file object
    pdfFileObj = open(file_path, "rb")

    # creating a pdf reader object
    pdfReader = PyPDF2.PdfReader(pdfFileObj)

    # printing number of pages in pdf file
    print(len(pdfReader.pages))

    # creating a page object
    pageObj = pdfReader.pages[page_number]

    # extracting text from page
    print(pageObj.extract_text())

    # closing the pdf file object
    pdfFileObj.close()

    # return pageObj.extract_text()
    return pageObj.extract_text()


# Add data as a variable
telemark_nedsatt = read_pdf_file(
    "NED155 Personer med nedsatt arbeidsevne. Kommune. Januar 2024.pdf", 42
)

# creating a list of months
months = [
    "januar",
    "februar",
    "mars",
    "april",
    "mai",
    "juni",
    "juli",
    "august",
    "september",
    "oktober",
    "november",
    "desember",
]

# Extract month based on matching with the list of months.
month = [month for month in months if month in telemark_nedsatt][0]

# Remove the first 5 lines and the final line in telemark_nedsatt
telemark_nedsatt = telemark_nedsatt.split("\n")[5:-1]

# Convert to a pandas dataframe
df = pd.DataFrame(telemark_nedsatt)

# Split the data into columns
df = df[0].str.split(" ", expand=True)

# Give column names "Kommune", "Antall personer", "Prosent"
df.columns = ["Kommunenr", "Kommune", "Prosent"]

# Add a column named "Måned" with the value of the month, with a first letter capitalized
df["Måned"] = month.capitalize()

# Add a column named "År" with the value of the year
df["År"] = 2024

# Convert "År" to an integer
df["År"] = df["År"].astype(int)

telemark_nedsatt = telemark_nedsatt.split("\n")[5:]
type(telemark_nedsatt)

# Convert to a pandas dataframe
df = pd.DataFrame(telemark_nedsatt)

# Split the data into columns
df = df[0].str.split(" ", expand=True)

# Give column names "Kommune", "Antall personer", "Prosent"
df.columns = ["Kommunenr", "Kommune", "Prosent"]
