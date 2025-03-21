import pandas as pd
import os

# Define the folder containing the CSV files
csv_folder = "csv"

# Get a list of all CSV files in the folder
csv_files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

with pd.ExcelWriter("output.xlsx", engine="openpyxl") as writer:
    for csv_file in csv_files:
        file_path = os.path.join(csv_folder, csv_file)
        try:
            # Read CSV with ";" as separator
            df = pd.read_csv(file_path, sep=";", on_bad_lines="skip")

            # Replace commas with dots in numeric columns and convert them to float
            df = df.apply(
                lambda x: (
                    x.str.replace(",", ".").astype(float)
                    if x.dtype == "object" and x.str.contains(",").any()
                    else x
                )
            )

            # Write to Excel
            sheet_name = os.path.splitext(csv_file)[0]
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        except Exception as e:
            print(f"Error reading {csv_file}: {e}")

print("Alle CSV-filene er nå importert til Excel-fil 'output.xlsx'")
