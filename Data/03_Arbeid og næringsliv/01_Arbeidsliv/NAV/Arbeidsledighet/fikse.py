import pandas as pd

# Read the CSV file
df = pd.read_csv('arbeidsledighet.csv')

# Remove rows containing '2025-'
df = df[~df.apply(lambda x: x.astype(str).str.contains('2025-')).any(axis=1)]

# Save the filtered data back to the CSV file
df.to_csv('arbeidsledighet.csv', index=False)