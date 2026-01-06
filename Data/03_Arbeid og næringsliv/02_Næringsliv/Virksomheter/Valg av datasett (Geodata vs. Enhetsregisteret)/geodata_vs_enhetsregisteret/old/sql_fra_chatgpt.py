import pandas as pd

# -------------------------------------------------------------------
# Settings
# -------------------------------------------------------------------
# Path to your CSV file
file_path = "./enhetsregisteret_kombinert.csv"  # change if needed

# -------------------------------------------------------------------
# Load data
# -------------------------------------------------------------------
df = pd.read_csv(file_path, low_memory=False)

# Standardize column names for easier handling
df = df.rename(columns={
    "Org. nr.": "orgnr",
    "Overordnet enhet": "parent",
    "Antall ansatte": "employees"
})

# -------------------------------------------------------------------
# Apply SQL-equivalent logic:
#
# "orgnr" NOT IN (
#     SELECT DISTINCT parent
#     FROM table
#     WHERE parent IS NOT NULL
# )
# -------------------------------------------------------------------

# Clean org numbers to consistent string format (strip trailing ".0")
parent_orgs = (
    df["parent"]
    .dropna()
    .astype(str)
    .str.replace(".0", "", regex=False)
    .unique()
)

df["orgnr_str"] = (
    df["orgnr"]
    .astype(str)
    .str.replace(".0", "", regex=False)
)

# Filter: keep rows where orgnr_str is NOT a parent_org
filtered = df[~df["orgnr_str"].isin(parent_orgs)].copy()

# -------------------------------------------------------------------
# Compute total employees
# -------------------------------------------------------------------
filtered["employees_num"] = pd.to_numeric(filtered["employees"], errors="coerce")
total_employees = filtered["employees_num"].sum()

# -------------------------------------------------------------------
# Output
# -------------------------------------------------------------------
print(f"Total rows in input:      {len(df):,}")
print(f"Rows after filter:        {len(filtered):,}")
print(f"Total employees (filtered): {int(total_employees):,}")
