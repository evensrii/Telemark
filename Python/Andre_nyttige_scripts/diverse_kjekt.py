# Basic overview of dataset
df.head()
df.info()
df[df.duplicated()]  # Ingen duplikater
df.isna().sum()  # No missing values (per feature)
df.isna().sum().sum()  # No missing values
round(df.isna().sum().sum() / df.size * 100, 1)  # Percentage of missing cells
df.describe()  # Mål for numeriske variabler
df.describe(include="object")  # Inluderer også kategoriske variabler
