import pandas as pd

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv("MetroPT3(AirCompressor).csv")

# Drop the first column (index 0)
df_modified = df.drop(df.columns[0], axis=1)  # axis=1 specifies column

# Save the modified DataFrame to a new CSV file (optional)
df_modified.to_csv("MetroPT3_AirCompressor.csv", index=False)

# Alternatively, print the modified DataFrame
print(df_modified)