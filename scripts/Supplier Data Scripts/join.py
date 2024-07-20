import pandas as pd

# Define a list of filenames
filenames = [f"agg_epd_{year}.csv" for year in range(2015, 2023)]

# Use concat to join all files together
df = pd.concat((pd.read_csv(filename) for filename in filenames), ignore_index=True)

# Save the combined dataset
df.to_csv("combined_dataset.csv", index=False)

print("CSV files have been combined!")
