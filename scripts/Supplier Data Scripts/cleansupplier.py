import pandas as pd

df1 = pd.read_csv(r'F:\GP practice\Suupliers\done\newdf.csv')

initial_rows = len(df1)

# Remove duplicate rows based on all columns
df_deduplicated = df1.drop_duplicates(subset=['AMP', 'Supplier', 'Discontinued', 'Parallel Import', 'VMP'])

# Calculate number of rows after deduplication
deduplicated_rows = len(df_deduplicated)

# Calculate number of rows deleted
rows_deleted = initial_rows - deduplicated_rows

print(f"{rows_deleted} rows will be deleted.")

df_deduplicated['keep'] = ''

# Mark rows with only one supplier for a given AMP
unique_supplier_mask = df_deduplicated.groupby(['AMP']).Supplier.transform('nunique') == 1
df_deduplicated.loc[unique_supplier_mask, 'keep'] = "One supplier"

# Identify rows with only one non-importer for a given AMP, but exclude rows already marked as "One supplier"
non_importer_mask = df_deduplicated['Parallel Import'] == 0
unique_non_importer_mask = df_deduplicated[non_importer_mask & (df_deduplicated['keep'] != "One supplier")].groupby('AMP').Supplier.transform('nunique') == 1
df_deduplicated.loc[unique_non_importer_mask & non_importer_mask, 'keep'] = "One non-importer"

# Count and print the results
count_one_supplier = df_deduplicated[df_deduplicated['keep'] == "One supplier"].shape[0]
count_one_non_importer = df_deduplicated[df_deduplicated['keep'] == "One non-importer"].shape[0]

print(f"Number of rows with 'One supplier': {count_one_supplier}")
print(f"Number of rows with 'One non-importer': {count_one_non_importer}")

# Save the entire deduplicated DataFrame
df_deduplicated.to_csv(r'F:\GP practice\Suupliers\done\deduplicated.csv', index=False)

# Filter and save rows that have values in the 'keep' column
filtered_df = df_deduplicated[df_deduplicated['keep'] != '']
filtered_df.to_csv(r'F:\GP practice\Suupliers\done\with_keep_values.csv', index=False)

#############
filtered_dfs = filtered_df.drop_duplicates(subset=['AMP', 'Supplier'])
filtered_dfs.to_csv(r'F:\GP practice\Suupliers\done\with_keep_values_use.csv', index=False)
