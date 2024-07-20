import pandas as pd

# Load the data
df2 = pd.read_csv('Disclosure_Agg_Supp.csv')
df3 = pd.read_csv('Company-name-matches.csv', encoding="ISO-8859-1")

# Assuming df3 has columns 'df2_company' and 'df1_company' for matching and replacing
# Create a dictionary for mapping company names
company_map = dict(zip(df3['df2_company'], df3['df1_company']))

# Copy the original Pharma Company Names
df2['Original Pharma Company Name'] = df2['Pharma Company Name']

# Replace the company names in df2
df2['Pharma Company Name'] = df2['Pharma Company Name'].map(company_map).fillna(df2['Pharma Company Name'])

# Count how many rows were replaced
rows_replaced = (df2['Original Pharma Company Name'] != df2['Pharma Company Name']).sum()
print(f"Number of rows replaced: {rows_replaced}")
print(df2[df2['Original Pharma Company Name'] != df2['Pharma Company Name']])

# Optionally, you can drop the 'Original Pharma Company Name' column if no longer needed
df2 = df2.drop(columns=['Original Pharma Company Name'])
df2.to_csv("Disclosure_Agg_Supp_NN.csv", index=False)
