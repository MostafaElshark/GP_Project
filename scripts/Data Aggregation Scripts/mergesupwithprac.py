import pandas as pd
import re

# Function to create and standardize a new column for matching
def standardize_text_for_matching(df, original_column, new_column):
    df[new_column] = df[original_column].str.lower().str.strip()  # Convert to lowercase and strip whitespace
    df[new_column] = df[new_column].apply(lambda x: re.sub(r'\W+', '', x) if isinstance(x, str) else x)  # Remove non-alphanumeric characters

# Read the datasets
df1 = pd.read_csv('Disclosure_Agg_Supp_NN.csv')
df2 = pd.read_csv('sup_agg_NNN.csv')

# Create and standardize new columns for matching
standardize_text_for_matching(df1, 'Pharma Company Name', 'Standardized_Pharma_Company_Name')
standardize_text_for_matching(df2, 'Pharma Company Name', 'Standardized_Pharma_Company_Name')

# Merge the datasets using the standardized columns
merged_df = pd.merge(df1, df2, left_on=['Year', 'PRACTICE_CODE', 'Standardized_Pharma_Company_Name'], 
                     right_on=['Year', 'PRACTICE_CODE', 'Standardized_Pharma_Company_Name'], how='outer')

# Fill null values in one column with values from the other and vice versa
merged_df['Pharma Company Name_x'].fillna(merged_df['Pharma Company Name_y'], inplace=True)
merged_df['Pharma Company Name_y'].fillna(merged_df['Pharma Company Name_x'], inplace=True)

# Create a new column with combined values
merged_df['Pharma Company Name'] = merged_df['Pharma Company Name_x']

# Drop the original columns
merged_df.drop(columns=['Pharma Company Name_x', 'Pharma Company Name_y'], inplace=True)

# Save the merged dataset (optional)
merged_df.to_csv('merged_dataset_agg_supp_NNN.csv', index=False)

print("Datasets have been merged and the Pharma Company Name column has been standardized!")
