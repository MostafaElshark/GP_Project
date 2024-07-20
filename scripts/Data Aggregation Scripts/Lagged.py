import pandas as pd
import re

# Function to create and standardize a new column for matching
def standardize_text_for_matching(df, original_column, new_column):
    df[new_column] = df[original_column].str.lower().str.strip()  # Convert to lowercase and strip whitespace
    df[new_column] = df[new_column].apply(lambda x: re.sub(r'\W+', '', x) if isinstance(x, str) else x)  # Remove non-alphanumeric characters

# Read the datasets
df1 = pd.read_csv('Disclosure_Agg_Supp_NN.csv')  # Assuming df1 is the Disclosure UK data
df2 = pd.read_csv('sup_agg_NNN.csv')    # Assuming df2 is the Prescription data

# Create and standardize new columns for matching
standardize_text_for_matching(df1, 'Pharma Company Name', 'Standardized_Pharma_Company_Name')
standardize_text_for_matching(df2, 'Pharma Company Name', 'Standardized_Pharma_Company_Name')

# Create a lagged year column in df1 (Disclosure UK data)
# This assumes 'Year' is an integer column
df1['Lagged_Year'] = df1['Year'] + 1

# Merge the datasets using the standardized columns and the lagged year
merged_df = pd.merge(df1, df2, left_on=['Lagged_Year', 'PRACTICE_CODE', 'Standardized_Pharma_Company_Name'], 
                     right_on=['Year', 'PRACTICE_CODE', 'Standardized_Pharma_Company_Name'], how='outer')

# Clean up the DataFrame
merged_df.drop(columns=['Standardized_Pharma_Company_Name', 'Pharma Company Name_y'], inplace=True)
merged_df.rename(columns={'Pharma Company Name_x': 'Pharma Company Name'}, inplace=True)

# Save the merged dataset (optional)
merged_df.to_csv('merged_dataset_agg_supp_lagged_N.csv', index=False)

print("Datasets have been merged!")
