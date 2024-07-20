import pandas as pd
import numpy as np

class DataConnector:
    
    def __init__(self, data_dict):
        self.data_dict = data_dict
    
    @staticmethod
    def preprocess_data(results, resultslocation, resultsAddress1, resultsAddress2):
        # Step 1: Add an identifier to each dataframe
        results['matchedwith'] = 'Name Matched With Name'
        resultslocation['matchedwith'] = 'Name Matched with Location'
        resultsAddress1['matchedwith'] = 'Name Matched with Address1'
        resultsAddress2['matchedwith'] = 'Name Matched with Address2'
        # Step 2: Concatenate the four dataframes
        combined_results = pd.concat([results, resultslocation, resultsAddress1, resultsAddress2])
        combined_results.sort_values(by=['fuzzy_score', 'weighted_sum'], ascending=[False, False], inplace=True)
        # Step 3: Remove duplicates, keeping the row with the highest 'fuzzy_score' and 'weighted_sum'
        # Assuming 'level_0' is the ID you want to use to identify duplicates
        combined_results.drop_duplicates(subset=['level_0'], keep='first', inplace=True)
        
        return combined_results
    
    def match_and_update(self, df1, combined_results):
        columns_to_add = [
            'Matched_InstitutionName_levenshtein', 'Matched_InstitutionName_jarowinkler', 
            'Matched_Addres1_Location', 'Matched_Address1', 'Matched_Address2', 
            'Matched_Postcode', 'weighted_sum', 'Combined_Name_Match', 'fuzzy_score', 
            'matchedwith', 'Organisation Code', "Commissioner", "Practice Name"
        ]
        
        for col in columns_to_add:
            if col not in df1.columns:
                df1[col] = np.NaN
        
        counter = 0
        for _, row in combined_results.iterrows():
            idx_newdf = row.get('level_0', None)
            idx_source_df = row.get('level_1', None)
            matchedwith = row.get('matchedwith', None)

            if idx_newdf is None or idx_source_df is None or matchedwith is None:
                print("Warning: Missing essential columns in combined_results.")
                continue

            print(f"Processing source: {matchedwith}, idx_newdf: {idx_newdf}, idx_source_df: {idx_source_df}")

            source_data = self.data_dict.get(matchedwith, {})
            the_data = source_data.get('data', None)
            col_mapping = source_data.get('col_mapping', {})
            
            if the_data is None:
                print(f"Warning: No DataFrame found for matchedwith: {matchedwith}")
                continue

            for col in columns_to_add:
                if col in row.index and col in df1.columns:
                    df1.at[idx_newdf, col] = row[col]
                else:
                    print(f"Warning: Column {col} not found in combined_results or df1")
            
            primary_name_col = col_mapping.get('Organisation Code', 'Organisation Code')
            
            if primary_name_col not in df1.columns:
                print(f"Warning: {primary_name_col} not found in df1")
                continue

            
            counter += 1
            for dest_col, source_col in col_mapping.items():
                df1.at[idx_newdf, dest_col] = the_data.at[idx_source_df, source_col]

            print(f"After Update - df1.at[{idx_newdf}, {primary_name_col}]: {df1.at[idx_newdf, primary_name_col]}")
                    
        print(f"Matched {counter} rows.")
        return df1
    
    def connect(self, df1, results, resultslocation, resultsAddress1, resultsAddress2):
        combined_results = self.preprocess_data(results, resultslocation, resultsAddress1, resultsAddress2)
        return self.match_and_update(df1, combined_results)


# Usage:

# Your data_dict
data_dict = {
    'Name Matched With Name': {
        'data': df2,
        'col_mapping': {'Organisation Code': 'Organisation Code', 'Commissioner': 'Commissioner', 'Practice Name': 'Name'}
    },
    'Name Matched with Location': {
        'data': df2,
        'col_mapping': {'Organisation Code': 'Organisation Code', 'Commissioner': 'Commissioner', 'Practice Name': 'Name'}
    },
    'Name Matched with Address1': {
        'data': df2,
        'col_mapping': {'Organisation Code': 'Organisation Code', 'Commissioner': 'Commissioner', 'Practice Name': 'Name'}
    },
    'Name Matched with Address2': {
        'data': df2,
        'col_mapping': {'Organisation Code': 'Organisation Code', 'Commissioner': 'Commissioner', 'Practice Name': 'Name'}
    }
}

connector = DataConnector(data_dict)
df1_updated = connector.connect(df1, results, resultslocation, resultsAddress1, resultsAddress2)
