import pandas as pd
import recordlinkage
from recordlinkage.preprocessing import clean
from fuzzywuzzy import fuzz
from TextCleaner import TextCleaner

class DataLinker:
    
    def __init__(self, df1, df2):
        self.df1 = df1.copy()
        self.df2 = df2.copy()
        self.text_cleaner = TextCleaner()
        self.matches = None
        self.result = None
    
    def preprocess(self):
        # Lowercase and clean up postcodes
        self.df1['Postcode'] = self.df1['Postcode'].str.lower().str.replace(' ', '')
        self.df2['Postcode'] = self.df2['Postcode'].str.lower().str.replace(' ', '')
        
        # Clean Institution Names using TextCleaner
        self.df1['Cleaned Name'] = self.df1['Address Line 2'].apply(self.text_cleaner.clean_abstract)
        self.df2['Cleaned Name'] = self.df2['Name'].apply(self.text_cleaner.clean_abstract)

        # Use recordlinkage's clean function for location and address
        for col in ['Cleaned Name','Location', 'Address Line 1', 'Address Line 2']:
            self.df1[f'c{col}'] = clean(self.df1[col])
        
        for col in ['Cleaned Name', 'Address Line 1', 'Address Line 2']:
            self.df2[f'c{col}'] = clean(self.df2[col])        
        
    def create_pairs(self):
        # Create pairs of records to compare, blocking on postcode
        indexer = recordlinkage.Index()
        indexer.block('Postcode')
        self.pairs = indexer.index(self.df1, self.df2)
    
    def compare_pairs(self):
        # Compare the pairs of records
        compare = recordlinkage.Compare()
        compare.string('cCleaned Name', 'cCleaned Name', method='levenshtein', threshold=0.85, label='Matched_InstitutionName_levenshtein')
        compare.string('cCleaned Name', 'cCleaned Name', method='jarowinkler', threshold=0.90, label='Matched_InstitutionName_jarowinkler')
        compare.string('cCleaned Location', 'cCleaned Name', method='levenshtein', threshold=0.90, label='Matched_Location_Name')
        compare.string('cCleaned Address Line 1', 'cCleaned Name', method='levenshtein', threshold=0.90, label='Matched_Address1_Name')
        compare.string('cCleaned Address Line 2', 'cCleaned Name', method='levenshtein', threshold=0.90, label='Matched_Address2_Name')
        compare.string('cLocation', 'cAddress Line 1', threshold=0.80, method='damerau_levenshtein', label='Matched_Location_Address1')
        compare.string('cAddress Line 1', 'cAddress Line 1', threshold=0.80, method='damerau_levenshtein', label='Matched_Address1')
        compare.string('cAddress Line 2', 'cAddress Line 2', threshold=0.80, method='damerau_levenshtein', label='Matched_Address2')
        compare.exact('Postcode', 'Postcode', label='Matched_Postcode')
        # Customizations: Add or modify the compare.string() methods to customize the comparison
        self.features = compare.compute(self.pairs, self.df1, self.df2)
    
    def classify(self):
        # Filter the features to keep only those which have a non-zero value 
        # in any of the desired name matching columns
        self.features = self.features[
            (self.features['Matched_InstitutionName_levenshtein'] != 0) |
            (self.features['Matched_InstitutionName_jarowinkler'] != 0) |
            (self.features['Matched_Location_Name'] != 0) |
            (self.features['Matched_Address1_Name'] != 0) |
            (self.features['Matched_Address2_Name'] != 0)
        ]

        # Define the weights for each feature
        WEIGHTS = {
            'Matched_InstitutionName_levenshtein': 1,
            'Matched_InstitutionName_jarowinkler': 1,
            'Matched_Location_Name': 1,
            'Matched_Address1_Name': 1,
            'Matched_Address2_Name': 1,
            'Matched_Location_Address1': 1,
            'Matched_Address1': 1,
            'Matched_Address2': 1,
            'Matched_Postcode': 1,
        }

        # Compute the weighted sum of the features
        self.features['weighted_sum'] = sum(self.features[col] * WEIGHTS[col] for col in WEIGHTS if col in self.features.columns)

        # Keep only the matches with a weighted sum above a certain threshold
        self.matches = self.features[self.features['weighted_sum'] >= 2]
    
    def post_process(self):
        # Reset the index of the matches
        self.result = self.matches.reset_index()
        
        # Dictionary to map matched columns to their corresponding cleaned columns
        columns_map = {
            'Matched_InstitutionName_levenshtein': 'cCleaned Name',
            'Matched_InstitutionName_jarowinkler': 'cCleaned Name',
            'Matched_Location_Name': 'cCleaned Location',
            'Matched_Address1_Name': 'cCleaned Address Line 1',
            'Matched_Address2_Name': 'cCleaned Address Line 2'
        }
        
        # Compute the FuzzyWuzzy score for each match
        for index, row in self.result.iterrows():
            name2 = self.df2.loc[row['level_1'], 'cCleaned Name']
            
            # Check which columns have a value of 1 and compute the fuzzy score based on them
            scores = []
            for col, value in row.items():
                if col in columns_map and value == 1:
                    name1 = self.df1.loc[row['level_0'], columns_map[col]]
                    scores.append(fuzz.ratio(name1, name2))
            
            # Here, you might want to define how you'll aggregate the scores 
            # (e.g., take the average, max, etc.) for the final 'fuzzy_score'. 
            # I'm using max() for this example.
            self.result.at[index, 'fuzzy_score'] = max(scores) if scores else 0
        
        # Sort the matches by FuzzyWuzzy score and keep only the best match for each record
        self.result.sort_values('fuzzy_score', ascending=False, inplace=True)
        self.result.drop_duplicates(['level_0'], keep='first', inplace=True)
    
    def link_data(self):
        # Perform the entire data linking process
        self.preprocess()
        self.create_pairs()
        self.compare_pairs()
        self.classify()
        self.post_process()
        return self.result

'''# Customizations: Replace the file paths and column names with your own data
df1 = pd.read_csv('path/to/your/data1.csv')
df2 = pd.read_csv('path/to/your/data2.csv')

# Perform the data linking
linker = DataLinker(df1, df2)
result = linker.link_data()
'''