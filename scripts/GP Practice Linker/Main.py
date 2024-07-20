import pandas as pd
from DataLinker import DataLinker
from DataConnector import DataConnector
from TextCleaner import TextCleaner

def main():
    # Replace with the path to your csv files
    file1 = 'path/to/your/data1.csv'
    file2 = 'path/to/your/data2.csv'

    # Read the CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Text Cleaning
    cleaner = TextCleaner()
    df1['Cleaned Name'] = df1['Institution Name'].apply(cleaner.clean_abstract)
    df2['Cleaned Name'] = df2['Name'].apply(cleaner.clean_abstract)
    
    # Data Linking
    linker = DataLinker(df1, df2)
    linked_data = linker.link_data()

    # Data Connecting
    connector = DataConnector(df1, df2, linked_data)
    connected_data = connector.connect_data()

    # Save the results to CSV files
    linked_data.to_csv('linked_data.csv', index=False)
    connected_data.to_csv('connected_data.csv', index=False)

    print('Data linking and connecting completed. Results saved to linked_data.csv and connected_data.csv.')

if __name__ == "__main__":
    main()
