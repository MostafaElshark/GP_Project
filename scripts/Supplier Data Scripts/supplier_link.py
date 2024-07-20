import os
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import pandas as pd
import logging

# Setup logging
logging.basicConfig(filename='combine_and_aggregate.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

chunksize = 100000  # Modify based on your memory capacity and file size

column_types = {
	"YEAR_MONTH": "int32",
    "REGIONAL_OFFICE_NAME": "string",
    "ICB_NAME": "string",
    "PCO_NAME": "string",
    "PRACTICE_NAME": "string",
    "ADDRESS_1": "string",
    "ADDRESS_2": "string",
    "ADDRESS_3": "string",
    "ADDRESS_4": "string",
    "POSTCODE": "string",
    "BNF_CHEMICAL_SUBSTANCE": "string",
    "CHEMICAL_SUBSTANCE_BNF_DESCR": "string",
    "BNF_CODE": "string",
    "BNF_DESCRIPTION": "string",
    "BNF_CHAPTER_PLUS_CODE": "string",
    "REGIONAL_OFFICE_CODE": "string",
    "ICB_CODE": "string",
    "PCO_CODE": "string",
    "PRACTICE_CODE": "string",
    "QUANTITY": "float32",
    "ITEMS": "int32",
    "TOTAL_QUANTITY": "float32",
    "ADQUSAGE": "float32",
    "NIC": "float32",
    "ACTUAL_COST": "float32"
}


def merge_and_process_dataframes(df1_path, df2_path, output_folder, year):
    try:
        # Read in the dataframes
        df1 = dd.read_csv(df1_path, dtype=column_types)
        df2 = dd.read_csv(df2_path, dtype={"AMP": "string", "Supplier": "string"}).drop_duplicates(subset='AMP')
        df2['AMP'] = df2['AMP'].str.lower()
        df2 = df2.drop_duplicates(subset='AMP')

        # Convert columns to lowercase for better matching
        df1['BNF_DESCRIPTION'] = df1['BNF_DESCRIPTION'].str.lower()
        df2['AMP'] = df2['AMP'].str.lower()

        # Rename 'YEAR_MONTH' column to 'Just Year' in df1
        df1 = df1.rename(columns={"YEAR_MONTH": "YEAR"})

        # Merge the dataframes
        merged_df = df1.merge(df2, left_on='BNF_DESCRIPTION', right_on='AMP', how='left')

        # Save the result
        output_path = os.path.join(output_folder, f'edp_{year}.csv')
        merged_df.to_csv(output_path, index=False, single_file=True)

        logging.info("Dataframes merged and processed successfully.")

    except Exception as e:
        logging.error(f"Error during merging and processing dataframes: {str(e)}")


def main():
    years = list(range(2015, 2023))
    input_folder = r'C:\Users\Administrator\Documents\gp'
    for year in years:
        output_folder = os.path.join(input_folder, f'edp_{year}')
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        df1_path = os.path.join(input_folder, f'aggregated_epd_{year}.csv')
        df2_path = os.path.join(input_folder, f'with_keep_values_use.csv')
        merge_and_process_dataframes(df1_path, df2_path, output_folder, year)    
    logging.info("All tasks completed successfully!")

if __name__ == '__main__':
    try:
        with LocalCluster(memory_limit='20GB', processes=True, threads_per_worker=2, n_workers=3, timeout='2000s', local_directory=r'C:\Users\Administrator') as cluster:
            with Client(cluster) as client:
                main()
    except Exception as e:
        logging.error(f"Error in the cluster/client setup: {str(e)}")
