import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import logging
import os
import multiprocessing

logging.basicConfig(filename='aggregation.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

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
    "BNF_CODE": "object",
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
    "ACTUAL_COST": "float32",
    "AMP": "string",
    "Supplier": "string",
    "BNF Code": "object"
  }

def aggregate_large_file(year, input_folder):
    try:
        logging.info(f"Starting aggregation for year {year}...")
        input_file = os.path.join(input_folder, f'edp_{year}.csv')

        df = dd.read_csv(input_file, dtype=column_types)

        # Removing unnecessary columns
        columns_to_remove = ['BNF_CHEMICAL_SUBSTANCE', 'CHEMICAL_SUBSTANCE_BNF_DESCR', 'BNF_CODE', 
                             'BNF_DESCRIPTION', 'BNF_CHAPTER_PLUS_CODE', 'BNF Code', 'AMP','Supplier', 'REGIONAL_OFFICE_NAME', 'REGIONAL_OFFICE_CODE', 'ICB_NAME', 'ICB_CODE',
                'PCO_NAME', 'PCO_CODE']
        df = df.drop(columns=columns_to_remove)

        # Aggregate
        aggregated = df.groupby([
                'YEAR', 'PRACTICE_NAME', 'PRACTICE_CODE', 'ADDRESS_1', 'ADDRESS_2',
                'ADDRESS_3', 'ADDRESS_4', 'POSTCODE'
            ]).agg({
                'QUANTITY': 'sum',
                'ITEMS': 'sum',
                'ADQUSAGE': 'sum',
                'NIC': 'sum',
                'ACTUAL_COST': 'sum'
            }).compute()

        # Calculating TOTAL_QUANTITY based on QUANTITY and ITEMS
        aggregated['TOTAL_QUANTITY'] = aggregated['QUANTITY'] * aggregated['ITEMS']

        output_file = os.path.join(input_folder, f'agg_epd_{year}.csv')
        aggregated.reset_index().to_csv(output_file, index=False)
        
        logging.info(f"Aggregated data for year {year} saved successfully.")

    except Exception as e:
        logging.error(f"Error during aggregation for year {year}: {str(e)}")


def main():
    cluster = LocalCluster(
        processes=True,
        threads_per_worker=2,
        n_workers=4,
        timeout='2000s',
        local_directory=r'C:\Users\Administrator'
    )
    client = Client(cluster)
    
    # Print the dashboard link for monitoring
    print("Dashboard Link:", client.dashboard_link)
    
    try:
        years = list(range(2015, 2023))
        input_folder = r'C:\Users\Administrator\Documents\gp'
        for year in years:
            aggregate_large_file(year, input_folder)
        logging.info("All years aggregated successfully!")
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")

    client.close()

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
