import pandas as pd
import requests
from bs4 import BeautifulSoup as bs4
import os
import threading
import queue
import time
import urllib.parse
import logging  # Added logging
from threading import Lock  # Added Lock

# Define constants for retries and retry delay
MAX_RETRIES = 5
RETRY_DELAY = 2

# Initialize logging
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

mydf = pd.read_excel(r'.\product_BNF.xlsx')
num_workers = 20
failed_requests = []
Ids = set()
newdf = pd.DataFrame(columns=['BNF Code', 'BNF Name', 'AMP', 'Supplier', 'Discontinued', 'Parallel Import', 'VMP', 'Results'])
newdf.to_csv('newdf.csv', index=False)

# Create a lock for thread-safe operations
lock = Lock()

def fetch_data_with_retries(session, url):
    for _ in range(MAX_RETRIES):
        try:
            response = session.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f'Retry {MAX_RETRIES} for URL {url}')
            time.sleep(RETRY_DELAY)
    return None

def worker(worker_num: int, q: queue) -> None:
    with requests.Session() as session:
        while True:
            Ids.add(f'Worker: {worker_num}, PID: {os.getpid()}, TID: {threading.get_ident()}')
            idm, name = q.get()
            url = f'https://services.nhsbsa.nhs.uk/dmd-browser/search/results?ampName={urllib.parse.quote_plus(name)}&searchType=AMP&showInvalidItems=false&hideParallelImport=false&hideSpecialOrder=false&hideDiscontinuedItems=false&size=200'
            logging.info(f'WORKER {worker_num}: API request for id: {name} started ...')
            
            response = fetch_data_with_retries(session, url)
            
            if response is None:
                logging.error(f'WORKER {worker_num}: API request for id: {idm} failed after {MAX_RETRIES} retries.')
                failed_requests.append((idm, name))
            else:
                getresponse = bs4(response.text, 'html.parser')
            if getresponse.find('p', id='searchNoResultsFound') is None:
                resultsfound = getresponse.find('div', id='pageAndResultCount').text
                numberofresults = resultsfound.split('(')[1].split(' ')[0]
                supplier = []
                ampname = []
                vmpname = []
                discontinued = []
                parallel = []
                for i in range(0, int(numberofresults)):
                    try:
                        supplier.append(getresponse.find('td', id=f'amp-supplier-{i}').text)
                    except:
                        supplier.append('None')
                    try:
                        ampname.append(getresponse.find('span', id=f'amp-name-{i}').text)
                    except:
                        ampname.append('None')
                    try:
                        vmpname.append(getresponse.find('span', id=f'vmp-name-{i}').text)
                    except:
                        vmpname.append('None')
                    try:
                        # Check if the "Discontinued" checkbox is present and add 1 if checked, 0 if not
                        discontinued_td = getresponse.find('td', id=f'amp-discontinued-{i}')
                        discontinued_checkbox = discontinued_td.find('svg', class_='nhsuk-icon__tick-red')
                        discontinued.append(1 if discontinued_checkbox else 0)
                    except:
                        discontinued.append(0)  # Default to 0 if not found
                    try:
                        # Check if the "Parallel Import" checkbox is present and add 1 if checked, 0 if not
                        parallel_td = getresponse.find('td', id=f'amp-parallel-import-product-{i}')
                        parallel_checkbox = parallel_td.find('svg', class_='nhsuk-icon__tick-red')
                        parallel.append(1 if parallel_checkbox else 0)
                    except:
                        parallel.append(0)  # Default to 0 if not foun
                newdf = pd.DataFrame({'BNF Code': idm,'BNF Name': name, 'AMP': ampname, 'Supplier': supplier, 'Discontinued': discontinued, 'Parallel Import': parallel, 'VMP': vmpname, 'Results': numberofresults})
                
                # Use the lock for thread-safe DataFrame operations
                with lock:
                    newdf.to_csv('newdf.csv', mode='a', header=False, index=False)
            q.task_done()

def main():
    q = queue.Queue()
    for i in range(len(mydf)):
        q.put((mydf['BNF Code'][i], mydf['BNF Name'][i]))

    for i in range(num_workers):
        threading.Thread(target=worker, args=(i, q), daemon=True).start()

    q.join()

if __name__ == "__main__":
    print('THREADING')
    start_time = time.time()
    main()
    print(f'\nDataframe ({len(failed_requests)} failed requests')
    print("\n--- %s seconds ---" % (time.time() - start_time))
    print(list(Ids))
