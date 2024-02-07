import requests
import pandas as pd
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys
from tqdm import tqdm

# Add the CAP_config.py directory to the Python path
sys.path.append(r'D:\Tom\Python Scripts\CAP')

# Now import the variables from CAP_config
from CAP_config import SUBSCRIBER_ID, PASSWORD, FIXED_VALUATION_DATE

# Set the log file directory with the date at the end
log_dir = 'D:\\Tom\\Python Scripts\\CAP\\CAP Pricing\\Logs\\'
log_filename = f'CAP_Pricing_errors_{datetime.now().strftime("%Y%m%d")}.log'
log_path = os.path.join(log_dir, log_filename)

# Configure logging to use the updated log file path
logging.basicConfig(filename=log_path, level=logging.ERROR)

# Constants
URL = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
DATABASE = 'CAR'
VALUATION_DATE = datetime.now().strftime('%Y-%m-%d')
OUTPUT_DIR = 'D:\\Tom\\Python Scripts\\CAP\\CAP Pricing\\'
INPUT_CSV_FILENAME = 'CAP_Pricing_Input.csv'
OUTPUT_CSV_FILENAME = 'CAP_Pricing_Output.csv'

# Initialize a lock object
print_lock = Lock()

# Read input CSV
input_csv_path = os.path.join(OUTPUT_DIR, INPUT_CSV_FILENAME)
df = pd.read_csv(input_csv_path)

# Function to round up mileage to nearest 1000 miles
def round_up_to_nearest_thousand(mileage):
    return int((mileage + 999) / 1000) * 1000

# Function to fetch and parse the response from the API
def fetch_and_parse_data(session, payload):
    try:
        response = session.post(URL, headers=HEADERS, data=payload)
        response.raise_for_status()  # Raise an exception for non-200 status codes
        root = ET.fromstring(response.content)
        namespace = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}
        valuation = root.find('.//ns:Valuation', namespace)
        if valuation is not None:
            clean = valuation.find('ns:Clean', namespace).text
            retail = valuation.find('ns:Retail', namespace).text
            return clean, retail
        else:
            return "n/a", "n/a"
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None

# Processing function for ThreadPoolExecutor
def process_row(session, row, total_valid_rows):
    # Check if any of the required columns have missing or NaN values
    if row.isna().any():
        return None  # Skip processing for this row


    reg_date = datetime.strptime(row['DFR'], '%d/%m/%Y').strftime('%Y-%m-%d')
    rounded_mileage = round_up_to_nearest_thousand(row['CB Mileage'])

    payload = {
        'subscriberId': SUBSCRIBER_ID,
        'password': PASSWORD,
        'database': DATABASE,
        'capid': int(row['Apex CAPID']),
        'valuationDate': VALUATION_DATE,
        'regDate': reg_date,
        'mileage': rounded_mileage
    }

    # Fetch current valuation
    clean, retail = fetch_and_parse_data(session, payload)

    # Fetch old valuation
    payload['valuationDate'] = FIXED_VALUATION_DATE
    clean_old, retail_old = fetch_and_parse_data(session, payload)

    return [row['VRM'], clean_old, retail_old, clean, retail, rounded_mileage, row['Apex CAPID'], reg_date, VALUATION_DATE, FIXED_VALUATION_DATE]

# Prepare for output CSV
output_header = ['VRM', 'Clean_Month', 'Retail_Month', 'Clean_Live', 'Retail_Live', 'Mileage', 'CAP ID', 'DFR', 'Live_Date', 'Month_Date']

# Check if the output file exists and rename it if it does
output_csv_path = os.path.join(OUTPUT_DIR, f"{OUTPUT_CSV_FILENAME.split('.')[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")

if os.path.exists(output_csv_path):
    os.rename(output_csv_path, os.path.join(OUTPUT_DIR, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{OUTPUT_CSV_FILENAME}"))

# Using ThreadPoolExecutor to process rows
with requests.Session() as session:
    with ThreadPoolExecutor(max_workers=20) as executor:
        valid_rows = [row for _, row in df.iterrows() if not row.isna().any()]
        total_valid_rows = len(valid_rows)
        futures = {executor.submit(process_row, session, row, total_valid_rows): row for row in valid_rows}

        # Open the output file to start writing data
        with open(output_csv_path, 'w', newline='') as f_output:
            csv_writer = csv.writer(f_output)
            csv_writer.writerow(output_header)

            # Use tqdm for progress tracking
            for future in tqdm(as_completed(futures), total=total_valid_rows, unit="row"):
                row = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        csv_writer.writerow(result)
                except Exception as exc:
                    logging.error(f'An error occurred: {exc}')
                    with print_lock:
                        print(f'An error occurred with row {row.name + 1} of {total_valid_rows}: {exc}')

# Count the number of rows that were processed
processed_rows = len(valid_rows)
print(f'Total number of rows processed: {processed_rows}')
