import asyncio
import aiohttp
import pandas as pd
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import datetime, timedelta
import logging
import os
import sys
from tqdm import tqdm
import re

# Get the current script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory (one level up)
parent_dir = os.path.dirname(script_dir)

# Constructing the paths using the parent directory
cap_config_path = os.path.join(parent_dir)
log_dir = os.path.join(script_dir, 'Logs')
input_dir = script_dir
output_dir = os.path.join(script_dir, 'Outputs')

# Add the CAP_config.py directory to the Python path
sys.path.append(cap_config_path)


# Now import the variables from CAP_config
from CAP_config import SUBSCRIBER_ID, PASSWORD, FIXED_VALUATION_DATE

# Set the log file directory with the date at the end
log_filename = f'CAPID_Lookup_errors_{datetime.now().strftime("%Y%m%d")}.log'
log_path = os.path.join(log_dir, log_filename)

# Configure logging to use the updated log file path
logging.basicConfig(filename=log_path, level=logging.ERROR)

# Constants
LIVE_VALUATION_URL = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
CAPID_VALUATION_URL = "https://soap.cap.co.uk/vrm/capvrm.asmx/CAPIDValuation"
VRM_URL = "	https://soap.cap.co.uk/vrm/capvrm.asmx/VRMValuation"
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
DATABASE = 'CAR'
VALUATION_DATE = datetime.now().strftime('%Y-%m-%d')
INPUT_CSV_FILENAME = 'CAPID_Lookup_Input.csv'
OUTPUT_CSV_FILENAME = 'CAPID_Lookup_Output.csv'


# Read input CSV
input_csv_path = os.path.join(input_dir, INPUT_CSV_FILENAME)
df = pd.read_csv(input_csv_path)

mileage_column = next((col for col in df.columns if re.search(r'mile', col, re.IGNORECASE)), None)
capid_column = next((col for col in df.columns if re.search(r'capid', col, re.IGNORECASE)), None)
vrm_column = next((col for col in df.columns if re.search(r'vrm|reg', col, re.IGNORECASE)), None)


if mileage_column is None:
    print("Mileage column not found in the input file.")
    sys.exit(1)

if capid_column is None:
    print("CAPID column not found in the input file.")
    sys.exit(1)

if vrm_column is None:
    print("VRM/Reg column not found in the input file.")
    sys.exit(1)

def convert_excel_date(serial):
    excel_epoch = datetime(1899, 12, 30)  # Excel's epoch starts on January 1, 1900, but there's an off-by-two error
    converted_date = excel_epoch + timedelta(days=serial)
    return converted_date.strftime('%d/%m/%Y')

def is_excel_date_format(date_str):
    try:
        int(date_str)
        return True
    except ValueError:
        return False

# Function to round up mileage to nearest 1000 miles
def round_up_to_nearest_thousand(mileage):
    return int((mileage + 999) / 1000) * 1000

# Function to fetch and parse the response from the API
async def fetch_and_parse_data(session, url, payload):
    try:
        async with session.post(url, headers=HEADERS, data=payload) as response:
            response.raise_for_status()  # Raise an exception for non-200 status codes
            content = await response.text()
            root = ET.fromstring(content)

            if url == LIVE_VALUATION_URL:
                namespace = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}
                valuation = root.find('.//ns:Valuation', namespace)
                if valuation is not None:
                    clean = valuation.find('ns:Clean', namespace).text
                    retail = valuation.find('ns:Retail', namespace).text
                    return {'clean': clean, 'retail': retail}
                else:
                    return {"clean": "n/a", "retail": "n/a"}

            elif url == CAPID_VALUATION_URL:
                namespace = {'ns': 'https://soap.cap.co.uk/vrm'}
                capid_lookup = root.find('.//ns:CAPIDLookup', namespace)
                if capid_lookup is not None:
                    cap_man = capid_lookup.find('ns:CAPMan', namespace).text
                    cap_mod = capid_lookup.find('ns:CAPMod', namespace).text
                    cap_der = capid_lookup.find('ns:CAPDer', namespace).text
                    return {'CAPMan': cap_man, 'CAPMod': cap_mod, 'CAPDer': cap_der}
                else:
                    return {"CAPMan": "n/a", "CAPMod": "n/a", "CAPDer": "n/a"}
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return {"error": "error"}


async def process_row(session, row, total_valid_rows):
    # Check if any of the required columns have missing or NaN values
    if row.isna().any():
        return None  # Skip processing for this row
    
        # Check if the date is in the 5-digit Excel format and convert if necessary
    if is_excel_date_format(row['DFR']):
        row['DFR'] = convert_excel_date(int(row['DFR']))

    reg_date = datetime.strptime(row['DFR'], '%d/%m/%Y').strftime('%Y-%m-%d')
    rounded_mileage = round_up_to_nearest_thousand(row[mileage_column])
    capid_value = row[capid_column]
    vrm_value = row[vrm_column]


    live_payload = {
        'subscriberId': SUBSCRIBER_ID,
        'password': PASSWORD,
        'database': DATABASE,
        'capid': int(row['CAPID']),
        'valuationDate': VALUATION_DATE,
        'regDate': reg_date,
        'mileage': rounded_mileage
    }

    # Fetch current valuation from LIVE_VALUATION_URL
    live_data = await fetch_and_parse_data(session, LIVE_VALUATION_URL, live_payload)
    if "error" in live_data:
        return None  # Skip this row due to error

    # Extracting the clean and retail values for the current valuation
    clean_live = live_data.get('clean', 'n/a')
    retail_live = live_data.get('retail', 'n/a')

    # Fetch old valuation from LIVE_VALUATION_URL
    live_payload['valuationDate'] = FIXED_VALUATION_DATE
    live_old_data = await fetch_and_parse_data(session, LIVE_VALUATION_URL, live_payload)
    if "error" in live_old_data:
        return None  # Skip this row due to error

    # Extracting the clean and retail values for the old valuation
    clean_month = live_old_data.get('clean', 'n/a')
    retail_month = live_old_data.get('retail', 'n/a')
    
    # Construct payload for CAPID_VALUATION_URL
    capid_payload = {
        'SubscriberID': SUBSCRIBER_ID,
        'Password': PASSWORD,
        'Database': DATABASE,
        'CAPID': int(row[capid_column]),  # Use the extracted CAPID column value
        'RegisteredDate': reg_date,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': False  # Set to True if you need standard equipment data
    }

    # Fetch data from CAPID_VALUATION_URL
    capid_data = await fetch_and_parse_data(session, CAPID_VALUATION_URL, capid_payload)
    if "error" in capid_data:
        return None  # Skip this row due to error

    return [row[vrm_column], None, capid_data['CAPMan'], capid_data['CAPMod'], capid_data['CAPDer'], row['DFR'],
            row['CAPID'], row['Mileage'], None, None, None, None, None, None, None, None,
            clean_month, None, None, retail_month, None, None, None, None, None, None,
            None, None, None, clean_live, None, None, retail_live, VALUATION_DATE, FIXED_VALUATION_DATE]



# Prepare for output CSV with updated headers
output_header = [
    "VRM", "Unused1", "CAPMan", "CAPMod", "CAPDer", "DFR", "CAPID", "Mileage",
    "Unused2", "Unused3", "Unused4", "Unused5", "Unused6", "Unused7", "Unused8",
    "Unused9", "Clean_Month", "Unused10", "Unused11", "Retail_Month", "Unused12",
    "Unused13", "Unused14", "Unused15", "Unused16", "Unused17", "Unused18", "Unused19",
    "Unused20", "Clean_Live", "Unused21", "Unused22", "Retail_Live", "Live_Date",
    "Month_Date"
]

# Check if the output file exists and rename it if it does
output_csv_path = os.path.join(output_dir, f"{OUTPUT_CSV_FILENAME.split('.')[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")

if os.path.exists(output_csv_path):
    os.rename(output_csv_path, os.path.join(output_dir, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{OUTPUT_CSV_FILENAME}"))


# Async function to process all rows
async def process_all_rows():
    async with aiohttp.ClientSession() as session:
        valid_rows = [row for _, row in df.iterrows() if not row.isna().any()]
        total_valid_rows = len(valid_rows)

        tasks = [process_row(session, row, total_valid_rows) for row in valid_rows]
        responses = []

        for future in tqdm(asyncio.as_completed(tasks), total=total_valid_rows, unit="row"):
            result = await future
            if result is not None:
                responses.append(result)
        return responses

# Function to run the async process_all_rows and write to CSV
def main():
    results = asyncio.run(process_all_rows())

    with open(output_csv_path, 'w', newline='') as f_output:
        csv_writer = csv.writer(f_output)
        csv_writer.writerow(output_header)
        for result in results:
            csv_writer.writerow(result)

    print(f'Total number of rows processed: {len(results)}')

if __name__ == "__main__":
    main()