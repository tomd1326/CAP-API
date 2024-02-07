import aiohttp
from aiohttp import TCPConnector
import asyncio
from datetime import datetime, timedelta
import csv
from datetime import datetime
from xml.etree import ElementTree
import os
from collections import OrderedDict
from tqdm.asyncio import tqdm

# Constants
onedrive_path = os.path.join(os.path.expanduser("~"), "OneDrive - Motor Depot")
script_directory = os.path.dirname(os.path.realpath(__file__))  # Get the directory where the script is running
input_file_path = os.path.join(script_directory, '..', '..', 'VRM_Input.csv')  # Updated input file path
output_directory = os.path.join(script_directory, 'Outputs')  # Outputs directory within the script's directory
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Import CAP_config
import sys
sys.path.append(os.path.join(os.path.expanduser("~"), "OneDrive - Motor Depot", "Python Scripts", "CAP"))
import CAP_config


current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file_path = os.path.join(output_directory, f'CAP_VRM_Output_{current_datetime}.csv')
logs_directory = os.path.join(onedrive_path, "Python Scripts", "CAP", "CAP VRM Lookup", "Logs")  # Logs directory within the Outputs directory
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

errors_log_path = os.path.join(logs_directory, f'CAP_VRM_errors_{current_datetime}.log')

# CAP API info
url_monthly = 'https://soap.cap.co.uk/vrm/capvrm.asmx/VRMValuation'  # Monthly values API endpoint
url_live = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'  # Live values API endpoint
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
subscriber_id = CAP_config.SUBSCRIBER_ID  # Updated to use CAP_config
password = CAP_config.PASSWORD         # Updated to use CAP_config


# Function to display progress in KB
def get_file_size_in_kb(file_path):
    return os.path.getsize(file_path) / 1024

def log_api_response(vrm, response, is_live=False):
    log_type = 'live' if is_live else 'monthly'
    log_filename = f'CAP_VRM_{log_type}_responses_{current_datetime}.log'
    log_file_path = os.path.join(logs_directory, log_filename)

    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f"{timestamp}: VRM: {vrm}, Response: {response}\n")

def round_mileage(mileage):
    return round((int(mileage) + 500) / 1000) * 1000

async def post_cap_vrm_request(session, vrm, rounded_mileage):
    data = {
        'SubscriberID': subscriber_id,
        'Password': password,
        'VRM': vrm,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': 'false'
    }
    async with session.post(url_monthly, headers=headers, data=data) as response:
        return await response.text(), response.status, vrm

async def post_cap_request_live_values(session, vrm, capid, registered_date, rounded_mileage):
    data = {
        'subscriberId': subscriber_id,
        'password': password,
        'database': 'CAR',
        'capid': capid,
        'valuationDate': datetime.now().strftime('%Y-%m-%d'),
        'regDate': registered_date,
        'mileage': rounded_mileage
    }
    try:
        async with session.post(url_live, data=data) as response:
            response_text = await response.text()
            return response_text
    except Exception as e:
        print(f"Error during request for VRM {vrm}: {e}")
        return None




def extract_values(response):
    root = ElementTree.fromstring(response)
    namespaces = {'ns': 'https://soap.cap.co.uk/vrm'}


    database = root.find('.//ns:VRMLookup/ns:Database', namespaces)
    capid = root.find('.//ns:VRMLookup/ns:CAPID', namespaces)
    clean = root.find('.//ns:Valuation/ns:Clean', namespaces)
    retail = root.find('.//ns:Valuation/ns:Retail', namespaces)

    capman = root.find('.//ns:VRMLookup/ns:CAPMan', namespaces)
    caprange = root.find('.//ns:VRMLookup/ns:CAPRange', namespaces)
    capmod = root.find('.//ns:VRMLookup/ns:CAPMod', namespaces)
    capder = root.find('.//ns:VRMLookup/ns:CAPDer', namespaces)

    database_text = database.text if database is not None else 'Not Found'
    capid_text = capid.text if capid is not None else 'Not Found'
    clean_text = clean.text if clean is not None else 'Not Found'
    retail_text = retail.text if retail is not None else 'Not Found'

    capman_text = capman.text if capman is not None else 'Not Found'
    caprange_text = caprange.text if caprange is not None else 'Not Found'
    capmod_text = capmod.text if capmod is not None else 'Not Found'
    capder_text = capder.text if capder is not None else 'Not Found'

    registered_date = root.find('.//ns:VRMLookup/ns:RegisteredDate', namespaces)
    if registered_date is not None and registered_date.text:
        # Parse the existing date format
        registered_date_obj = datetime.strptime(registered_date.text, '%Y-%m-%dT%H:%M:%S')
        # Format it to the desired format
        registered_date_text = registered_date_obj.strftime('%d/%m/%Y')
    else:
        registered_date_text = 'Not Found'

    # Existing return statement with the addition of registered_date_text
    return database_text, capid_text, capman_text, caprange_text, capmod_text, capder_text, clean_text, retail_text, registered_date_text

def extract_live_values(response):
    root = ElementTree.fromstring(response)
    namespaces = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}

    clean_live = root.find('.//ns:ValuationDate/ns:Valuations/ns:Valuation/ns:Clean', namespaces)
    retail_live = root.find('.//ns:ValuationDate/ns:Valuations/ns:Valuation/ns:Retail', namespaces)

    clean_live_text = clean_live.text if clean_live is not None else 'Not Found'
    retail_live_text = retail_live.text if retail_live is not None else 'Not Found'

    return clean_live_text, retail_live_text


def log_error(vrm, status_code):
    log_filename = f'CAP_VRM_errors_{current_datetime}.log'
    log_file_path = os.path.join(logs_directory, log_filename)
    
    with open(log_file_path, 'a', encoding='utf-8') as error_file:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        error_file.write(f"{timestamp}: {vrm}, HTTP Status Code: {status_code}\n")

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None

async def process_row(session, row, index, pbar):
    try:
        # Convert column names to lowercase for case-insensitive matching
        vrm_column = next((key for key in row.keys() if key.lower() == 'vrm' or 'reg' in key.lower()), None)
        mileage_column = next((key for key in row.keys() if 'mile' in key.lower()), None)

        if vrm_column is None:
            raise ValueError("No 'VRM' or 'REG' column found in the CSV.")
        
        if mileage_column is None:
            raise ValueError("No column containing 'mile' found in the CSV.")


        # Use the round_mileage function to round the mileage
        rounded_mileage = round_mileage(row[mileage_column])

        response, status_code, vrm = await post_cap_vrm_request(session, row[vrm_column], rounded_mileage)
        database, capid, capman, caprange, capmod, capder, clean, retail, registered_date = extract_values(response)

        # Convert the registered_date to the required format
        formatted_registered_date = convert_date_format(registered_date)
        if not formatted_registered_date:
            raise ValueError(f"Invalid date format for VRM {vrm}")

        live_response = await post_cap_request_live_values(session, vrm, capid, formatted_registered_date, rounded_mileage)
        live_clean, live_retail = extract_live_values(live_response)

        row_to_write = OrderedDict([
            ('VRM', row[vrm_column]),
            ('Unused1', ''),  # Unused column
            ('CAPMan', capman),
            ('CAPMod', capmod),
            ('CAPDer', capder),
            ('RegisteredDate', registered_date),
            ('CAPID', capid),
            ('Mileage', row[mileage_column]),
            ('Unused2', ''),  # Unused column
            ('Unused3', ''),  # Unused column
            ('Unused4', ''),  # Unused column
            ('Unused5', ''),  # Unused column
            ('Unused6', ''),  # Unused column
            ('Unused7', ''),  # Unused column
            ('Unused8', ''),  # Unused column
            ('Unused9', ''),  # Unused column
            ('Monthly_Clean', clean),
            ('Unused10', ''),  # Unused column
            ('Unused11', ''),  # Unused column
            ('Monthly_Retail', retail),
            ('Unused12', ''),  # Unused column
            ('Unused13', ''),  # Unused column
            ('Unused14', ''),  # Unused column
            ('Database', database),  # Unused column
            ('Unused16', ''),  # Unused column
            ('Unused17', ''),  # Unused column
            ('Unused18', ''),  # Unused column
            ('Unused19', ''),  # Unused column
            ('Unused20', ''),  # Unused column
            ('Live_Clean', live_clean),
            ('Unused21', ''),  # Unused column
            ('Unused22', ''),  # Unused column
            ('Live_Retail', live_retail)
        ])

        if capid == 'Not Found':
            log_error(vrm, status_code)

        return index, row_to_write

    except Exception as exc:
        log_error(row[vrm_column], f"Exception: {exc}")
        pbar.update(1)  # Ensure the progress bar is updated even if an exception occurs
        return index, None
    
   
async def process_file():
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)

    # Open the input file
    infile = open(input_file_path, mode='r', newline='', encoding='utf-8-sig')
    reader = csv.DictReader(infile)

    total_rows = sum(1 for row in reader)
    infile.seek(0)  # Reset the file pointer to the beginning

    conn = aiohttp.TCPConnector(limit_per_host=40)  # Adjust the limit as needed

    async with aiohttp.ClientSession(connector=conn) as session:
        with open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=[
                'VRM', 'Unused1', 'CAPMan', 'CAPMod', 'CAPDer', 'RegisteredDate', 
                'CAPID', 'Mileage', 'Unused2', 'Unused3', 'Unused4', 'Unused5', 
                'Unused6', 'Unused7', 'Unused8', 'Unused9', 'Monthly_Clean', 
                'Unused10', 'Unused11', 'Monthly_Retail', 'Unused12', 'Unused13', 
                'Unused14', 'Database', 'Unused16', 'Unused17', 'Unused18', 
                'Unused19', 'Unused20', 'Live_Clean', 'Unused21', 'Unused22', 'Live_Retail'
            ])
            writer.writeheader()

            rows_written = 0  # Initialize the counter for the number of rows written
            with tqdm(total=total_rows, desc="Processing Rows") as pbar:
                batch_size = 50  # Define the batch size
                tasks = []
                for index, row in enumerate(reader):
                    task = asyncio.create_task(process_row(session, row, index, pbar))
                    tasks.append(task)

                    # When batch size is reached, await completion of these tasks
                    if len(tasks) >= batch_size:
                        results = await asyncio.gather(*tasks)
                        rows_in_batch = 0
                        for _, row_to_write in results:
                            if row_to_write is not None:
                                writer.writerow(row_to_write)
                                rows_in_batch += 1  # Increment the counter after each row is written
                        pbar.update(rows_in_batch)  # Update the progress bar by the number of rows processed in this batch
                        tasks.clear()  # Reset the task list for the next batch

                # Process any remaining tasks
                if tasks:
                    results = await asyncio.gather(*tasks)
                    rows_in_batch = 0
                    for _, row_to_write in results:
                        if row_to_write is not None:
                            writer.writerow(row_to_write)
                            rows_in_batch += 1  # Increment the counter after each row is written
                    pbar.update(rows_in_batch)  # Update the progress bar by the number of rows processed in this batch


    infile.close()
    print("All rows processed and CSV file is built.")
    print(f"Total rows written to the output file: {rows_written}")


if __name__ == '__main__':
    asyncio.run(process_file())
