import aiohttp
import asyncio
import csv
from datetime import datetime
from xml.etree import ElementTree
import os
from collections import OrderedDict
from tqdm.asyncio import tqdm
from datetime import datetime

# Constants
onedrive_path = os.path.join(os.path.expanduser("~"), "OneDrive - Motor Depot")
script_directory = os.path.dirname(os.path.realpath(__file__))  # Get the directory where the script is running
input_file_path = os.path.join(onedrive_path, "Python Scripts", "CAP", "CAP VRM Lookup", "CAP_VRM_Input.csv")
output_directory = os.path.join(script_directory, 'Outputs')  # Outputs directory within the script's directory
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file_path = os.path.join(output_directory, f'CAP_VRM_Output_{current_datetime}.csv')
logs_directory = os.path.join(onedrive_path, "Python Scripts", "CAP", "CAP VRM Lookup", "Logs")  # Logs directory within the Outputs directory
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

errors_log_path = os.path.join(logs_directory, f'CAP_VRM_errors_{current_datetime}.log')
url_monthly = 'https://soap.cap.co.uk/vrm/capvrm.asmx/VRMValuation'  # Monthly values API endpoint
url_live = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'  # Live values API endpoint
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
subscriber_id = '101148'
password = 'DRM148'

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


async def post_request(session, vrm, mileage):
    rounded_mileage = round((int(mileage) + 999) / 1000) * 1000
    data = {
        'SubscriberID': subscriber_id,
        'Password': password,
        'VRM': vrm,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': 'false'
    }
    async with session.post(url_monthly, headers=headers, data=data) as response:
        return await response.text(), response.status, vrm
    
# Modified post_request_live_values function
async def post_request_live_values(session, vrm, capid, registered_date, mileage):
    valuation_date = datetime.now().strftime('%Y-%m-%d')

    # Apply the same rounding logic as in the post_request function
    rounded_mileage = round((int(mileage) + 999) / 1000) * 1000

    data = {
        'subscriberId': subscriber_id,
        'password': password,
        'database': 'CAR',
        'capid': capid,
        'valuationDate': valuation_date,
        'regDate': registered_date,
        'mileage': rounded_mileage  # Use the rounded mileage here
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

    capid = root.find('.//ns:VRMLookup/ns:CAPID', namespaces)
    clean = root.find('.//ns:Valuation/ns:Clean', namespaces)
    retail = root.find('.//ns:Valuation/ns:Retail', namespaces)

    capman = root.find('.//ns:VRMLookup/ns:CAPMan', namespaces)
    caprange = root.find('.//ns:VRMLookup/ns:CAPRange', namespaces)
    capmod = root.find('.//ns:VRMLookup/ns:CAPMod', namespaces)
    capder = root.find('.//ns:VRMLookup/ns:CAPDer', namespaces)

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
    return capid_text, capman_text, caprange_text, capmod_text, capder_text, clean_text, retail_text, registered_date_text

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

async def process_row(session, writer, row, index, pbar):
    try:
        response, status_code, vrm = await post_request(session, row['VRM'], row['Mileage'])
        capid, capman, caprange, capmod, capder, clean, retail, registered_date = extract_values(response)

        # Convert the registered_date to the required format
        formatted_registered_date = convert_date_format(registered_date)
        if not formatted_registered_date:
            raise ValueError(f"Invalid date format for VRM {vrm}")

        live_response = await post_request_live_values(session, vrm, capid, formatted_registered_date, row['Mileage'])
        live_clean, live_retail = extract_live_values(live_response)

        row_to_write = OrderedDict([
            ('VRM', row['VRM']),
            ('Mileage', row['Mileage']),
            ('RegisteredDate', registered_date),
            ('CAPID', capid),
            ('CAPMan', capman),
            ('CAPRange', caprange),
            ('CAPMod', capmod),
            ('CAPDer', capder),
            ('Monthly_Clean', clean),
            ('Monthly_Retail', retail),
            ('Live_Clean', live_clean),   # New field for live clean value
            ('Live_Retail', live_retail)  # New field for live retail value
        ])

        if capid == 'Not Found':
            log_error(vrm, status_code)

        return index, row_to_write

    except Exception as exc:
        log_error(row['VRM'], f"Exception: {exc}")
        return index, None
    
async def process_file():
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)

    # Determine the total number of rows in the CSV file (excluding header)
    with open(input_file_path, 'r', encoding='utf-8-sig') as f:
        total_rows = sum(1 for row in f) - 1  # Subtract 1 for the header

    async with aiohttp.ClientSession() as session:
        with open(input_file_path, mode='r', newline='', encoding='utf-8-sig') as infile, \
             open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=['VRM', 'Mileage', 'RegisteredDate', 'CAPID', 'CAPMan', 'CAPRange', 'CAPMod', 'CAPDer', 'Monthly_Clean', 'Monthly_Retail', 'Live_Clean', 'Live_Retail'])
            writer.writeheader()

            with tqdm(total=total_rows, desc="Processing Rows") as pbar:
                tasks = []
                for index, row in enumerate(reader):
                    task = asyncio.create_task(process_row(session, writer, row, index, pbar))
                    tasks.append(task)

                results = await asyncio.gather(*tasks)  # Wait for all tasks to complete and gather results
                results.sort(key=lambda x: x[0])  # Sort results based on index to maintain original order

                rows_written = 0  # Initialize the counter for the number of rows written
                for _, row_to_write in results:
                    if row_to_write is not None:
                        writer.writerow(row_to_write)  # Write each processed row to the CSV file
                        rows_written += 1  # Increment the counter after each row is written
                        pbar.update(1)  # Update the progress bar after each row is written

            print("All rows processed and CSV file is built.")
            print(f"Total rows written to the output file: {rows_written}")





if __name__ == '__main__':
    asyncio.run(process_file())
