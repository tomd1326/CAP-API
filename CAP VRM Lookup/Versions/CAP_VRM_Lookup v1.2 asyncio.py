import aiohttp
import asyncio
import csv
from datetime import datetime
from xml.etree import ElementTree
import os
from collections import OrderedDict
from tqdm.asyncio import tqdm

# Constants
input_file_path = 'D:\\Tom\\Python Scripts\\CAP\\CAP VRM Lookup\\CAP_VRM_Input.csv'
current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file_path = f'D:\\Tom\\Python Scripts\\CAP\\CAP VRM Lookup\\CAP_VRM_Output_{current_datetime}.csv'
logs_directory = 'D:\\Tom\\Python Scripts\\CAP\\CAP VRM Lookup\\Logs'  # New directory for logs
errors_log_path = os.path.join(logs_directory, f'CAP_VRM_errors_{current_datetime}.log')  # Updated errors log path
url = 'https://soap.cap.co.uk/vrm/capvrm.asmx/VRMValuation'
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
subscriber_id = '101148'
password = 'DRM148'

# Function to display progress in KB
def get_file_size_in_kb(file_path):
    return os.path.getsize(file_path) / 1024

async def post_request(session, vrm, mileage):
    rounded_mileage = round((int(mileage) + 999) / 1000) * 1000
    data = {
        'SubscriberID': subscriber_id,
        'Password': password,
        'VRM': vrm,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': 'false'
    }
    async with session.post(url, headers=headers, data=data) as response:
        return await response.text(), response.status, vrm
    
async def post_request_live_values(session, capid, registered_date, mileage):
    valuation_date = datetime.now().strftime('%Y-%m-%d')
    data = {
        'subscriberId': subscriber_id,
        'password': password,
        'database': 'CAR',
        'capid': capid,
        'valuationDate': valuation_date,
        'regDate': registered_date,
        'mileage': mileage
    }
    url_live_values = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
    async with session.post(url_live_values, data=data) as response:
        return await response.text()


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
    registered_date_text = registered_date.text if registered_date is not None else 'Not Found'

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

async def process_file():
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
    
    async with aiohttp.ClientSession() as session:
        with open(input_file_path, mode='r', newline='', encoding='utf-8-sig') as infile, \
                open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=['VRM', 'Mileage', 'RegisteredDate', 'CAPID', 'CAPMan', 'CAPRange', 'CAPMod', 'CAPDer', 'Clean', 'Retail'])
            writer.writeheader()

            tasks = []
            for index, row in enumerate(reader):
                task = asyncio.create_task(process_row(session, writer, row, index))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
            results.sort(key=lambda x: x[0])  # Sort by index

            for _, row_to_write in results:
                writer.writerow(row_to_write)

            print("All rows processed and CSV file is built.")

async def process_row(session, writer, row, index):
    try:
        response, status_code, vrm = await post_request(session, row['VRM'], row['Mileage'])
        capid, capman, caprange, capmod, capder, clean, retail, registered_date = extract_values(response)

        row_to_write = OrderedDict([
            ('VRM', row['VRM']),
            ('Mileage', row['Mileage']),
            ('RegisteredDate', registered_date),
            ('CAPID', capid),
            ('CAPMan', capman),
            ('CAPRange', caprange),
            ('CAPMod', capmod),
            ('CAPDer', capder),
            ('Clean', clean),
            ('Retail', retail)
        
        ])

        if capid == 'Not Found':
            log_error(vrm, status_code)

        return index, row_to_write

    except Exception as exc:
        log_error(row['VRM'], f"Exception: {exc}")
        return index, None

if __name__ == '__main__':
    asyncio.run(process_file())
