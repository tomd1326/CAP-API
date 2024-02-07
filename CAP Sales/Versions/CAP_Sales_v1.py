import requests
import pandas as pd
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import os
from threading import Lock
from tqdm import tqdm  # Import tqdm

# Update file paths
home_dir = os.path.expanduser('~')
base_path = os.path.join(home_dir, 'OneDrive - Motor Depot', 'Python Scripts', 'CAP', 'CAP Sales')
input_csv_path = os.path.join(base_path, 'CAP_Sales_Input.csv')
output_csv_base_path = os.path.join(base_path, 'Output', 'CAP_Sales_Output.csv')
current_date = datetime.now().strftime("%Y%m%d")
error_log_path = os.path.join(base_path, 'Logs', f'CAP_Sales_errors_{current_date}.log')

# Configure logging
logging.basicConfig(filename=error_log_path, level=logging.ERROR,
                    format='%(asctime)s [Registration:%(registration)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


# Constants
URL = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
SUBSCRIBER_ID = '101148'
PASSWORD = 'DRM148'
DATABASE = 'CAR'
NAMESPACE_USEDVALUESLIVE = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}
NAMESPACE_VRM = {'ns': 'https://soap.cap.co.uk/vrm'}
VRM_URL = 'https://soap.cap.co.uk/vrm/capvrm.asmx/CAPIDValuation'

df = pd.read_csv(input_csv_path)

print_lock = Lock()

def round_up_to_nearest_thousand(mileage):
    return int((mileage + 999) / 1000) * 1000

def detect_and_convert_date_format(date_str):
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    raise ValueError(f"Date format for '{date_str}' not recognized.")

def fetch_valuation(payload, registration, capid, session):
    response = session.post(URL, headers=HEADERS, data=payload)
    if response.status_code != 200:
        logging.error(f"Server returned status code {response.status_code}: {response.content}",
                      extra={'registration': registration})
        return None

    root = ET.fromstring(response.content)

    valuation_date_element = root.find('.//ns:ValuationDate/ns:Date', NAMESPACE_USEDVALUESLIVE)
    if valuation_date_element is not None:
        valuation_date_str = valuation_date_element.text
        valuation_date_obj = datetime.strptime(valuation_date_str, "%Y-%m-%dT%H:%M:%S")
        valuation_date = valuation_date_obj.strftime("%d/%m/%Y")
    else:
        logging.error(f"Valuation date not found in the XML response: {ET.tostring(root).decode()}",
                      extra={'registration': registration})
        return None

    clean_element = root.find('.//ns:Clean', NAMESPACE_USEDVALUESLIVE)
    retail_element = root.find('.//ns:Retail', NAMESPACE_USEDVALUESLIVE)

    if clean_element is not None and retail_element is not None:
        clean = clean_element.text
        retail = retail_element.text
    else:
        clean = retail = ''

    return valuation_date, clean, retail


def fetch_vrm_data(payload, registration, capid, session):
    response = session.post(VRM_URL, headers=HEADERS, data=payload)
    
    if response.status_code != 200:
        logging.error(f"VRM API returned status code {response.status_code}: {response.content}",
                      extra={'registration': registration})
        return None

    root = ET.fromstring(response.content)

    capid_lookup = root.find('.//ns:CAPIDLookup', NAMESPACE_VRM)
    if capid_lookup is None or capid_lookup.find('.//ns:Success', NAMESPACE_VRM).text != 'true':
        logging.error("CAPIDLookup element missing or not successful in VRM API response",
                      extra={'registration': registration})
        return None

    # Extracting individual fields
    cap_man = capid_lookup.find('.//ns:CAPMan', NAMESPACE_VRM).text if capid_lookup.find('.//ns:CAPMan', NAMESPACE_VRM) is not None else ''
    cap_range = capid_lookup.find('.//ns:CAPRange', NAMESPACE_VRM).text if capid_lookup.find('.//ns:CAPRange', NAMESPACE_VRM) is not None else ''
    cap_mod = capid_lookup.find('.//ns:CAPMod', NAMESPACE_VRM).text if capid_lookup.find('.//ns:CAPMod', NAMESPACE_VRM) is not None else ''
    cap_der = capid_lookup.find('.//ns:CAPDer', NAMESPACE_VRM).text if capid_lookup.find('.//ns:CAPDer', NAMESPACE_VRM) is not None else ''
    mod_introduced = capid_lookup.find('.//ns:ModIntroduced', NAMESPACE_VRM).text if capid_lookup.find('.//ns:ModIntroduced', NAMESPACE_VRM) is not None else None
    mod_discontinued = capid_lookup.find('.//ns:ModDiscontinued', NAMESPACE_VRM).text if capid_lookup.find('.//ns:ModDiscontinued', NAMESPACE_VRM) is not None else None
    der_introduced = capid_lookup.find('.//ns:DerIntroduced', NAMESPACE_VRM).text if capid_lookup.find('.//ns:DerIntroduced', NAMESPACE_VRM) is not None else None
    der_discontinued = capid_lookup.find('.//ns:DerDiscontinued', NAMESPACE_VRM).text if capid_lookup.find('.//ns:DerDiscontinued', NAMESPACE_VRM) is not None else None
    cap_code = capid_lookup.find('.//ns:CAPcode', NAMESPACE_VRM).text if capid_lookup.find('.//ns:CAPcode', NAMESPACE_VRM) is not None else ''

    return cap_man, cap_range, cap_mod, cap_der, mod_introduced, mod_discontinued, der_introduced, der_discontinued, cap_code

output_rows = []

# Use tqdm in the ThreadPoolExecutor context
with ThreadPoolExecutor() as executor, requests.Session() as session, tqdm(total=len(df), desc="Processing Rows") as progress:
    futures = []
    for idx, row in enumerate(df.itertuples(), 1):
        reg_date = detect_and_convert_date_format(row.DateFirstRegistered)
        valuation_date = detect_and_convert_date_format(row.ValuationDate)
        rounded_mileage = round_up_to_nearest_thousand(row.Mileage)
        capid = int(row.CapID) if not pd.isna(row.CapID) else None

        payload = {
            'subscriberId': SUBSCRIBER_ID,
            'password': PASSWORD,
            'database': DATABASE,
            'capid': capid,
            'valuationDate': valuation_date,
            'regDate': reg_date,
            'mileage': rounded_mileage
        }
        row = row._asdict()
        row['ValuationDate'] = valuation_date
        df.loc[idx - 1] = pd.Series(row)

        future = executor.submit(fetch_valuation, payload, row['Registration'], capid, session)
        futures.append((future, payload, row, capid))

    for future, payload, row, capid in futures:
        valuation_info = future.result()
        if valuation_info is not None:
            valuation_date, clean, retail = valuation_info
        else:
            valuation_date = clean = retail = ''
        
        rounded_mileage_10000 = None
        if not clean or not retail:
            rounded_mileage_10000 = round(payload['mileage'] / 10000) * 10000
            if rounded_mileage_10000 != payload['mileage']:
                retry_payload = payload.copy()
                retry_payload['mileage'] = rounded_mileage_10000
                # Changed function call to match updated signature
                valuation_info = fetch_valuation(retry_payload, row['Registration'], capid, session)
                if valuation_info is not None:
                    valuation_date, clean, retail = valuation_info

        vrm_payload = {
        'SubscriberID': SUBSCRIBER_ID,
        'Password': PASSWORD,
        'Database': DATABASE,
        'CAPID': capid,
        'RegisteredDate': reg_date,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': False  # Changed from 'No' to False
        }
        vrm_info = fetch_vrm_data(vrm_payload, row['Registration'], capid, session)
        if vrm_info is not None:
            cap_man, cap_range, cap_mod, cap_der, mod_introduced, mod_discontinued, der_introduced, der_discontinued, cap_code = vrm_info
        else:
            cap_man = cap_range = cap_mod = cap_der = mod_introduced = mod_discontinued = der_introduced = der_discontinued = cap_code = ''

        output_row = [row['Registration'], payload['mileage'], capid, reg_date, clean, retail, valuation_date, rounded_mileage_10000,
                      cap_man, cap_range, cap_mod, cap_der, mod_introduced, mod_discontinued, cap_code]
        output_rows.append(output_row)

        progress.update(1)

output_csv_path = output_csv_base_path
base, ext = os.path.splitext(output_csv_base_path)
output_csv_path = f"{base}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"

with open(output_csv_path, 'w', newline='') as f_output:
    csv_writer = csv.writer(f_output)
    csv_writer.writerow(['VRM', 'mileage', 'CAP ID', 'Reg Date', 'Clean', 'Retail', 'ValuationDate', 'Mileage10000',
                     'CAPMan', 'CAPRange', 'CAPMod', 'CAPDer', 'ModIntroduced', 'ModDiscontinued', 'CAP Code'])
    csv_writer.writerows(output_rows)

df.to_csv(input_csv_path, index=False)
