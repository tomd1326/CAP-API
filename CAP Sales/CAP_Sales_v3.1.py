import aiohttp
import asyncio
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import os
import csv
from tqdm.asyncio import tqdm  # Import tqdm for async

# Update file paths
home_dir = os.path.expanduser('~')
base_path = os.path.join(home_dir, 'OneDrive - Motor Depot', 'Python Scripts', 'CAP', 'CAP Sales')
input_csv_path = os.path.join(base_path, 'CAP_Sales_Input.csv')
output_csv_base_path = os.path.join(base_path, 'Outputs', 'CAP_Sales_Output.csv')
current_date = datetime.now().strftime("%Y%m%d")
error_log_path = os.path.join(base_path, 'Logs', f'CAP_Sales_errors_{current_date}.log')

# Configure logging
logging.basicConfig(filename=error_log_path, level=logging.ERROR,
                    format='%(asctime)s [Registration:%(registration)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


# Constants
LIVE_URL = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
SUBSCRIBER_ID = '101148'
PASSWORD = 'DRM148'
DATABASE = 'CAR'
NAMESPACE_USEDVALUESLIVE = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}
NAMESPACE_VRM = {'ns': 'https://soap.cap.co.uk/vrm'}
VRM_URL = 'https://soap.cap.co.uk/vrm/capvrm.asmx/CAPIDValuation'

df = pd.read_csv(input_csv_path)

def round_up_to_nearest_thousand(mileage):
    return int((mileage + 500) / 1000) * 1000

def detect_and_convert_date_format(date_str):
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    raise ValueError(f"Date format for '{date_str}' not recognized.")

async def fetch_valuation(payload, registration, session):
    async with session.post(LIVE_URL, headers=HEADERS, data=payload) as response:
        if response.status != 200:
            response_text = await response.text()
            logging.error(f"Server returned status code {response.status}: {response_text}",
                          extra={'registration': registration})
            return None

        response_text = await response.text()
        root = ET.fromstring(response_text)

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


async def fetch_vrm_data(payload, registration, session):
    async with session.post(VRM_URL, headers=HEADERS, data=payload) as response:
        if response.status != 200:
            response_text = await response.text()
            logging.error(f"VRM API returned status code {response.status}: {response_text}",
                          extra={'registration': registration})
            return None

        response_text = await response.text()
        root = ET.fromstring(response_text)

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

async def process_row(row, session):
    reg_date = detect_and_convert_date_format(row.DateFirstRegistered)
    sale_valuation_date = detect_and_convert_date_format(row.SaleDate)
    purchase_valuation_date = detect_and_convert_date_format(row.PurchaseDate)    
    rounded_mileage = round_up_to_nearest_thousand(row.Mileage)
    capid = int(row.CAPID) if not pd.isna(row.CAPID) else None

    sale_payload = {
        'subscriberId': SUBSCRIBER_ID,
        'password': PASSWORD,
        'database': DATABASE,
        'capid': capid,
        'valuationDate': sale_valuation_date,
        'regDate': reg_date,
        'mileage': rounded_mileage
    }
    sale_valuation_info = await fetch_valuation(sale_payload, row.Registration, session)
    if sale_valuation_info is not None:
        sale_valuation_date, sale_clean, sale_retail = sale_valuation_info
    else:
        sale_valuation_date = sale_clean = sale_retail = ''

        # Calculate valuations for Purchase Date
    purchase_payload = {
        'subscriberId': SUBSCRIBER_ID,
        'password': PASSWORD,
        'database': DATABASE,
        'capid': capid,
        'valuationDate': purchase_valuation_date,
        'regDate': reg_date,
        'mileage': rounded_mileage
    }
    purchase_valuation_info = await fetch_valuation(purchase_payload, row.Registration, session)
    if purchase_valuation_info is not None:
        purchase_valuation_date, purchase_clean, purchase_retail = purchase_valuation_info
    else:
        purchase_valuation_date = purchase_clean = purchase_retail = ''


    rounded_mileage_10000 = None
    if not clean or not retail:
        rounded_mileage_10000 = round(sale_payload['mileage'] / 10000) * 10000
        if rounded_mileage_10000 != sale_payload['mileage']:
            retry_payload = sale_payload.copy()
            retry_payload['mileage'] = rounded_mileage_10000
            valuation_info = await fetch_valuation(retry_payload, row.Registration, session)
            if valuation_info is not None:
                valuation_date, clean, retail = valuation_info

    vrm_payload = {
        'SubscriberID': SUBSCRIBER_ID,
        'Password': PASSWORD,
        'Database': DATABASE,
        'CAPID': capid,
        'RegisteredDate': reg_date,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': False
    }
    vrm_info = await fetch_vrm_data(vrm_payload, row.Registration, session)
    if vrm_info is not None:
        cap_man, cap_range, cap_mod, cap_der, mod_introduced, mod_discontinued, der_introduced, der_discontinued, cap_code = vrm_info
    else:
        cap_man = cap_range = cap_mod = cap_der = mod_introduced = mod_discontinued = der_introduced = der_discontinued = cap_code = ''

    return [
        row.Registration, sale_payload['mileage'], capid, reg_date,
        sale_clean, sale_retail, sale_valuation_date,
        purchase_clean, purchase_retail, purchase_valuation_date,
        cap_man, cap_range, cap_mod, cap_der, mod_introduced, mod_discontinued, cap_code
    ]




async def main():
    df = pd.read_csv(input_csv_path)
    output_rows = []

    async with aiohttp.ClientSession() as session:
        tasks = [process_row(row, session) for row in df.itertuples()]
        for output_row in tqdm(asyncio.as_completed(tasks), total=len(df), desc="Processing Rows"):
            result = await output_row
            output_rows.append(result)

    # Writing output CSV file
    output_csv_path = f"{os.path.splitext(output_csv_base_path)[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    with open(output_csv_path, 'w', newline='') as f_output:
        csv_writer = csv.writer(f_output)
        csv_writer.writerow([
            'VRM', 'mileage', 'CAP ID', 'Reg Date',
            'SaleClean', 'SaleRetail', 'SaleValuationDate',
            'PurchaseClean', 'PurchaseRetail', 'PurchaseValuationDate',
            'CAPMan', 'CAPRange', 'CAPMod', 'CAPDer', 'ModIntroduced', 'ModDiscontinued', 'CAP Code'
        ])
        csv_writer.writerows(output_rows)

    df.to_csv(input_csv_path, index=False)

if __name__ == '__main__':
    asyncio.run(main())
