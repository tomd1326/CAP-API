from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import requests
from xml.etree import ElementTree
import os

# Constants
input_file_path = 'D:\\Tom\\Python Scripts\\CAP\\CAP VRM Lookup\\CAP_VRM_Input.csv'
output_file_path = 'D:\\Tom\\Python Scripts\\CAP\\CAP VRM Lookup\\CAP_VRM_Output.csv'
errors_log_path = 'D:\\Tom\\Python Scripts\\CAP\\CAP VRM Lookup\\CAP_VRM_errors.log'
url = 'https://soap.cap.co.uk/vrm/capvrm.asmx/VRMValuation'
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
subscriber_id = '101148'
password = 'DRM148'

# Function to display progress in KB
def get_file_size_in_kb(file_path):
    return os.path.getsize(file_path) / 1024

def post_request(vrm, mileage):
    rounded_mileage = round((int(mileage) + 999) / 1000) * 1000  # Round mileage up to nearest 1000
    data = {
        'SubscriberID': subscriber_id,
        'Password': password,
        'VRM': vrm,
        'Mileage': rounded_mileage,
        'StandardEquipmentRequired': 'false'
    }
    response = requests.post(url, headers=headers, data=data)
    return response.text, response.status_code, vrm

def extract_values(response):
    # Parse the XML with namespaces
    root = ElementTree.fromstring(response)
    # Extract namespaces from the XML
    namespaces = {'ns': 'https://soap.cap.co.uk/vrm'}  # Add the correct namespace URL

    # Find the CAPID and other elements using the namespace
    capid = root.find('.//ns:VRMLookup/ns:CAPID', namespaces)
    clean = root.find('.//ns:Valuation/ns:Clean', namespaces)
    retail = root.find('.//ns:Valuation/ns:Retail', namespaces)

    # Extract additional elements from the namespace VRMLookup
    capman = root.find('.//ns:VRMLookup/ns:CAPMan', namespaces)
    caprange = root.find('.//ns:VRMLookup/ns:CAPRange', namespaces)
    capmod = root.find('.//ns:VRMLookup/ns:CAPMod', namespaces)
    capder = root.find('.//ns:VRMLookup/ns:CAPDer', namespaces)

    # Extract text or 'Not Found' if elements are None
    capid_text = capid.text if capid is not None else 'Not Found'
    clean_text = clean.text if clean is not None else 'Not Found'
    retail_text = retail.text if retail is not None else 'Not Found'

    # Extract additional values or 'Not Found' if elements are None
    capman_text = capman.text if capman is not None else 'Not Found'
    caprange_text = caprange.text if caprange is not None else 'Not Found'
    capmod_text = capmod.text if capmod is not None else 'Not Found'
    capder_text = capder.text if capder is not None else 'Not Found'

    return capid_text, capman_text, caprange_text, capmod_text, capder_text, clean_text, retail_text

def log_error(vrm, status_code):
    with open(errors_log_path, 'a', encoding='utf-8') as error_file:
        error_file.write(f"{vrm}, HTTP Status Code: {status_code}\n")

def process_file():
    with open(input_file_path, mode='r', newline='', encoding='utf-8-sig') as infile, \
            open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=['VRM', 'Mileage', 'CAPID', 'CAPMan', 'CAPRange', 'CAPMod', 'CAPDer', 'Clean', 'Retail'])
        writer.writeheader()

        total_rows = sum(1 for _ in reader)  # Count total rows
        infile.seek(0)  # Reset file pointer to the beginning
        next(reader)  # Skip header row

        processed_rows = 0
        total_size_kb = get_file_size_in_kb(input_file_path)
        processed_size_kb = 0

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_row = {executor.submit(post_request, row['VRM'], row['Mileage']): row for row in reader}

            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    response, status_code, vrm = future.result()
                    capid, capman, caprange, capmod, capder, clean, retail = extract_values(response)
                    writer.writerow({
                        'VRM': vrm,
                        'Mileage': row['Mileage'],
                        'CAPID': capid,
                        'CAPMan': capman,
                        'CAPRange': caprange,
                        'CAPMod': capmod,
                        'CAPDer': capder,
                        'Clean': clean,
                        'Retail': retail
                    })
                    if capid == 'Not Found':
                        log_error(vrm, status_code)

                    processed_rows += 1
                    processed_size_kb += get_file_size_in_kb(output_file_path)
                    print(f"Row {processed_rows} of {total_rows} processed - VRM: {vrm}")

                except Exception as exc:
                    log_error(row['VRM'], f"Exception: {exc}")

        print(f"All rows processed. Building the CSV file...")
        final_size_kb = get_file_size_in_kb(output_file_path)
        print(f"Processed: {final_size_kb:.2f} KB / Total: {total_size_kb:.2f} KB")

if __name__ == '__main__':
    process_file()
