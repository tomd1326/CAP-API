import sys
import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import date
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import glob
import re
from tqdm import tqdm

# Add the CAP_config.py directory to the Python path
sys.path.append(r'D:\Tom\Python Scripts\CAP')

# Now import the variables from CAP_config
from CAP_config import SUBSCRIBER_ID, PASSWORD, FIXED_VALUATION_DATE

# Create a timestamp for the log file
current_date = datetime.now().strftime('%Y-%m-%d %H_%M_%S')
log_file = f'D:/Tom/Python Scripts/CAP/CAP Stock/Logs/CAP_Stock_errors_{current_date}.log'

# Configure logging
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(message)s')

# Constants
URL = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
DATABASE = 'CAR'
VALUATION_DATE = datetime.now().strftime('%Y-%m-%d')
input_excel_pattern = r'C:\Users\Tom\OneDrive - Motor Depot\Pricing\Input Files\vehicles-autoedit*.xlsx'

NAMESPACE = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}

# Load and filter out rows with any blank input data from Excel
input_files = glob.glob(input_excel_pattern)

# Check if there are input files and if they meet the size criteria
valid_input_files = []
for input_file in input_files:
    file_size = os.path.getsize(input_file)  # Get the file size in bytes
    if file_size > 1024 * 1024:  # Check if the file size is greater than 1MB
        valid_input_files.append(input_file)

if not valid_input_files:
    print("No matching Excel files found with pattern that are larger than 1MB.")
    exit()

input_excel_path = input_files[0]

# Select the first valid input file
input_excel_path = valid_input_files[0]

# Read the entire Excel file
df = pd.read_excel(input_excel_path)

# Convert 'StockID' to string
df['StockID'] = df['StockID'].astype(str)

# Add new columns with default values
df['CleanLive'] = 0.0  # Initialize as float
df['RetailLive'] = 0.0  # Initialize as float
df['CleanMonth'] = 0.0  # Initialize as float
df['RetailMonth'] = 0.0  # Initialize as float

# Clean the Price column
df['Price'] = df['Price'].replace({'£': '', ',': ''}, regex=True)
df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0)

# Define the required columns
required_columns = ['Registration', 'DateFirstRegistered', 'Mileage', 'CapID', 'Status']

# Filter out rows with Status 'COURTESY'
df = df[df['Status'] != 'COURTESY']

# Remove data in 'Standard Equipment' and 'Classified Features' columns
df['Standard Equipment'] = ''
df['Classified Features'] = ''
df['Notes'] = ''
df['Optional Extras'] = ''

df.reset_index(drop=True, inplace=True)

max_retries = 0
print_lock = threading.Lock()

# Location history file pattern
location_history_pattern = r'C:\Users\Tom\OneDrive - Motor Depot\Pricing\Input Files\vehicles-location-history*.csv'

def process_location_history(location_history_file):
    # Load the location history file
    location_df = pd.read_csv(location_history_file)

    # Convert 'Stock ID' to string
    location_df['Stock ID'] = location_df['Stock ID'].astype(str)

    # Convert Location column to uppercase
    location_df['Location'] = location_df['Location'].str.upper()

    # Convert Location column to uppercase
    location_df['Location'] = location_df['Location'].str.upper()

    # Specify the date format 'dd/mm/yyyy' when parsing 'Date Arrived'
    location_df['Date Arrived'] = pd.to_datetime(location_df['Date Arrived'], format='%d/%m/%Y')

    # Filter rows with specific locations
    valid_locations = ['READY TODAY', 'FORECOURT', 'GL COMPOUND', 'SF COMPOUND', 'TEMP LOAN CAR', 'COMPANY CAR']
    location_df = location_df[location_df['Location'].isin(valid_locations)]

    # Sort by Date Arrived
    location_df = location_df.sort_values(by='Date Arrived')

    return location_df

# Process location history file
location_history_files = glob.glob(location_history_pattern)
if location_history_files:
    location_history_file = location_history_files[0]  # Assuming there is only one matching file
    location_df = process_location_history(location_history_file)
else:
    print(f"No matching location history files found with pattern: {location_history_pattern}")
    exit()

def lookup_date_arrived(row, location_df):
    stock_id = str(row['StockID'])  # Convert to string for safety
    matching_rows = location_df[location_df['Stock ID'] == stock_id]
    
    if not matching_rows.empty:
        matching_rows = matching_rows.sort_values(by='Date Arrived')
        return matching_rows.iloc[0]['Date Arrived']
    
    return None

# Add a new column 'Date Arrived' to the autoedit file by looking up the Date Arrived from location history
df['Date Arrived'] = df.apply(lookup_date_arrived, args=(location_df,), axis=1)


# Functions to round up mileage
def round_up_to_nearest(mileage, round_to):
    return int((mileage + round_to - 1) / round_to) * round_to

# Function to fetch with retries
def fetch_with_retries(payload, registration, mileage_for_request, capid, reg_date, session, valuation_date_type, round_to):
    response = session.post(URL, headers=HEADERS, data=payload)

    # Check for successful response, proceed only if successful
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        valuation = root.find('.//ns:Valuation', NAMESPACE)

        if valuation is not None:
            clean_element = valuation.find('ns:Clean', NAMESPACE)
            retail_element = valuation.find('ns:Retail', NAMESPACE)
            clean = clean_element.text if clean_element is not None else ''
            retail = retail_element.text if retail_element is not None else ''

            if not clean and round_to == 1000:
                # If Clean value is missing for 1000 rounding, try 10000 rounding
                mileage_for_request = round_up_to_nearest(mileage_for_request, 10000)
                payload['mileage'] = mileage_for_request
                return fetch_with_retries(payload, registration, mileage_for_request, capid, reg_date, session, valuation_date_type, 10000)

            return (valuation_date_type, registration, clean, retail, mileage_for_request if round_to == 10000 else '')
    else:
        logging.error(f"Server returned status code {response.status_code}: {response.content}, Registration: {registration}, Mileage: {mileage_for_request}")

    return None

max_workers = 10

# Define a function to process each row
def process_row(idx, row, df):
    registration = row['Registration']
    reg_date = datetime.strptime(row['DateFirstRegistered'], '%d/%m/%Y').strftime('%Y-%m-%d')
    capid = int(row['CapID'])

    # Create payload for requests
    payload = {
        'subscriberId': SUBSCRIBER_ID,
        'password': PASSWORD,
        'database': DATABASE,
        'capid': capid,
        'regDate': reg_date
    }

    # Initialize variables to store results
    current_valuation = {'clean': '', 'retail': ''}
    fixed_valuation = {'clean': '', 'retail': ''}
    rounded_up_10000 = ''

    # Round up mileage to nearest 1000 for initial request
    rounded_mileage = round_up_to_nearest(row['Mileage'], 1000)
    payload['mileage'] = rounded_mileage
    payload['valuationDate'] = VALUATION_DATE

    # Set up the future task for current valuation date
    future1 = executor.submit(fetch_with_retries, payload.copy(), registration, rounded_mileage, capid, reg_date,
                        session, 'current', 1000)

    # Adjust payload for fixed valuation date and set up future task
    payload['valuationDate'] = FIXED_VALUATION_DATE
    future2 = executor.submit(fetch_with_retries, payload, registration, rounded_mileage, capid, reg_date, session,
                        'fixed', 1000)

    # Process the futures as they complete for each row
    for future in as_completed([future1, future2]):
        result = future.result()
        if result is not None:
            valuation_date_type, registration, clean, retail, rounded_up_10000 = result
            if valuation_date_type == 'current':
                current_valuation['clean'] = clean
                current_valuation['retail'] = retail
            elif valuation_date_type == 'fixed':
                fixed_valuation['clean'] = clean
                fixed_valuation['retail'] = retail

    # Update the dataframe with the results
    df.at[idx, 'CleanLive'] = current_valuation['clean']
    df.at[idx, 'RetailLive'] = current_valuation['retail']
    df.at[idx, 'CleanMonth'] = fixed_valuation['clean']
    df.at[idx, 'RetailMonth'] = fixed_valuation['retail']

# Add new columns to the dataframe
df['CleanLive'] = ''
df['RetailLive'] = ''
df['CleanMonth'] = ''
df['RetailMonth'] = ''

# Function to remove text after "For only" in ExtrasSpec column
def remove_text_after_string(text):
    if isinstance(text, str):  # Check if the value is a string
        match = re.search(r'For only', text)
        if match:
            return text[:match.start()]
    return text

# Check if "ExtrasSpec" column exists in the DataFrame
if 'ExtrasSpec' in df.columns:
    # Apply the remove_text_after_string function to the "ExtrasSpec" column
    df['ExtrasSpec'] = df['ExtrasSpec'].apply(remove_text_after_string)

# Extract the numeric string (ddmmyyyyhhmmss) from the input file name
input_file_datetime = re.search(r'(\d{14})', os.path.basename(input_excel_path))
if input_file_datetime:
    input_file_datetime = input_file_datetime.group()
else:
    input_file_datetime = "unknown_datetime"

# Add today's date to a new column in the DataFrame
df['TodayDate'] = date.today().strftime('%d/%m/%Y')

# Define the output CSV path with the new date and time, excluding the original 14-digit string
output_csv_path = os.path.join(os.path.dirname(input_excel_path), f'vehicles-autoedit_{input_file_datetime}_CAP_Figures.csv')

# Open the CSV file in append mode at the beginning
with open(output_csv_path, 'a', newline='') as f_output, ThreadPoolExecutor(max_workers=max_workers) as executor, requests.Session() as session:
    # Wrap the loop with tqdm for a progress bar
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        # Check if any of the required columns are blank
        if not row[required_columns].isnull().any():
            process_row(idx, row, df)

    # Create a new DataFrame with values only to remove formatting
    df_values_only = pd.DataFrame(df.values, columns=df.columns)

    # Save the updated dataframe to a new CSV file
    df_values_only.to_csv(output_csv_path, index=False)

print(f"Script completed. Processed data saved to {output_csv_path}. Errors and info messages logged to {log_file}")