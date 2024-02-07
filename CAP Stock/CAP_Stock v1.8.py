import sys
import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, date
import logging
import glob
import re
from tqdm import tqdm
import shutil
import aiohttp
import asyncio

# Get the home directory of the current user
home_directory = os.path.expanduser('~')

# Add the 'CAP' directory to the Python path
script_directory = os.path.dirname(os.path.realpath(__file__))
cap_config_directory = os.path.join(script_directory, '..', '..', 'CAP')
sys.path.append(cap_config_directory)

# Now import the variables from CAP_config
from CAP_config import SUBSCRIBER_ID, PASSWORD, FIXED_VALUATION_DATE

# Create a timestamp for the log file
current_date = datetime.now().strftime('%Y-%m-%d %H_%M_%S')
log_file = os.path.join(script_directory, 'Logs', f'CAP_Stock_errors_{current_date}.log')

# Configure logging
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(message)s')

# Constants
LIVE_URL = 'https://soap.cap.co.uk/usedvalueslive/capusedvalueslive.asmx/GetUsedLive_IdRegDateMileage'
CAPID_URL = '	https://soap.cap.co.uk/vrm/capvrm.asmx/CAPIDValuation'
VRM_URL = 'https://soap.cap.co.uk/vrm/capvrm.asmx/VRMValuation'
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
DATABASE = 'CAR'
VALUATION_DATE = datetime.now().strftime('%Y-%m-%d')

if4c_excel_path = os.path.join(home_directory, 'OneDrive - Motor Depot', 'Pricing', 'Input Files', 'IF4C.xlsx')
input_excel_pattern = os.path.join(home_directory, 'OneDrive - Motor Depot', 'Pricing', 'Input Files', 'vehicles-autoedit*.xlsx')
location_history_pattern = os.path.join(home_directory, 'OneDrive - Motor Depot', 'Pricing', 'Input Files', 'vehicles-location-history*.csv')

NAMESPACE = {'ns': 'https://soap.cap.co.uk/usedvalueslive'}

# Load and filter out rows with any blank input data from Excel
input_files = glob.glob(input_excel_pattern)

if not input_files:
    print("No matching Excel files found with pattern.")
    exit()

# Select the first valid input file
input_excel_path = input_files[0]

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

# Location history file pattern
location_history_pattern = os.path.join(home_directory, 'OneDrive - Motor Depot', 'Pricing', 'Input Files', 'vehicles-location-history*.csv')

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

class LiveURLHandler:
    @staticmethod
    async def fetch_live_valuation(payload, registration, mileage_for_request, capid, reg_date, session, valuation_date_type, round_to):
        async with session.post(LIVE_URL, headers=HEADERS, data=payload) as response:
            # Check for successful response, proceed only if successful
            if response.status == 200:
                content = await response.text()
                root = ET.fromstring(content)
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
                        return await LiveURLHandler.fetch_live_valuation(payload, registration, mileage_for_request, capid, reg_date, session, valuation_date_type, 10000)

                    return (valuation_date_type, registration, clean, retail, mileage_for_request if round_to == 10000 else '')
            else:
                content = await response.text()
                logging.error(f"Server returned status code {response.status}: {content}, Registration: {registration}, Mileage: {mileage_for_request}")



# Define a function to process each row
async def process_row(idx, row, df, session):
    registration = row['Registration']
    reg_date = datetime.strptime(row['DateFirstRegistered'], '%d/%m/%Y').strftime('%Y-%m-%d')
    capid = int(row['CapID'])

    # Initialize variables to store results
    current_valuation = {'clean': '', 'retail': ''}
    fixed_valuation = {'clean': '', 'retail': ''}

    # Round up mileage to nearest 1000 for initial request
    rounded_mileage = round_up_to_nearest(row['Mileage'], 1000)

    # Create payload for current valuation
    current_payload = {
        'subscriberId': SUBSCRIBER_ID,
        'password': PASSWORD,
        'database': DATABASE,
        'capid': capid,
        'regDate': reg_date,
        'mileage': rounded_mileage,
        'valuationDate': VALUATION_DATE
    }

    # Create payload for fixed valuation
    fixed_payload = current_payload.copy()
    fixed_payload['valuationDate'] = FIXED_VALUATION_DATE

    # Set up async tasks for current valuation date and fixed valuation date
    task1 = asyncio.create_task(
        LiveURLHandler.fetch_live_valuation(current_payload, registration, rounded_mileage, capid, reg_date, session, 'current', 1000)
    )
    task2 = asyncio.create_task(
        LiveURLHandler.fetch_live_valuation(fixed_payload, registration, rounded_mileage, capid, reg_date, session, 'fixed', 1000)
    )

    # Await both tasks and process results
    results = await asyncio.gather(task1, task2)
    # Process results and update the dataframe with the results
    for result in results:
        if result is not None:
            valuation_date_type, registration, clean, retail, _ = result
            if valuation_date_type == 'current':
                current_valuation['clean'] = clean
                current_valuation['retail'] = retail
            elif valuation_date_type == 'fixed':
                fixed_valuation['clean'] = clean
                fixed_valuation['retail'] = retail

    # Ensure the values are of float type before assigning them to the DataFrame
    df.at[idx, 'CleanLive'] = pd.to_numeric(current_valuation['clean'], errors='coerce')
    df.at[idx, 'RetailLive'] = pd.to_numeric(current_valuation['retail'], errors='coerce')
    df.at[idx, 'CleanMonth'] = pd.to_numeric(fixed_valuation['clean'], errors='coerce')
    df.at[idx, 'RetailMonth'] = pd.to_numeric(fixed_valuation['retail'], errors='coerce')



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
input_file_name = os.path.basename(input_excel_path)
input_file_datetime_match = re.search(r'(\d{14})', input_file_name)
if input_file_datetime_match:
    input_file_datetime = input_file_datetime_match.group()
    input_file_datetime = datetime.strptime(input_file_datetime, '%d%m%Y%H%M%S').strftime('%Y_%m_%d_%H%M%S')
else:
    input_file_datetime = "unknown_datetime"

# Add today's date to a new column in the DataFrame
df['TodayDate'] = date.today().strftime('%d/%m/%Y')

# Define the output CSV path dynamically
output_dir = os.path.join(home_directory, r'OneDrive - Motor Depot\Pricing\Input Files')
output_csv_filename = f'vehicles-autoedit_{input_file_datetime}_CAP_Figures.csv'
output_csv_path = os.path.join(output_dir, output_csv_filename)

# Check if the output file already exists
while os.path.exists(output_csv_path):
    # Rename the new file with a timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    renamed_filename = f'vehicles-autoedit_{input_file_datetime}_CAP_Figures_{timestamp}.csv'
    renamed_output_csv_path = os.path.join(output_dir, renamed_filename)
    shutil.move(output_csv_path, renamed_output_csv_path)
    print(f"Output file already exists. Renamed to {renamed_output_csv_path}")

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, row in df.iterrows():
            if not row[required_columns].isnull().any():
                tasks.append(asyncio.create_task(process_row(idx, row, df, session)))

        # Create a progress bar for the tasks
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing rows"):
            await f  # Await the completion of each task and update the progress bar

        # Create a new DataFrame with values only to remove formatting
        df_values_only = pd.DataFrame(df.values, columns=df.columns)

        # Save the updated dataframe to a new CSV file
        df_values_only.to_csv(output_csv_path, index=False)

        print(f"Script completed. Processed data saved to {output_csv_path}. Errors and info messages logged to {log_file}")


# Run the main async function
asyncio.run(main())

# Define the destination directory in OneDrive\Apex\
apex_dir = os.path.join(home_directory, 'OneDrive - Motor Depot', 'Exports', 'Apex Stock')

# Create the directory if it doesn't exist
os.makedirs(apex_dir, exist_ok=True)

# Define the destination path for the copy of the output CSV file
output_csv_copy_path = os.path.join(apex_dir, output_csv_filename)

# Copy the output CSV file to the Apex directory
shutil.copy(output_csv_path, output_csv_copy_path)

print(f"Copy of the output file saved to {output_csv_copy_path} in Apex directory.")

