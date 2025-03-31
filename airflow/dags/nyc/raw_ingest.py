import os
import json
import time
import logging
import requests

from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("logger")

NYC_DIR = os.path.dirname(__file__)
DAGS_DIR = os.path.dirname(NYC_DIR)
AIRFLOW_DIR = os.path.dirname(DAGS_DIR)
DATA_DIR = os.path.join(AIRFLOW_DIR, "data")


def get_service_requests(params, max_retries=3, backoff_factor=2):
    url = os.environ.get("SERVICE_REQUESTS_URL")
    retry_status_codes = [429, 500, 502, 503, 504]
    
    logging.info(f"Consuming data from {url}...")
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                logging.info(f"OFFSET {params.get('$offset', 'N/A')} - STATUS {response.status_code} - Attempt {attempt + 1}/{max_retries}")
                data = response.json()
                
                if data:
                    logging.info(f"Successfully retrieved {len(data)} records")
                else:
                    logging.info("No more records to consume")
                
                return data
            
            elif response.status_code in retry_status_codes and attempt < max_retries:
                wait_time = backoff_factor ** attempt
                logging.warning(f"Request failed with status code: {response.status_code}. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
                logging.warning(f"Response body: {response.text[:200]}...")
                time.sleep(wait_time)
                continue
            
            else:
                logging.error(f"Request failed with status code: {response.status_code}. No more retries.")
                logging.error(f"Response body: {response.text[:200]}...")
                return []
                
        except requests.exceptions.Timeout:
            if attempt < max_retries:
                wait_time = backoff_factor ** attempt
                logging.warning(f"Request timed out. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logging.error("Request timed out after all retry attempts")
                return []
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = backoff_factor ** attempt
                logging.warning(f"Network error: {str(e)}. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"Network error after all retry attempts: {str(e)}")
                return []
                
        except ValueError as e:
            logging.error(f"Failed to parse JSON response: {str(e)}")
            logging.error(f"Response body: {response.text[:200]}...")
            return []
            
        except Exception as e:
            logging.error(f"Unexpected error processing response: {str(e)}")
            return []
    
    return []


def save_service_requests(data, date_reference, offset):
    try:
        directory = f"{DATA_DIR}/service-requests/{date_reference}"
        os.makedirs(directory, exist_ok=True)

        logging.info(f"Saving {offset}.json into {directory}.")
        with open(f"{directory}/{offset}.json", "w") as f:
            f.write(json.dumps(data, indent=4))
    except Exception as e:
        logging.error(f"Error saving service requests: {str(e)}")
        raise


def consume_service_requests(date_reference, limit=100, initial_offset = 0, max_retries=3):
    offset = initial_offset
    check_records = True
    logging.info(f"Starting to consume service requests from {date_reference} with initial offset {initial_offset}. Limit: {limit}, Max Retries: {max_retries}")
    while check_records:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": f"created_date <= '{date_reference}T23:59:59.999' and created_date > '{date_reference}T00:00:00.000'",
            "$$app_token": os.environ.get("SERVICE_REQUESTS_TOKEN")
        }
        
        try:
            data = get_service_requests(params, max_retries)
            
            if data:
                save_service_requests(data, date_reference, offset)
                offset += limit
            else:
                check_records = False    
            
        except requests.RequestException as e:
            logging.error(f"Network error occurred: {str(e)}")
            logging.error(f"Error at process {params}.")
            check_records = False
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            logging.error(f"Error at process {params}.")
            check_records = False
            

def raw_ingest(**kwargs):
    DATE_REFERENCE = kwargs.get("yesterday_ds")
    LIMIT = kwargs.get("limit") # 1000
    INITIAL_OFFSET = kwargs.get("initial_offset") # 0
    MAX_RETRIES = kwargs.get("max_retries") # 3
    consume_service_requests(DATE_REFERENCE, LIMIT, INITIAL_OFFSET, MAX_RETRIES)