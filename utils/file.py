import pandas as pd
import yaml
import json 

# LOGGER
from utils.logger import get_logger
logger = get_logger(__name__)

def read_csv(file_path):
    print ( "file_path : " ,file_path)
    
    """Reads a CSV file and returns a DataFrame"""
    try:
        data = pd.read_csv(file_path)
        return True, data.to_json(orient='records')
    except FileNotFoundError as e:
        msg= f"File not found: {file_path} - {e}"
        logger.error(msg)
        return False, str(e)
    except pd.errors.EmptyDataError:
        msg = f"No data: {file_path} is empty."
        logger.warning(msg)
        return False, msg
    
def read_yaml(file_path):
    """Reads a YAML file and returns the data"""
    
    try:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            return True, data
    except FileNotFoundError as e:
        msg= f"File not found: {file_path} - {e}"
        logger.error(msg)
        return False, str(e)
    except yaml.YAMLError as e:
        msg = f"YAML error in file: {file_path} - {e}"
        logger.error(msg)
        return False, str(e)
    
    
def write_json(file_path, data):
    """Writes data to a JSON file"""
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        return True, f"Data successfully written to {file_path}"
    except Exception as e:
        msg = f"Error writing to JSON file: {file_path} - {e}"
        logger.error(msg)
        return False, str(e)