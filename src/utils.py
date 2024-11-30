import requests
import logging
import sys
import uuid
import yaml
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout, ConnectionError
from src.exception import CustomException  
from src.logger import logging



def get_parsed_html(url: str) -> BeautifulSoup:
    """
    Fetches and parses HTML content from a given URL.

    Args:
        url (str): The URL to fetch the HTML content from.

    Returns:
        BeautifulSoup: Parsed HTML content.

    Raises:
        CustomException: If there's an issue with the network request, 
                         a custom exception is raised with details.
    """
    try:
        #uncomment when running for derena 
        # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}  
        # response = requests.get(url, headers=headers)
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'lxml',from_encoding='utf-8')
        return soup
    except (Timeout, ConnectionError, RequestException) as e:
        error_message = f"Error fetching {url}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message, sys) from e  


def get_unique_id()->str:
    '''
    Generate a unique identifier.
    
    Returns:
        str: A 24-character unique identifier.
        
    '''

    id = str(uuid.uuid4()).replace('-', '')[:24]

    return id



def load_config(config_path: str) -> dict:
    """
    Loads the configuration from a YAML file.
    
    Args:
        config_path (str): The path to the config file
    
    Returns:
        str: A dict of paths
        
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def data_loader(path):
    """
    Loads the data needed to be transformed

    Args:
        path (_type_): Path to input data

    Raises:
        CustomException: When file is not found in path

    Returns:
        df: Dataframe
    """
    try:
        df = pd.read_json(path)
        logging.info(f'Loaded input data from: {path}')
        
        return df
         
    except FileNotFoundError as e:
        error_message = f'Error loading data from {path}: str{e}'
        logging.error(error_message)
        raise CustomException(error_message)