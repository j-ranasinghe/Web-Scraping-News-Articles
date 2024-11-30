import json
import sys
from pathlib import Path
from src.utils import load_config
from src.logger import logging
from src.exception import CustomException


def list_json_files(directory: Path) -> list:
    """
    Lists the JSON files in a given directory.

    Args:
        directory (Path): Path to the directory with JSON files.

    Raises:
        CustomException: if the directory does not exist or no JSON files are found.

    Returns:
        list: List of JSON file paths.
    """
    try:
        if not directory.exists():
            raise CustomException(f"Directory does not exist: {directory}")
        
        json_files = list(directory.rglob("*.json"))
        
        if not json_files:
            logging.warning(f"No JSON files found in directory: {directory}")
            raise CustomException("No JSON files found.")
        
        return json_files
        
    except Exception as e:
        error_message = f"Error listing JSON files in directory {directory}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)


def merge_json(file_paths: list, output_path: Path):
    """
    Merges the individual JSON files.

    Args:
        file_paths (list): List of JSON file paths.
        output_path (Path): Path for the merged output file.

    Raises:
        CustomException: If there's an error during merging or saving.
    """
    logging.info(f'Started merging to {output_path}')
    merged_data = []

    for file_path in file_paths:
        try:
            with open(file_path, 'r', encoding="utf-8") as file:
                data = json.load(file)
                # Append data based on whether it’s a list or dictionary
                if isinstance(data, list):
                    merged_data.extend(data)
                else:
                    merged_data.append(data)
                
        except Exception as e:
            error_message = f"Error merging content from {file_path}: {str(e)}"
            logging.error(error_message)
            continue    

    try:
        with open(output_path, 'w', encoding="utf-8") as output_file:
            json.dump(merged_data, output_file, ensure_ascii=False, indent=4)
        logging.info(f"Successfully merged data to: {output_path}")

    except Exception as e:
        error_message = f"Error writing merged data to {output_path}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)


if __name__ == "__main__":
    try:
        config = load_config('config.yaml')
        output_path = Path(config['MERGED_DATA_PATH'])  # Specify output data path
        directory = Path(config['SCRAPED_DATA_PATH'])   # Specify raw data path

        json_files = list_json_files(directory)
        merge_json(json_files, output_path)

    except CustomException as ce:
        logging.error(f"CustomException encountered: {str(ce)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
