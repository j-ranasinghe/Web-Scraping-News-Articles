import pandas as pd
import json
from pathlib import Path
from src.utils import load_config
from src.logger import logging
from src.exception import CustomException
from sklearn.model_selection import train_test_split

def load_json(file_path):
    """Load a JSON file into a pandas DataFrame."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        error_message = f"Error loading data from {file_path}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)

def split_and_save_smaller_sets(df, base_path, set_type, set_size=500):
    """
    Split the DataFrame into smaller sets of the given size and save them as separate JSON files.

    Args:
        df (DataFrame): The dataframe to be split.
        base_path (str): The base path to save the datasets.
        set_type (str): The type of dataset (train, dev, or test).
        set_size (int): The number of rows for each set (default is 500).

    Raises:
        CustomException: Raise error if splitting or saving fails.
    """
    try:
        # Ensure the path exists
        set_path = Path(base_path) / set_type
        set_path.mkdir(parents=True, exist_ok=True)

        # Calculate the number of splits needed
        num_splits = len(df) // set_size

        # Split and save each subset
        for i in range(num_splits + 1):
            start_index = i * set_size
            end_index = start_index + set_size

            # Slice the DataFrame for the current set
            smaller_set = df[start_index:end_index]

            # Construct the filename and save
            set_filename = set_path / f"{set_type}_set_{i+1}.json"
            with open(set_filename, 'w', encoding='utf-8') as f:
                json.dump(smaller_set.to_dict(orient='records'), f, indent=7, ensure_ascii=False)
            logging.info(f"Created dataset: {set_filename}")

        print(f"Data successfully split into smaller sets of {set_size} rows each for {set_type}!")

    except Exception as e:
        error_message = f"Error during data splitting for {set_type}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)

if __name__ == "__main__":
    # Load the configuration file
    config = load_config('config.yaml')

    # Load data from JSON paths specified in config.yaml
    train_df = load_json(config['TRAIN_DATA_PATH'])
    dev_df = load_json(config['DEV_DATA_PATH'])
    test_df = load_json(config['TEST_DATA_PATH'])

    # Use SPLIT_PATH for the output directory, creating subfolders for train, dev, and test
    split_and_save_smaller_sets(train_df, config['SPLIT_PATH'], 'train', set_size=500)
    split_and_save_smaller_sets(dev_df, config['SPLIT_PATH'], 'dev', set_size=500)
    split_and_save_smaller_sets(test_df, config['SPLIT_PATH'], 'test', set_size=500)
