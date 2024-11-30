import pandas as pd
import numpy as np
import json
import sys
import os
import re
from pathlib import Path
from sklearn.model_selection import train_test_split
from src.utils import get_unique_id, load_config, data_loader
from src.logger import logging
from src.exception import CustomException

config = load_config('config.yaml')
OUTPUT_PATH = Path(config['TRANSFORMED_DATA_PATH'])
MERGED_DATA_PATH = Path(config['MERGED_DATA_PATH']) 
    
def rename_categories(df):
    """
    Renames the category column values to common names for similar types

    Args:
        df (dataframe): Input dataframe with data to be changed

    Raises:
        KeyError: Key error raised if column does not exist

    Returns:
        df: The transformed DataFrame after applying the  operations
    """
    try:
        df['category'] = df['category'].replace({
            
                    'local news': 'Local-news',
                    'International_news': 'International-news',
                    'international news': 'International-news',
                    'business/all news': 'Business-news',
                    'Business_news': 'Business-news',
                    'sports/all news': 'Sports-news',
                    'Sports_news': 'Sports-news',
                    'entertainment/all news': 'Entertainment-news',
                    'All news': 'All-news'
                    
                })
        return df

        logging.info(f'Updated category names')

    except KeyError as e:
        error_message = f'Error in accessing column "category": str{e}'
        logging.error(error_message)
        raise CustomException(error_message)
    
    
    
def remove_whitespaces(df):
    """
    Removes leading and trailing whitespaces from passages

    Args:
        df (dataframe): Dataframe to be cleaned

    Raises:
        CustomException: Raise AttributeError if data not string

    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try:
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        logging.info('Removed leading and trailing whitespaces from all columns')
        return df
        
    except AttributeError as e:
        error_message = f"AttributeError: {e} - Ensure the column is of string type."
        logging.error(error_message)
        raise CustomException(error_message)
    
    
def remove_duplicates(df):
    """
    Removes duplicates from columns

    Args:
        df (dataframe): Dataframe to be cleaned

    Raises:
        CustomException: Raise KeyError if columns not found


    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try:
      df = df.drop_duplicates(subset='context', keep='first')
      df = df.drop_duplicates(subset='title', keep='first')
      logging.info('Initial duplicates removal complete')
      return df
    
    except KeyError as e:
        error_message = f'KeyError: {e} - Column "context" not found'
        logging.error(error_message)
        raise CustomException(error_message)
    
    
def remove_lines_from_context(df,site:str):
    """
    Removes the unwanted lines from passages

    Args:
        df (dataframe): Dataframe to be cleaned
        site (str): Site to be cleaned

    Raises:
        CustomException: Raise error if removing lines fail 

    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try:
        def first_line_remover(df):
               lines = df.split('\n')
               # Keep everything after the first line
               return '\n'.join(lines[1:]) if len(lines) > 1 else ''
               logging.info(f'Not enough lines to remove in {site}')
                
                
        def remove_first_and_last_lines(df, num_first=3):
            lines = df.split('\n')
            if len(lines) > num_first:
                modified_text = '\n'.join(lines[num_first:])
                # Find the index of the word "popular" 
                popular_index = modified_text.find("popular")
                if popular_index != -1:
                    # Remove everything from "popular" onwards
                    return modified_text[:popular_index].rstrip() 
                
                return modified_text.rstrip()  # If "popular" is not found, return the modified passage
            else:
                # If not enough lines, return an empty string
                return ''
            
        if site == 'ada':
            df.loc[df['site'] == 'ada', 'context'] = df.loc[df['site'] == 'ada', 'context'].apply(remove_first_and_last_lines)
            logging.info(f'Removed lines from front and end of {site}')
        if site == 'lankadeepa':
            df.loc[df['site'] == 'lankadeepa', 'context'] = df.loc[df['site'] == 'lankadeepa', 'context'].apply(first_line_remover)
            logging.info(f'Removed line from front of {site}')
    
        return df

    except Exception as e:
        error_message = f'Error {e} occurred while removing lines in {site}'
        logging.error(error_message)
        raise CustomException(error_message)

    
        
def remove_new_lines(df):
    """
    Removes the new lines from passages and takes to one paragragh

    Args:
        df (dataframe): Dataframe to be cleaned

    Raises:
        CustomException: Raise custom error 

    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try:
        df = df.apply(lambda x: x.str.replace('\n', ' ') if x.dtype == "object" else x)
        logging.info('Removed new lines from dataframe')
        return df
    except Exception as e:
        error_message = f'Error: {e} - Error in removing white lines from dataframe'
        logging.error(error_message)
        raise CustomException(error_message)
    

def remove_empty_passages(df):
    """
    Remove empty passages/titles which containa string of whitespaces

    Args:
        df (dataframe): The dataframe to be cleaned

    Raises:
        CustomException: Raise custom error if removing empty spaces fail

    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try: 
        df = df[df['context'].str.strip() != '']
        df = df[df['title'].str.strip() != '']
        logging.info('Remove empty passages and titles from dataframe')
        return df
    except Exception as e:
        error_message = f'Error: {e} - Error in removing empty passages/titles from dataframe'
        logging.error(error_message)
        raise CustomException(error_message)
       
    
def add_context_length(df):
    """
    Create new column as context length containing length of each context 

    Args:
        df (dataframe): Dataframe to be cleaned

    Raises:
        CustomException: Raise custom error 

    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try:
        df['context_length'] = df['context'].str.len()
        return df
    
    except Exception as e:
        error_message = f'Error {e} occurred while removing lines in {site}'
        logging.error(error_message)
        raise CustomException(error_message)


def sort_by_title(df):
    """
    Sort the dataframe by titles

    Args:
        df (dataframe): The dataframe to be cleaned

    Raises:
        CustomException: Raise KeyError if column not found

    Returns:
        dataframe: The transformed DataFrame after applying the  operations
    """
    try:
        df = df.sort_values(by='title', ascending=True)
        logging.info('Sorted dataframe by title')
        return df
    except KeyError as e:
        error_message = f'Error: {e} - Error cannot find the "title" column'
        logging.error(error_message)
        raise CustomException(error_message)    


def remove_special_characters(df):
    """
    Remove special characters from the dataframe

    Args:
        df (dataframe): The dataframe to be cleaned

    Raises:
        CustomException: Raise error if removing special characters fails

    Returns:
        dataframe: dataframe: The transformed DataFrame after applying the  operations
    """
    try:
        df['title'] = df['title'].str.replace('\t', '').str.replace('\r', '')
        return df    
    except e as Exception:
        error_message = f'Error: {e} - Error in removing special characters'
        logging.error(error_message)
        raise CustomException(error_message)


def filter_non_english_text(df):
    """
    Filter out rows that contain English letters from the 'context' column.

    Args:
        df (DataFrame): The dataframe to be cleaned.

    Raises:
        CustomException: Raise error if filtering fails.

    Returns:
        DataFrame: The filtered DataFrame with non-English text only.
    """
    try:
        # Define regular expressions 
        special_char_pattern = re.compile(r'[^\w\s]', re.UNICODE)  # Special characters excluding word characters and spaces
        english_letters_pattern = re.compile(r'[A-Za-z]')  # Matches English letters

        # Define counters for occurrences
        df['special_char_count'] = df['context'].apply(lambda x: len(re.findall(special_char_pattern, x)))
        df['english_letter_count'] = df['context'].apply(lambda x: len(re.findall(english_letters_pattern, x)))

        # Log the count of special characters and English letters
        logging.info(f"Special character count added: {df['special_char_count'].sum()}")
        logging.info(f"English letter count added: {df['english_letter_count'].sum()}")

        # Filter out rows where 'english_letter_count' is greater than 0 (i.e., rows that contain English letters)
        df_cleaned_no_english = df[df['english_letter_count'] == 0]
        
        # Log the number of rows filtered
        rows_dropped = len(df) - len(df_cleaned_no_english)
        logging.info(f"Dropped {rows_dropped} rows containing English letters.")

        return df_cleaned_no_english

    except Exception as e:
        error_message = f"Error during non-English text filtering: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)
    

def drop_rows_with_special_chars(df, column_name='context'):
    """
    Drop rows from the dataframe where the specified column contains special characters.

    Args:
        df (DataFrame): The dataframe to be cleaned.
        column_name (str): The column name to check for special characters (default is 'context').

    Raises:
        CustomException: Raise error if the process fails.

    Returns:
        DataFrame: The cleaned DataFrame with rows containing special characters removed.
    """
    try:
        # Define the special characters to check for
        special_chars = ['[', ']', '{', '}', '"', "'", '/', '\\']
        
        # Filter rows where the column contains any of the special characters
        mask = df[column_name].apply(lambda x: any(char in x for char in special_chars))

        # Drop rows that contain any of the special characters
        df_cleaned = df[~mask]  # ~mask means we keep rows where mask is False (i.e., no special characters)

        # Log the number of rows dropped
        rows_dropped = len(df) - len(df_cleaned)
        logging.info(f"Dropped {rows_dropped} rows with special characters from '{column_name}' column.")

        return df_cleaned

    except Exception as e:
        error_message = f"Error during special character filtering in column '{column_name}': {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)
    
    

def filter_math_sequences(df):
    """
    Filter out rows containing sequences that resemble mathematical equations from the 'context' column.

    Args:
        df (DataFrame): The dataframe to be cleaned.

    Raises:
        CustomException: Raise error if filtering fails.

    Returns:
        DataFrame: The filtered DataFrame with mathematical-like sequences removed.
    """
    try:
        # Define regex pattern to detect sequences resembling mathematical equations
        math_pattern = r'\d\s*[\+\-\*/^=]\s*\d'
        
        # Apply the pattern to detect rows with mathematical-like sequences
        math_rows = df['context'].str.contains(math_pattern)

        # Log the number of rows containing mathematical-like sequences
        rows_with_math = math_rows.sum()
        logging.info(f"Detected {rows_with_math} rows with mathematical-like sequences.")

        # Filter out rows with mathematical-like sequences
        df_filtered = df[~math_rows].reset_index(drop=True)

        # Log the number of rows removed
        rows_dropped = len(df) - len(df_filtered)
        logging.info(f"Dropped {rows_dropped} rows containing mathematical-like sequences.")

        return df_filtered
    
    except Exception as e:
        error_message = f"Error during filtering of mathematical-like sequences: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)



def filter_wiki_titles_by_occurrence(df, site='si.wikipedia.org', min_count=5):
    """
    Filter out rows where the 'title' column occurs less than a specified minimum count for a given site.

    Args:
        df (DataFrame): The dataframe to be filtered.
        site (str): The site to filter by (default is 'si.wikipedia.org').
        min_count (int): The minimum occurrence count for the title (default is 5).

    Raises:
        CustomException: Raise error if filtering fails.

    Returns:
        DataFrame: The filtered DataFrame with titles occurring less than the minimum count removed.
    """
    try:
        # Step 1: Filter the DataFrame for the specified site
        wiki_df = df[df['site'] == site]

        # Step 2: Count the occurrences of each title within the specified site
        title_counts_wiki = wiki_df['title'].value_counts()

        # Step 3: Identify titles that occur less than the specified minimum count
        titles_to_drop_wiki = title_counts_wiki[title_counts_wiki < min_count].index

        # Log the number of titles being dropped
        titles_dropped = len(titles_to_drop_wiki)
        logging.info(f"Identified {titles_dropped} titles to drop that occur less than {min_count} times.")

        # Step 4: Drop rows from the original DataFrame where the site is specified and the title occurs less than the minimum count
        df_filtered = df[~((df['site'] == site) & (df['title'].isin(titles_to_drop_wiki)))]

        # Log the number of rows removed
        rows_dropped = len(df) - len(df_filtered)
        logging.info(f"Dropped {rows_dropped} rows with titles occurring less than {min_count} times.")

        return df_filtered

    except Exception as e:
        error_message = f"Error during filtering wiki titles by occurrence for site '{site}': {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)
    
    

def clean_and_deduplicate_df(df, wiki_site='si.wikipedia.org'):
    """
    Clean and deduplicate the DataFrame by handling 'wiki' and non-'wiki' rows differently.

    Args:
        df (DataFrame): The dataframe to be cleaned and deduplicated.
        wiki_site (str): The site considered as 'wiki' (default is 'si.wikipedia.org').

    Raises:
        CustomException: Raise error if cleaning and deduplication fails.

    Returns:
        DataFrame: The cleaned and deduplicated DataFrame.
    """
    try:
        # Step 1: Separate "wiki" and non-"wiki" rows
        wiki_df = df[df['site'] == wiki_site]
        non_wiki_df = df[df['site'] != wiki_site]

        # Log the number of rows in each subset
        logging.info(f"Number of wiki rows: {len(wiki_df)}")
        logging.info(f"Number of non-wiki rows: {len(non_wiki_df)}")

        # Step 2: Drop duplicates in non-"wiki" rows based on 'title', keeping only the first occurrence
        non_wiki_df = non_wiki_df.drop_duplicates(subset='title', keep='first')

        # Log the number of rows after dropping duplicates based on 'title'
        logging.info(f"Number of non-wiki rows after deduplication by title: {len(non_wiki_df)}")

        # Step 3: Concatenate the "wiki" and deduplicated non-"wiki" data back together
        df_combined = pd.concat([wiki_df, non_wiki_df])

        # Step 4: Drop duplicates in the 'context' column, keeping the first occurrence
        df_combined = df_combined.drop_duplicates(subset='context', keep='first')

        # Log the number of rows after dropping duplicates based on 'context'
        logging.info(f"Number of rows after deduplication by context: {len(df_combined)}")

        # (Optional) Reset the index if needed
        df_combined = df_combined.reset_index(drop=True)

        logging.info("Data cleaning and deduplication completed successfully.")
        return df_combined

    except Exception as e:
        error_message = f"Error during cleaning and deduplication: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)



def ranging_passages(df):
    """
    Drop the passages out of range from 300 to 3500 words.

    Args:
        df (dataframe): The dataframe to be cleaned

    Raises:
        CustomException: Raise error if dropping fails

    Returns:
        dataframe: The transformed DataFrame after applying the operations
    """
    try:
        # Add a new column for word count
        df['word_count'] = df['context'].apply(lambda x: len(x.split()))

        # Filter the dataframe for passages with word count between 300 and 3500
        context_in_range = df[(df['word_count'] >= 25) & (df['word_count'] <= 250)]

        # Log the operation
        logging.info('Dropped passages out of range from 300-3500 words')

        return context_in_range
    except Exception as e:
        error_message = f'Error: {e} - Error in dropping out of range passages'
        logging.error(error_message)
        raise CustomException(error_message)



def drop_columns(df, columns_to_drop):
    """
    Drop specified columns from the DataFrame.

    Args:
        df (dataframe): The DataFrame from which columns need to be dropped.
        columns_to_drop (list): A list of column names to be dropped.

    Returns:
        dataframe: The DataFrame with the specified columns dropped.
    """
    try:
        # Drop the specified columns
        df_dropped = df.drop(columns=columns_to_drop)
        return df_dropped
    except KeyError as e:
        error_message = f"Error: The following columns are missing from the DataFrame: {e}"
        logging.error(error_message)
        raise CustomException(error_message)
    except Exception as e:
        error_message = f"Error: {e} - Error in dropping columns"
        logging.error(error_message)
        raise CustomException(error_message)
 
 
    
def write_to_json(df,path):
    """
    Write cleaned dataframe to JSON file

    Args:
        df (dataframe): The dataframe to be cleaned
        path (str): Output path to save JSON file
    """
    try:
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(
                df.to_dict(orient='records'), 
                file, 
                indent=7,
                ensure_ascii=False
            )
            logging.info(f'Dataframe written to JSON file: {path}')
            print(f"DataFrame successfully written to {path}.")
            
    except FileNotFoundError as e:
        error_message = f'FileNotFoundError: {e} -  The specified file path "{file_path}" does not exist.'
        logging.error(error_message)
        raise CustomException(error_message)
        
    except PermissionError as e :
        error_message = f'PermissionError: {e} - Do not have permission to write to "{file_path}".'
        logging.error(error_message)
        raise CustomException(error_message)
        
    except IOError as e:
        error_message = f'IOError: An I/O error occurred while writing to the file: {e}'
        logging.error(error_message)
        raise CustomException(error_message)
        
    except OSError as e:
        error_message = f'OSError: A system-related error occurred: {e}'
        logging.error(error_message)
        raise CustomException(error_message)
        
    except Exception as e:
        error_message = f'An unexpected error occurred: {e}'
        logging.error(error_message)
        raise CustomException(error_message)
    

def stratified_train_test_dev_split(df, path: str, test_size=0.1, dev_size=0.1, random_state=42):
    """
    Split the dataframe into train, dev, and test sets using stratified sampling to maintain category proportions.

    Args:
        df (DataFrame): The dataframe to be split.
        path (str): The directory where the split data will be saved.
        test_size (float): Proportion of data to allocate to the test set (default is 0.1 for 10%).
        dev_size (float): Proportion of data to allocate to the dev set (default is 0.1 for 10%).
        random_state (int): Random seed for reproducibility.

    Raises:
        CustomException: Raise error if splitting fails.
    """
    try:
        # First, split into train and temp (for dev and test) sets
        train_df, temp_df = train_test_split(df, test_size=test_size + dev_size, stratify=df['site'], random_state=random_state)  # 80% train, 20% remaining

        # Split temp_df into dev and test sets
        dev_df, test_df = train_test_split(temp_df, test_size=0.5, stratify=temp_df['site'], random_state=random_state)  # Split the remaining 20% into 10% dev and 10% test

        # Define file paths
        test_path = f'{path}/test.json'
        train_path = f'{path}/train.json'
        dev_path = f'{path}/dev.json'

        # Reset the index and add 'index' column for consistency
        train_df = train_df.reset_index(drop=True)
        train_df['index'] = train_df.index

        dev_df = dev_df.reset_index(drop=True)
        dev_df['index'] = dev_df.index

        test_df = test_df.reset_index(drop=True)
        test_df['index'] = test_df.index

        # Save the splits into separate JSON files
        with open(train_path, 'w', encoding='utf-8') as f_train:
            json.dump(train_df.to_dict(orient='records'), f_train, indent=7, ensure_ascii=False)
            logging.info(f'Created train dataset at {train_path}')

        with open(dev_path, 'w', encoding='utf-8') as f_dev:
            json.dump(dev_df.to_dict(orient='records'), f_dev, indent=7, ensure_ascii=False)
            logging.info(f'Created dev dataset at {dev_path}')

        with open(test_path, 'w', encoding='utf-8') as f_test:
            json.dump(test_df.to_dict(orient='records'), f_test, indent=7, ensure_ascii=False)
            logging.info(f'Created test dataset at {test_path}')

        print("Data successfully split into train, dev, and test sets!")

    except Exception as e:
        error_message = f"Error during data splitting: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)

    
if __name__ == "__main__":
    try: 
    
        path = OUTPUT_PATH/f'transformed_data_v1.json'
        

        df = data_loader(MERGED_DATA_PATH)
        df = rename_categories(df)
        df = filter_non_english_text(df)
        df = drop_rows_with_special_chars(df)
        df = filter_math_sequences(df)
        df = filter_wiki_titles_by_occurrence(df)
        df = remove_lines_from_context(df,'lankadeepa')
        df = remove_new_lines(df)
        df = clean_and_deduplicate_df(df)
        df = remove_empty_passages(df)
        df = add_context_length(df)
        df = sort_by_title(df)
        df = remove_special_characters(df)
        df = ranging_passages(df)
        df = remove_new_lines(df)
        df = drop_columns(df, ['special_char_count', 'english_letter_count'])
        write_to_json(df,path)
        stratified_train_test_dev_split(df,OUTPUT_PATH)
        print(df.shape)

        logging.info(f'Dataframe written to JSON file at {OUTPUT_PATH} and split completed')
        
    except Exception as e:
        error_message = f'An unexpected error occurred: {e}'
        logging.error(error_message)
        raise CustomException(error_message)
    
    