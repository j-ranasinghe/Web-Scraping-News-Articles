import logging
import sys
import re
import json
import yaml
import gzip
import time
from pathlib import Path
from src.utils import get_parsed_html, get_unique_id, load_config
from src.logger import logging

def read_all_titles(file_path):
    """
    Reads all titles from a gzipped file and returns them as a list.
    Args:
        file_path (str): The path to the gzipped file containing titles.
    Returns:
        list: A list of titles read from the file.
    Raises:
        FileNotFoundError: If the file does not exist.
        gzip.BadGzipFile: If the file is not in gzip format or is corrupted.
        UnicodeDecodeError: If there is an error decoding the file.
        Exception: For any other unexpected errors.
    """
    titles = []
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                titles.append(line.strip())
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except gzip.BadGzipFile:
        logging.error(f"File is not in gzip format or is corrupted: {file_path}")
    except UnicodeDecodeError:
        logging.error(f"Error decoding file {file_path}. Check encoding and file integrity.")
    except Exception as e:
        logging.error(f"Unexpected error occurred while reading {file_path}: {e}")
    return titles

def save_titles_to_json(titles, filename):
    """
    Saves a list of titles to a JSON file.
    Args:
        titles (list): A list of titles to be saved.
        filename (str, optional): The path to the JSON file where titles will be saved.
    Raises:
        CustomException: If there is an error during the file saving process.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(titles, f, ensure_ascii=False, indent=4)
        print(f"Successfully saved {len(titles)} titles to {filename}.")
    except FileNotFoundError:
        logging.error(f"Directory not found for file: {filename}. Please check the path.")
    except Exception as e:
        error_message = f"Error saving titles to {filename}: {e}"
        logging.error(error_message)
        raise CustomException(error_message, sys)

import re
import json
import logging
import time

def scrape_wikipedia_content(title):
    """
    Scrapes the title and first three paragraphs from a Wikipedia article in Sinhala.
    Each paragraph is saved as a separate entry with the same metadata.
    Args:
        title (str): The title of the Wikipedia article to scrape.
    Returns:
        list: A list of dictionaries, each containing the article metadata for a paragraph.
    Raises:
        CustomException: If there is an error fetching content from the Wikipedia article.
    """
    try:
        url = f"https://si.wikipedia.org/wiki/{title.replace(' ', '_')}"
        soup = get_parsed_html(url)
        
        if not soup:
            return []

        # Extract the title
        article_title = soup.find('h1', {'id': 'firstHeading'})
        if article_title:
            article_title = article_title.get_text()
        else:
            logging.warning(f"Title not found for {url}")
            return []  # Skip if title is not found

        # Extract paragraphs (first three)
        paragraphs = [p.get_text() for p in soup.find_all('p') if p.get_text().strip()]
        if not paragraphs:
            logging.warning(f"No paragraphs found for {url}")
            return []  # Skip if no paragraphs found
        
        # Generate a unique ID for the article
        article_id = get_unique_id()

        # Create a list of article info dictionaries for each paragraph
        article_info_list = []
        for i, para in enumerate(paragraphs[:10]):  # Only take the first 10 paragraphs
            article_info = {
                'category': 'Wiki',
                'site': 'si.wikipedia.org',
                'url': url,
                'title': article_title,
                'context': para,
                'id': article_id
            }
            article_info_list.append(article_info)

        return article_info_list  # Return a list of article info for each paragraph

    except Exception as e:
        error_message = f"Error fetching content from {url}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message)


# Function to scrape articles and save to JSON
def scrape_multiple_articles(titles, num_articles, output_file):
    """
    Scrapes multiple Wikipedia articles based on the provided titles and saves them to a JSON file.
    Each paragraph is saved as a separate entry.
    Args:
        titles (list of str): List of Wikipedia article titles to scrape.
        num_articles (int): Number of articles to scrape from the titles list.
        output_file (str, optional): Path to the output JSON file where scraped articles will be saved.
    Returns:
        None
    Raises:
        Exception: If there is an error initializing the output file or saving the articles.
    Notes:
        - The function pauses for 5 seconds between requests to avoid overwhelming the server.
        - If no articles are successfully scraped, a message is printed and no file is saved.
    """
    articles = []
    scraped_count = 0  # Initialize a counter for successfully scraped articles

    # Initialize output file with an empty list
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=4)
    except Exception as e:
        logging.error(f"Error initializing output file {output_file}: {e}")
        return

    # Define a regular expression to check for titles that contain non-Sinhala characters
    sinhala_title_pattern = re.compile(r'^[\u0D80-\u0DFF\s]+$')  # Match only Sinhala characters and spaces

    # Iterate over titles, skipping the first one and filtering out invalid titles
    for i, title in enumerate(titles[1:num_articles+1]):  # titles[1:] skips the first title
        # Skip titles that don't match the Sinhala characters pattern
        if not sinhala_title_pattern.match(title):
            print(f"Skipping invalid title (non-Sinhala characters): {title}")
            continue
        
        print(f"Scraping article {i + 1}/{num_articles}: {title}")
        try:
            article_info_list = scrape_wikipedia_content(title)
            if article_info_list:
                articles.extend(article_info_list)  # Add all paragraphs as separate entries
                scraped_count += len(article_info_list)  # Increment the counter for each paragraph
        except Exception as e:
            logging.error(f"Error scraping article {title}: {e}")
        
        time.sleep(5)  # Pause for 5 seconds between requests

    # Save articles to JSON if successfully scraped
    if articles:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=4)
            print(f"Saved {len(articles)} paragraphs to {output_file}")
            print(f"Total paragraphs scraped: {scraped_count}")  # Display total count
        except Exception as e:
            logging.error(f"Error saving articles to {output_file}: {e}")
    else:
        print(f"No articles were scraped, nothing to save.")



def main():
    config = load_config('config.yaml')
    TITLES_FILE = Path(config['TITLES_FILE_PATH'])
    FILE_PATH = Path(config['FILE_PATH_SINHALA_WIKI_DATA'])
    OUTPUT_PATH = Path(config['SCRAPED_WIKI_DATA_PATH'])
    
    # Load WIKI titles file and save to JSON
    titles = read_all_titles(FILE_PATH)
    save_titles_to_json(titles, TITLES_FILE)
    print(f"Saved {len(titles)} titles to {TITLES_FILE}.")
    
    #Load titles from JSON
    with open(TITLES_FILE, 'r', encoding='utf-8') as f:
        titles = json.load(f)
        
    # Define the number of articles to scrape
    num_articles = int(input("Enter the number of articles to scrape: "))
    
    # Scrape articles and save to JSON
    scrape_multiple_articles(titles, num_articles=num_articles, output_file=OUTPUT_PATH)

if __name__ == "__main__":
    main()
