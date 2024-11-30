import re
import json
import yaml
import sys
from pathlib import Path
from tqdm import tqdm
from src.utils import get_parsed_html, get_unique_id, load_config
from src.logger import logging
from src.exception import CustomException
     

def scrape_article(url, sitename:str,path:str):
    """
    Fetches and extracts HTML content from a given URL.

    Args:
        url (str): The URL to fetch the HTML content from.
        sitename(str): The name of the site HTML content is scraped from
        path(str): The path to output folder

    Raises:
        CustomException: If there's an issue with fetching the data, 
                         a custom exception is raised with details.
    Returns:
        None
                            
    Expected Output:
        A JSON file is created at "path/to/save/data/{article_id}.json" containing:
        {
            "category": "local",
            "site": "example_news",
            "url": "https://www.example.com/articles/123",
            "title": "Sample Article Title",
            "context": "This is the content of the article.",
            "id": "unique_article_id"
        }
    """
    try:
        s = get_parsed_html(url).find_all('div', {'class': 'row', 'style':'margin-bottom:10px'})
        
        
        article_info = []

        for idx, details in tqdm (enumerate(s,start=1)):
            try:
                content_url = details.find('a', attrs ={'href': re.compile("^https://")}).get('href')
                main_content = get_parsed_html(content_url)
                title = main_content.find('h1', class_ = 'main-tittle').text
                context = main_content.find('div', id = 'article-phara').text
                id =  get_unique_id()
                
                
                article_info = {
                    'category': category,
                    'site':sitename,
                    'url':content_url,
                    'title': title,
                    'context': context,
                    'id': id
                }
                
                file_path = path/sitename/f'{id}.json'
                with open(file_path, "w", encoding='utf-8') as f:
                    json.dump(article_info, f, ensure_ascii=False, indent=4)
                
                    
            except Exception as e:
                error_message = f"Error fetching {url}: {str(e)}"
                logging.error(error_message)
                continue
        
    except Exception as e:
        error_message = f"Error fetching content from {url}: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message, sys)
            
        
        
if __name__ == "__main__":
    
    config = load_config('config.yaml')
    OUTPUT_PATH = Path(config['SCRAPED_DATA_PATH'])
    SOURCE_URL_LOCAL = config['SOURCE_URL_HIRU_LOCAL']
    SOURCE_URL_SPORTS = config['SOURCE_URL_HIRU_SPORTS']
    SOURCE_URL_ENT = config['SOURCE_URL_HIRU_ENT']
    SOURCE_URL_BUSINESS = config['SOURCE_URL_HIRU_BUSINESS']
    SOURCE_URL_INT =config['SOURCE_URL_HIRU_INT']

    SITE_NAME = 'hiru_news' 
    TOTAL_PAGES = 89

    SOURCE_URL = SOURCE_URL_BUSINESS
    category = SOURCE_URL.split('/', maxsplit=3)[-1].split('.', maxsplit=1)[0].replace('-',' ')
    
    for page in tqdm(range(17, TOTAL_PAGES), desc=f"Scraping {SITE_NAME}", unit="page"):
        url = f"{SOURCE_URL}pageID={page}"
        scrape_article(url, SITE_NAME,OUTPUT_PATH)
        print(f"Completed scraping page: {page}")
        logging.info(f"Successfully scraped page: {page}")

    
    print(f"Completed scraping all pages for: {SITE_NAME}")
    logging.info(f"Successfully scraped Site: {SITE_NAME}")




            
        