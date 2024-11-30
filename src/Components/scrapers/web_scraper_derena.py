import re
import json
import yaml
import sys
import html
from urllib.parse import urlparse
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
        s = get_parsed_html(url).find_all('div', {'class': 'news-story'})
        
        article_info = []

        for idx, details in tqdm (enumerate(s,start=1)):
            try:
                content_url = details.find('a').get('href')
                parsed_url =urlparse(url)
                base_url = f'{parsed_url.scheme}://{parsed_url.netloc}'
                artical_url = f'{base_url}/{content_url}'
                article = get_parsed_html(artical_url)
                
                title = article.find('h1',  class_ = 'news-heading').text    
                content = article.find_all('p', style = 'text-align:justify')
            
                context = []
                for paragraph in content:
                    if paragraph.find('img'):
                        continue
  
                    context.append(paragraph.get_text(strip=True))
                context_text = '\n'.join(context)
                text  = html.unescape(context_text)       
                id =  get_unique_id()
               
            
                article_info = {
                    'category': category,
                    'site':sitename,
                    'url':content_url,
                    'title': title,
                    'context': text,
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
    SOURCE_URL_LOCAL  = config['SOURCE_URL_DERENA_local']
    SOURCE_URL_SPORTS = config['SOURCE_URL_DERENA_SPORTS']
    SOURCE_URL_INT =config['SOURCE_URL_DERENA_INT']

    SITE_NAME = 'adaderena'
    TOTAL_PAGES = 314

    SOURCE_URL = SOURCE_URL_LOCAL
    category = 'Local-news'
    
    for page in tqdm(range(13 ,TOTAL_PAGES), desc=f"Scraping {SITE_NAME}", unit="page"):
        url = f"{SOURCE_URL}pageno={page}"
        scrape_article(url, SITE_NAME,OUTPUT_PATH)
        print(f"Completed scraping page: {page}")
        logging.info(f"Successfully scraped page: {page}")

    
    print(f"Completed scraping all pages for: {SITE_NAME}")
    logging.info(f"Successfully scraped Site: {SITE_NAME}")




            
        