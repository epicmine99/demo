import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

def read_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

def scrape_articles(config):
    source_info = []
    articles_data = []
    cur_id = 0
    for source in config:
        cur_id += 1
        base_url = source['base_url']
        articles_url = source['articles_url']
        article_container_selector = source['source_selectors']['article_container']
        link_selector = source['source_selectors']['link']
        thumbnail_selector = source['source_selectors']['thumbnail']
        
        response = requests.get(articles_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        article_containers = soup.select(article_container_selector)

        for container in article_containers:
            link_element = container.select_one(link_selector)
            if link_element is not None:
                link = link_element['href']
                if not link.startswith('http'):
                    link = base_url + link
            else:
                # do nothing if link does not exist
                continue

            thumbnail_element = container.select_one(thumbnail_selector)
            if thumbnail_element is not None:
                thumbnail_url = thumbnail_element
            else:
                thumbnail_url = ''
            
            article_response = requests.get(link)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            article_details = {
                'title': article_soup.select_one(source['article_selectors']['title']).text.strip() if article_soup.select_one(source['article_selectors']['title']) else None,
                'url': link,
                'text': article_soup.select_one(source['article_selectors']['content']).text.strip() if article_soup.select_one(source['article_selectors']['content']) else None,
                'author': article_soup.select_one(source['article_selectors']['author']).text.strip() if article_soup.select_one(source['article_selectors']['author']) else None,
                'date_published': article_soup.select_one(source['article_selectors']['date_published']).text.strip() if article_soup.select_one(source['article_selectors']['date_published']) else None,
                'date_edited': article_soup.select_one(source['article_selectors']['date_edited']).text.strip() if article_soup.select_one(source['article_selectors']['date_edited']) else None,
                'thumbnail': article_soup.select_one(source['article_selectors']['thumbnail']).get('src') if article_soup.select_one(source['article_selectors']['thumbnail']) else '', # will update empty string with alterative search method for images
                'source_id': cur_id
            }
            articles_data.append(article_details)

        source_info.append({
            'name': source['name'],
            'base_url': source['base_url'],
            'article_scrapping_url': source['articles_url'],
            'logo': thumbnail_url,
            'id': cur_id
        })

    return pd.DataFrame(source_info), pd.DataFrame(articles_data)

def row_exists(table, unique_column, unique_value, engine):
    query = f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {unique_column} = %s)"
    with engine.connect() as connection:
        result = connection.execute(query, (unique_value,))
        return result.fetchone()[0]
    

config = read_config('sources.config.json')

source_df, articles_df = scrape_articles(config)
pd.set_option('display.max_columns', None)
database_url = 'postgresql+psycopg2://oceanic_impact:password@34.16.77.94'
engine = create_engine(database_url)

existing_titles = pd.read_sql_query("SELECT title FROM article", engine)['title']
existing_sources = pd.read_sql_query("SELECT name FROM source", engine)['name']

articles_df['date_published'] = pd.to_datetime(articles_df['date_published'], errors='coerce')
articles_df['date_edited'] = pd.to_datetime(articles_df['date_edited'], errors='coerce')

#handles NAT (not a time) by setting to a dummy time, will need to change later to universal null or change postgres permissions
default_datetime = pd.Timestamp('1900-01-01')
articles_df['date_published'].fillna(default_datetime, inplace=True)
articles_df['date_edited'].fillna(default_datetime, inplace=True)


filtered_articles = articles_df[~articles_df['title'].isin(existing_titles)]
filtered_sources = source_df[~source_df['name'].isin(existing_sources)]

filtered_sources.to_sql('source', con=engine, if_exists='append', index=False)
filtered_articles.to_sql('article', con=engine, if_exists='append', index=False)
