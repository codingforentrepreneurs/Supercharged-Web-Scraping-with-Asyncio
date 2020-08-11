import fire
import os
import asyncio
from arsenic import get_session, keys, browsers, services
import pandas as pd
from requests_html import HTML
import itertools
import re
import time
import pathlib
from urllib.parse import urlparse
import random

import logging
import structlog # pip install structlog


from supercharged.logging import set_arsenic_log_level
from supercharged.scrapers import scraper

from supercharged.storage import df_from_sql,  df_to_sql, list_to_sql



# /en/fabric/7137786-genevieve-floral-by-crystal_walen
async def extract_id_slug(url_path):
    path = url_path
    if path.startswith('http'):
        parsed_url = urlparse(path)
        path = parsed_url.path
    regex = r"^[^\s]+/(?P<id>\d+)-(?P<slug>[\w_-]+)$"
    group = re.match(regex, path)
    if not group:
        return None, None, path
    return group['id'], group['slug'], path



async def get_product_data(url, content):
    id_, slug_, path = await extract_id_slug(url)
    titleEl = content.find(".design-title", first=True)
    data = {
        'id': id_,
        'slug': slug_,
        'path': path,
    }
    title = None
    if titleEl == None:
        return data
    title = titleEl.text
    data['title'] = title
    sizeEl = content.find("#fabric-size", first=True)
    size = None
    if sizeEl != None:
        size = sizeEl.text
    data['size'] = size
    price_parent_el = content.find('.b-item-price', first=True)
    price_el = price_parent_el.find('.visuallyhidden', first=True)
    for i in price_el.element.iterchildren():
        attrs = dict(**i.attrib)
        try:
            del attrs['itemprop']
        except:
            pass
        attrs_keys = list(attrs.keys())
        data[i.attrib['itemprop']] = i.attrib[attrs_keys[0]]
    return data

async def get_parsable_html(body_html_str):
    return HTML(html=body_html_str)

async def get_links(html_r):
    fabric_links = [x for x in list(html_r.links) if x.startswith("/en/fabric")]
    datas = []
    for path in fabric_links:
        id_, slug_, _ = await extract_id_slug(path)
        data = {
            "id": id_,
            "slug": slug_,
            "path": path,
            "scraped": 0 # True / False -> 1 / 0 
        }
        datas.append(data)
    return datas

async def spoonflower_scraper(url, i=-1, timeout=60, start=None):
    body = await scraper(url, i=i, timeout=timeout, start=start, body_delay=10)
    content = await get_parsable_html(body) 
    links = await get_links(content)
    product_data = await get_product_data(url, content)
    if start != None:
        end = time.time() - start
        print(f'{i} took {end} seconds')
    # print(body)
    dataset = {
        "links": links,
        "product_data": product_data
    }
    return dataset

async def run(urls, timeout=60, start=None):
    results = []
    for i, url in enumerate(urls):
        results.append(
            asyncio.create_task(spoonflower_scraper(url, i=i, timeout=60, start=start))
        )
    list_of_links = await asyncio.gather(*results)
    return list_of_links

def get_saved_urls(limit=5):
    links_df = df_from_sql('spoonflower_links')
    urls = []
    scraped_ids = []
    used_df = False
    if not links_df.empty:
        sub_links_df = links_df.copy()
        sub_links_df = sub_links_df[sub_links_df['scraped'] == 0]
        sub_links_df = sub_links_df.sample(limit)
        urls = [f"https://www.spoonflower.com{x}" for x in sub_links_df.path.tolist()]
        scraped_ids = sub_links_df.id.tolist()
        if len(urls) > 0:
            used_df = True
    return urls, scraped_ids, used_df

def get_list_range(limit=10, is_random=True, random_max=150):
    urls = []
    for i in range(limit):
        if is_random:
            page = random.randint(i+1, random_max)
        else:
            page = i + 1
        urls.append(f"https://www.spoonflower.com/en/shop?on=fabric&page_offset={page}")
    return urls

def run_spoonflower(use_links=True, use_list_range=False, is_random=True, save_csv=False, limit=10):
    set_arsenic_log_level()
    start = time.time()
    urls = ['https://www.spoonflower.com/en/shop?on=fabric']
    scraped_ids = []
    used_df = False
    if use_links == True and use_list_range == False:
        urls, scraped_ids, used_df = get_saved_urls(limit=limit)
    if use_list_range == True:
        urls = get_list_range(limit=limit, is_random=is_random)
    results = asyncio.run(run(urls, start=start))
    end = time.time() - start
    links = [x['links'] for x in results] # [[], [], []]
    links = itertools.chain.from_iterable(links)
    links = list(links)
    link_columns = ['id', 'slug', 'path', 'scraped']
    list_to_sql(datas=links, 
        table_name='spoonflower_links',
        columns=link_columns)
    product_data = [x['product_data'] for x in results]
    product_columns = ['id', 'slug', 'path', 'title', 'size', 'price', 'priceCurrency', 'priceValidUntil']
    list_to_sql(datas=product_data,                 
            table_name='spoonflower_fabrics', 
            columns=product_columns)
    if used_df:
        links_df = df_from_sql('spoonflower_links')
        link_cond = links_df['id'].isin(scraped_ids)
        links_df.loc[link_cond, 'scraped'] = 1
        df_to_sql(links_df, table_name='spoonflower_links')
    if save_csv:
        links_df = df_from_sql('spoonflower_links')
        links_df.to_csv('spoonflower_links.csv')
        fabrics_df = df_from_sql('spoonflower_fabrics')
        fabrics_df.to_csv('spoonflower_fabrics.csv')
    # return results