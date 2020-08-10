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

import logging
import structlog # pip install structlog


from supercharged.logging import set_arsenic_log_level
from supercharged.scrapers import scraper

from supercharged.storage import store_links_as_df_pickle



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
    body = await scraper(url, i=i, timeout=timeout, start=start, body_delay=10) # save this locally??
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

def run_spoonflower():
    set_arsenic_log_level()
    start = time.time()
    urls = ['https://www.spoonflower.com/en/shop?on=fabric', 
            'https://www.spoonflower.com/en/fabric/6444170-catching-fireflies-by-thestorysmith']
    name = "link.pkl"
    results = asyncio.run(run(urls, start=start))
    print(results)
    end = time.time() - start
    print(f'total time is {end}')
    df = store_links_as_df_pickle(results, name=name)
    print(df.head())