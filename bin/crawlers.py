import logging
import os

from helpers import dequeue_url, enqueue_url, make_request
from models import Product, Shop
from settings import Settings

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

settings = Settings ()

# global variables for jeeping track of shops and products
product_counter = 0
shop_cache = []


def create_directory_urls():
    # extract category ids
    category_file = '{}.txt'.format(settings.category_level)
    with open(os.path.join(ROOT, 'data', category_file)) as f:
        categories = [int(x) for x in f.read().split('\n')]

    # iterate through categories to create urls
    directory_url = 'https://shopee.co.id/api/v2/search_items/?by=pop&limit=100&match_id={id}&newest={newest}&order=desc&page_type=search'
    for c in categories:
        for i in range(0, 8100, 100):
            enqueue_url('directory_q', directory_url.format(id=c, newest=i))
        logging.info('urls created for category: %i' % c)

    logging.info('directory urls successfully seeded')


def harvest_directories():
    # bring in globals
    global product_counter
    global shop_cache

    # grab directory url
    directory_url = dequeue_url('directory_q')
    if not directory_url:
        logging.info('COMPLETE: directory url processing')
        logging.info('%i product urls processed' % product_counter)
        logging.info('%i shop urls processed' % len(shop_cache))

    # extract json
    json_obj = make_request(directory_url)

    # format data in to shop and product urls
    product_url = 'https://shopee.co.id/api/v2/item/get?itemid={itemid}&shopid={shopid}'
    shop_url = 'https://shopee.co.id/api/v2/shop/get?is_brief=1&shopid={shopid}'
    for item in json_obj['items']:
        enqueue_url('product_q', product_url.format(itemid=item['itemid'], shopid=item['shopid']))
        product_counter += 1
        # only add shopid if new
        if item['shopid'] not in shop_cache:
            shop_cache.append(item['shopid'])
            enqueue_url('shop_q', shop_url.format(shopid=item['shopid']))


def harvest_products():
    # pop url from redis db
    url = dequeue_url('product_q')
    if not url:
        logging.info('COMPLETE: product url processing')

    # process and insert product data
    json_obj = make_request(url)
    product = Product(json_obj)
    product.save()
    logging.info('COMPLETE: product (%i); shop (%i)' % (product.id, product.shopid))


def harvest_shops():
    # pop url from redis db
    url = dequeue_url('shop_q')
    if not url:
        logging.info('COMPLETE: shop url processing')

    # process and insert shop data
    json_obj = make_request(url)
    shop = Shop(json_obj)
    shop.save()
    logging.info('COMPLETE: shop (%i)' % shop.id)
