import eventlet
eventlet.monkey_patch()
import logging
import logging.config
import os
import redis
import sys

from datetime import datetime
from db_setup import create_db
from helpers import dequeue_url, enqueue_url, make_request
from models import Product, Shop
from settings import Settings
from sys import argv


# collect date of run
GLOBAL_DATETIME = datetime.now()
DATE = GLOBAL_DATETIME.strftime('%Y_%m_%d')
TIMESTAMP = GLOBAL_DATETIME.strftime('%Y-%m-%d')


# instantiate settings variables
settings = Settings()


# create GreenPools and Piles
pool = eventlet.GreenPool(settings.max_threads)
pile = eventlet.GreenPile(pool)


# set up Redis
redis = redis.StrictRedis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db)


# global variables for jeeping track of shops and products
product_counter = 0
shop_cache = []


def create_directory_urls():
    # extract category ids
    category_file = '{}.txt'.format(settings.category_level)
    with open(os.path.join(settings.root, 'data', category_file)) as f:
        categories = [int(x) for x in f.read().split('\n')[:-1]]

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

    logging.info('COMPLETE: directory at %s' % directory_url)
    pile.spawn(harvest_directories)


def harvest_products():
    # pop url from redis db
    url = dequeue_url('product_q')
    print(url)
    if not url:
        logging.info('COMPLETE: product url processing')
        return

    # process and insert product data
    json_obj = make_request(url)
    product = Product(DATE, TIMESTAMP, json_obj)
    # map and save products
    product.map()
    if product.save() == 0:
        logging.info('COMPLETE: product (%i); shop (%i)' % (product.id, product.shopid))
    else:
        logging.warning('INSERT FAIL: product (%i); shop (%i)' % (product.id, product.shopid))

    pile.spawn(harvest_products)


def harvest_shops():
    # pop url from redis db
    url = dequeue_url('shop_q')
    if not url:
        logging.info('COMPLETE: shop url processing')
        return

    # process and insert shop data
    json_obj = make_request(url)
    shop = Shop(DATE, TIMESTAMP, json_obj)
    if shop.save() == 0:
        logging.info('COMPLETE: shop (%i)' % shop.id)
    else:
        logging.info('INSERT FAIL: shop (%i)' % shop.id)

    pile.spawn(harvest_shops)


def shopee_scraper():
    # begin processing of directory urls
    logging.info('STARTING: processing directory urls')
    [pile.spawn(harvest_directories) for _ in range(settings.max_threads)]
    pool.waitall()
    # being processing of shop urls
    logging.info('STARTING: processing shop urls')
    [pile.spawn(harvest_shops) for _ in range(settings.max_threads)]
    pool.waitall()
    # begin processing of product urls
    logging.info('STARTING: processing product urls')
    [pile.spawn(harvest_products) for _ in range(settings.max_threads)]
    pool.waitall()


if __name__ == '__main__':
    # set up log file
    logging.config.fileConfig(os.path.join(settings.root, 'bin', 'logging.conf'), disable_existing_loggers=False)

    # seed urls if first run
    if len(argv) > 1 and argv[1] == 'seed':
        logging.info('FLUSHING REDIS DB')
        redis.flushdb()
        logging.info('STARTING: seeding direcotry urls')
        create_directory_urls()

    # look for second positional arg
    if len(argv) > 2 and argv[2] == 'create':
        logging.info('CREATING NEW SET OF DBS FOR {}'.format(DATE))
        if create_db == 0:
            logging.info('COMPLETE: db set up')
        else:
            logging.error('DB SET UP FAILED')
            logging.error('EXITING PROGRAM')
            sys.exit()

    shopee_scraper()
