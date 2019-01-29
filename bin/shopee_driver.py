import eventlet
import logging
import os

from crawlers import (create_directory_urls, harvest_directories,
                      harvest_products, harvest_shops)
from settings import Settings
from sys import argv


# instantiate settings variables
settings = Settings()


# create GreenPools and Piles
pool = eventlet.GreenPool(settings.max_threads)
pile = eventlet.GreenPile(pool)


def shopee_scraper():
    # begin processing of directory urls
    logging.info('STARTING: processing directory urls')
    [pile.spawn(harvest_directories) for _ in range(settings.max_threads)]
    pool.waitall()
    # begin processing of product urls
    logging.info('STARTING: processing product urls')
    [pile.spawn(harvest_products) for _ in range(settings.max_threads)]
    pool.waitall()
    # being processing of shop urls
    logging.info('STARTING: processing shop urls')
    [pile.spawn(harvest_shops) for _ in range(settings.max_threads)]
    pool.waitall()


def test_shopee_scraper():
    pass


if __name__ == '__main__':
    # set up log file
    logging.config.fileConfig(os.path.join(settings.root, 'bin', 'logging.conf'))

    # seed urls if first run
    if len(argv) > 1 and argv[2] == 'seed':
        logging.infor('STARTING: seeding direcotry urls')
        create_directory_urls()

    # test if param passed, otherwise run
    if len(argv) > 2 and argv[3] == 'test':
        test_shopee_scraper()
    else:
        shopee_scraper()
