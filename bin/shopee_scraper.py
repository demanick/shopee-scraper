import csv
import logging.config
import os
import requests
import sys

from objects.item import ShopeeItem
from time import sleep


ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class ScrapeError(Exception):
    pass


def shopee_scraper():
    # set up variables for scraping
    url = "https://shopee.sg/api/v2/search_items/"
    headers = {
        'user-agent': 'Mozilla/5.0'
    }
    payload = {
        'by': 'pop',
        'limit': 100,
        'match_id': 2,
        'newest': 0,
        'order': 'desc',
        'page_type': 'search'
    }

    # set up variables for storing and tracking
    items = []
    failure = 0
    running=True

    # loop through data until all is collected
    while running:
        # get json
        try:
            r = requests.get(url, headers=headers, params=payload)
        except Exception as e:
            # keep track of failures and exit program if 5 in a row
            logging.error('Exception occurred', exc_info=True)
            logging.error('items %i - %i: FAILED' % (payload['newest'], payload['newest'] + payload['limit']))
            failure += 1
            if failure >= 5:
                raise ScrapeError("5 requests failed in a row, check logs")
            continue

        # iterate over items
        json_obj = r.json()
        # end loop if no more items
        if len(json_obj['items']) < 100:
            running = False
        for item in json_obj['items']:
            # initialize a ShopeeItem class and extract item data
            s = ShopeeItem(item['itemid'], item['shopid'])
            if s.collect_data() == 0:
                temp_dict = vars(s)
                temp_dict.pop('raw_json', None)
                items.append(temp_dict)
                logging.debug('item %s: SUCCESS' % item['itemid'])
            else:
                logging.error('item %s: FAILED' % item['itemid'])
                logging.error(s.raw_json)
            # sleep for 3 seconds to give server a rest
            sleep(3)

        # log and update
        logging.info('items %i - %i: SUCCESS' % (payload['newest'], payload['newest'] + payload['limit']))
        payload['newest'] += payload['limit']
        # sleep for 3 seconds to give server a rest
        sleep(3)
        break

    # write the whole thing to a csv
    file_path = os.path.join(ROOT, 'data', 'shopee.csv')
    with open(file_path, 'w', encoding='utf-8', newline='') as csvfile:
        fieldnames = items[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(items)
    print("\nCOMPLETE, FIND DATA AT {}".format(file_path))


if __name__ == '__main__':
    # configure logger
    logging.config.fileConfig(os.path.join(ROOT, 'bin', 'logging.conf'))
    # run scraper
    shopee_scraper()
