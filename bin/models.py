import json
import logging
import MySQLdb as mdb
import os

from datetime import datetime
from db_setup import create_db
from helpers import make_request
from settings import Settings


log = logging.getLogger(__name__)


settings = Settings()


conn = mdb.connect(host=settings.host, user=settings.user,
                    passwd=settings.pw, db=settings.db_name)
cur = conn.cursor()


class Product(object):
    def __init__(self, date, timestamp, json_obj):
        # set date
        self.date = date
        self.timestamp = timestamp

        # all data is nested under 'item' key
        item_json = json_obj['item']

        # extract data
        self.id = item_json['itemid']
        self.name = item_json['name']
        self.catids = [c['catid'] for c in item_json['categories']]
        self.shopid = item_json['shopid']
        self.price_min = item_json['price_min']
        self.price_max = item_json['price_max']
        self.discount = item_json['show_discount']
        self.currency = item_json['currency']
        self.rating = item_json['item_rating']['rating_star']
        self.rating_count = self._rating_count(item_json)
        self.likes = item_json['liked_count']
        self.comments = item_json['cmt_count']
        self.sold = item_json['sold']
        self.stock = item_json['stock']
        self.free_shipping = item_json['show_free_shipping']

    def map(self):
        for catid in self.catids:
            try:
                cur.execute('''
                INSERT INTO product_id_map_{} (
                    extract_date,
                    catid,
                    productid
                )
                VALUES (%s, %s, %s)'''.format(self.date), (
                    self.timestamp,
                    catid,
                    self.id
                ))
                conn.commit()
            except Exception as e:
                log.error(e)
                conn.rollback()

    def save(self):
        try:
            cur.execute('''
            INSERT INTO products_{} (
                extract_date,
                id,
                name,
                shopid,
                price_max,
                price_min,
                discount,
                currency,
                rating,
                rating_count,
                comments,
                likes,
                sold,
                stock,
                free_shipping
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''.format(self.date), (
                self.timestamp,
                self.id,
                self.name,
                self.shopid,
                self.price_min,
                self.price_max,
                self.discount,
                self.currency,
                self.rating,
                self.rating_count,
                self.likes,
                self.comments,
                self.sold,
                self.stock,
                self.free_shipping
            ))
            conn.commit()
            return 0
        except Exception as e:
            log.error(e)
            conn.rollback()
            return -1

    def _rating_count(self, item_json):
        rcount = 0
        for i in range(6):
            rcount += item_json['item_rating']['rating_count'][i]
        return rcount


class Shop(object):
    def __init__(self, date, timestamp, json_obj):
        # set date attribute
        self.date = date
        self.timestamp = timestamp

        # data is nested under data key
        shop_json = json_obj['data']

        # extract data
        self.id = shop_json['shopid']
        self.name = shop_json['name']
        self.followers = shop_json['follower_count']
        self.products = shop_json['item_count']
        self.rating = shop_json['account']['total_avg_star']
        self.rating_count = sum((shop_json['rating_good'], shop_json['rating_normal'], shop_json['rating_bad']))
        self.mall = shop_json['is_official_shop']

    def save(self):
        try:
            cur.execute('''
            INSERT INTO shops_{} (
                extract_date,
                id,
                name,
                followers,
                products,
                rating,
                rating_count,
                mall
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''.format(self.date), (
                self.timestamp,
                self.id,
                self.name,
                self.followers,
                self.products,
                self.rating,
                self.rating_count,
                self.mall
            ))
            conn.commit()
            return 0
        except Exception as e:
            log.error(e)
            conn.rollback()
            return -1


if __name__ == '__main__':
    # run unit test for file
    create_db('temp')
    timestamp = datetime.now().strftime('%Y-%m-%d')

    # extract testing data from files
    with open(os.path.join(settings.root, 'data', 'shop.json'), 'r') as read_file:
        test_shop_json = json.load(read_file)

    with open(os.path.join(settings.root, 'data', 'product.json'), 'r') as read_file:
        test_product_json = json.load(read_file)

    # instantiate Product and Shop objects
    shop = Shop('temp', timestamp, test_shop_json)
    product = Product('temp', timestamp, test_product_json)

    # test saving
    if shop.save() == 0:
        print('saving shop record successful')
    else:
        print('saving shop record failed')

    if product.save() == 0:
        print('Saving product record successful')
    else:
        print('Saving product record failed')
