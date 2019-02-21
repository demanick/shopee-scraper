import logging
import MySQLdb as mdb

from datetime import datetime
from helpers import make_request
from settings import Settings


log = logging.getLogger(__name__)


settings = Settings()


conn = mdb.connect(host=settings.host, user=settings.user,
                    passwd=settings.pw, db=settings.db_name)
cur = conn.cursor()


def set_up(date):
    cur.execute('''CREATE TABLE IF NOT EXISTS shops_{} (
        extract_date date,
        id bigint,
        name varchar(2056),
        followers int,
        products int,
        rating float,
        ratings_count int,
        PRIMARY KEY (id)
    )'''.format(date))
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS products_{} (
        extract_date date,
        id bigint,
        name varchar(2056),
        shopid bigint,
        price_min float,
        price_max float,
        show_discount int,
        currency varchar(50),
        rating float,
        rating_count int,
        comments int,
        likes int,
        sold int,
        stock int,
        free_shipping boolean,
        PRIMARY KEY (id),
        FOREIGN KEY (shopid) REFERENCES shops_{}(id)
    );'''.format(date, date))
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS productid_map_{} (
        extract_date date,
        productid int,
        catid int
    )'''.format(date))
    conn.commt()
    cur.execute('''CREATE TABLE IF NOT EXISTS categories_{} (
        extract_date date,
        catid int,
        catname varchar(255),
        cattier varchar(255),
        PRIMARY KEY (caitd)
    )'''.format(date))
    conn.commit()
    return 0


class Category(object):
    def __init__(self, date, timestamp, json_obj):
        cat_json = json_obj

        # translate to database immediately, this is a statics object
        for cat in cat_json:
            main = cat['main']
            self._save(date, timestamp, main['catid'], main['name'], 'main')
            # move on to sub json
            sub1_json = cat['sub']
            for sub1 in sub1_json:
                self._save(date, timestamp, sub1['catid'], sub1['name'], 'sub1')
                # move on to sub_sub json
                sub2_json = sub1['sub_sub']
                for sub2 in sub2_json:
                    self._save(date, timestamp, sub2['catid'], sub2['name'], 'sub2')


    def _save(self, date, timestamp, catid, catname, catlevel):
        try:
            cur.execute('''
            INSERT INTO categories_{} (
                extract_date,
                catid,
                catname,
                catlevel
            )
            VALUES (%s, %s, %s, %s)
            )'''.format(date),(
                timestamp,
                catid,
                catname,
                catlevel
            ))
            conn.commit()
            return 0
        except Exception as e:
            log.error(e)
            conn.rollback()
            return -1


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
                catid,
                shopid,
                price_min,
                price_max,
                discount,
                currency,
                rating,
                rating_count,
                likes,
                comments,
                sold,
                stock,
                free_shipping
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                ratings_count,
                mall
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''.format(self.date), (
                self.tiemstamp,
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
    # set up tables
    date = datetime.now().strftime('%Y_%m_%d')
    timestamp = datetime.now().strftime('%Y-%m-%d')
    set_up(date)
    # get data from API endpoint fro categories
    cat_json = make_request('https://shopee.co.id/api/v1/category_list/')
    Category(date, make_request)
    conn.close()
