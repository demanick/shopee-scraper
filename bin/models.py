import MySQLdb as mdb


host = "scrapingdb.cexqjqkwvxs5.us-east-1.rds.amazonaws.com"
user = "nad385"
pw = "9dETROIT^"
db_name = "shopee"

conn = mdb.connect(host=host, user=user, passwd=pw, db=db_name)
cur = conn.cursor()


class Product(object):
    def __init__(self, json_obj):
        # all data is nested under 'item' key
        item_json = json_obj['item']

        # extract data
        self.id = item_json['itemid']
        self.name = item_json['name']
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

    def save(self):
        try:
            cur.execute('''
            INSERT INTO products (
                id,
                name,
                shop_id,
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
            ''',(
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
        except:
            conn.rollback()
            return -1

    def _rating_count(self, item_json):
        rcount = 0
        for i in range(6):
            rcount += item_json['item_rating']['rating_count'][i]
        return rcount


class Shop(object):
    def __init__(self, json_obj):
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
            INSERT INTO (
                id,
                name,
                followers,
                products,
                rating,
                rating_count,
                mall
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',(
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
        except:
            conn.rollback()
            return -1


if __name__ == '__main__':
    # set up tables
    cur.execute('''CREATE TABLE IF NOT EXISTS products (
        id bigint,
        name varchar(2056),
        followers int,
        products int,
        rating float,
        ratings_count int,
        PRIMARY KEY (id)
    )''')
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS products (
        id bigint,
        name varchar(2056),
        shopid bigint,
        price_min float,
        price_max float,
        show_discount int,
        currency varchar(50),
        rating float,
        rating_count int,
        likes int,
        comments int,
        sold int,
        stock int,
        free_shipping boolean,
        PRIMARY KEY (id),
        FOREIGN KEY (shopid) REFERENCES shops(id)
    );''')
    conn.commit()
