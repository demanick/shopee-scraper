import logging
import MySQLdb as mdb
import sys

from datetime import datetime
from settings import Settings
from sys import argv


# set up logger
log = logging.getLogger(__name__)


# instantiate environment settings
settings = Settings()


# connect to database
conn = mdb.connect(host=settings.host, user=settings.user,
                    passwd=settings.pw, db=settings.db_name)
cur = conn.cursor()


def delete_db(date):
    '''Deletes all tables for a specified date'''
    base_tables = ['product_cat_map{}', 'categories_{}', 'products_{}', 'shops_{}']
    date_tables = [t.format(date) for t in base_tables]
    for t in date_tables:
        try:
            cur.execute('''DROP TABLE IF EXISTS {}'''.format(t))
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error(e)
            return -1

    return 0


def create_db(date):
    '''Creates all tables for a specific date'''
    # create shops table first
    try:
        cur.execute('''CREATE TABLE IF NOT EXISTS shops_{} (
            extract_date date,
            id bigint,
            name varchar(2056),
            followers int,
            products int,
            rating float,
            rating_count int,
            PRIMARY KEY (id)
        )'''.format(date))
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(e)
        return -1

    # create products table
    try:
        cur.execute('''CREATE TABLE IF NOT EXISTS products_{} (
            extract_date date,
            id bigint,
            name varchar(2056),
            shopid bigint,
            price_min float,
            price_max float,
            discount int,
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
    except Exception as e:
        conn.rollback()
        log.error(e)
        if delete_db(date) == 0:
            return -1
        else:
            return -2

    # create product_cat_map table
    try:
        cur.execute('''CREATE TABLE IF NOT EXISTS product_cat_map_{} (
            extract_date date,
            productid int,
            catid int,
            FOREIGN KEY (productid) REFERENCES products_{},
            FOREIGN KEY (catid) REFERENCES categories
        )'''.format(date, date))
        conn.commt()
    except Exception as e:
        conn.rollback()
        log.error(e)
        if delete_db(date) == 0:
            return -1
        else:
            return -2

    return 0


# define wrapper function for the purpose of error returning
def create_db_wrapper(date):
    c = create_db(date)
    if c == 0:
        print('db successfully created')
    elif c == -1:
        print('full db could not be created, all tables deleted')
    else:
        print('full db could not be created, tables could not be deleted')


if __name__ == '__main__':
    # make sure user passed arguments
    if len(argv < 2):
        print('This program requires the user to pass at least one argument')
        sys.exit()

    # check if user passed a date parameter
    if len(argv) > 2:
        date = argv[2]
    else:
        date = datetime.now().strftime('%Y_%m_%d')

    # follow instructions as defined by user
    if argv[1] == 'create':
        create_db_wrapper(date)
    elif argv[1] == 'delete':
        if delete_db(date) == 0:
            print('db successfully deleted')
        else:
            print('db could not be deleted')
    elif argv[1] == 'replace':
        if delete_db(date) == 0:
            print('db successfully deleted')
            create_db_wrapper(date)
        else:
            print('db coudl not be deleted')
    else:
        print('{} is an invalid argument, please stick to create, delete, and replace'.format(argv[1]))
