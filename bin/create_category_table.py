''''
Note that this file is more akin to a script but I had to leave it here
because it needs access to the connection params housed in the settings.py file
'''

import MySQLdb as mdb

from helpers import make_request
from settings import Settings


# instantiate settings object
settings = Settings()


# connect to database
conn = mdb.connect(host=settings.host, user=settings.user,
                    passwd=settings.pw, db=settings.db_name)
cur = conn.cursor()


def create():
    # create categories table
    cur.execute('''CREATE TABLE IF NOT EXISTS categories (
        id int,
        name varchar(255),
        tier varchar(255),
        PRIMARY KEY (id)
    )''')
    conn.commit()

    # retrieve category hierarchy
    cat_json = make_request('https://shopee.co.id/api/v1/category_list/')

    # iterate over results
    for cat in cat_json:
        main = cat['main']
        _save_category(main['catid'], main['name'], 'main')
        # move on to sub json
        sub1_json = cat['sub']
        for sub1 in sub1_json:
            _save_category(sub1['catid'], sub1['name'], 'sub1')
            # move on to sub_sub json
            sub2_json = sub1['sub_sub']
            for sub2 in sub2_json:
                _save_category(sub2['catid'], sub2['name'], 'sub2')


def _save_category(id, name, tier):
    try:
        cur.execute('''INSERT INTO categories (
            id,
            name,
            tier
        )
        VALUES (%s, %s, %s)''', (
            id,
            name,
            tier
        ))
        conn.commit()
    except:
        conn.rollback()
        print('Missed category {}, {}, {}'.format(id, name, tier))


if __name__ == '__main__':
    main()
    print('Categories table successfully created')
