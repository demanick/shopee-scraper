import requests


def main():
    '''Estimates the size of Shopee data'''
    # retrieve data on Shopee cateogries in JSON format
    url = 'https://shopee.sg/api/v1/category_list/'
    r = requests.get(url)
    json_doc = r.json()

    # estimate size of data using main categories
    cats = len(json_doc)
    print("Main Categories:\t{}".format(8100*cats))

    # estimate size of data using tier 1 & tier 2 sub categories
    sub_cats1 = 0
    sub_cats2 = 0
    for i in range(cats):
        sub_cats1 += len(json_doc[i]['sub'])
        for j in range(len(json_doc[i]['sub'])):
            sub_cats2 += len(json_doc[i]['sub'][j]['sub_sub'])
    print("Tier 1 Categories:\t{}".format(8100*sub_cats1))
    print("Tier 2 Categories:\t{}".format(8100*sub_cats2))


if __name__ == '__main__':
    main()
