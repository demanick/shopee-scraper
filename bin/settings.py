import csv
import os
import yaml


ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class Settings(object):
    def __init__(self):
        # load params from config
        with open(os.path.join(ROOT, 'settings.yaml')) as config_file:
            params = yaml.load(config_file)

        # environment
        self.root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

        # extract HTTP params
        self.headers = params['headers']
        self.max_requests = params['max_requests']

        # extract MySQL params
        self.host = params['host']
        self.user = params['user']
        self.pw = params['pw']
        self.db_name = params['db_name']

        # extract proxy params
        self.proxy_user = params['proxy_user']
        self.proxy_pass = params['proxy_pass']
        self.proxy_port = params['proxy_port']

        # extract Redis params
        self.redis_host = params['redis_host']
        self.redis_port = params['redis_port']
        self.redis_db = params['redis_db']

        # crawling
        self.category_level = params['category_level']
        self.max_threads = params['max_threads']

        # load proxy ips from csv
        # self.proxy_urls = []
        # with open(os.path.join(ROOT, 'data', 'ips-static.csv'), 'r') as csvfile:
            # proxyreader = csv.reader(csvfile)
            # for row in proxyreader:
                # self.proxy_urls.append(row[0])

if __name__ == '__main__':
    # print params as disctionary
    settings = Settings()
    settings_dict = vars(settings)
    print(settings.headers)

    # recursive function to explode out dict
    def explode_dict(d, offset=0):
        for k, v in d.items():
            print(" "*offset, end="")
            print("{}:".format(k), end="")
            print(" "*(25-offset-len(k)), end="")
            if isinstance(v, dict):
                print()
                explode_dict(v, offset=offset+4)
            else:
                print(v)

    explode_dict(settings_dict)
