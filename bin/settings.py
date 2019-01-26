import os
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class Settings(object):
    def __init__(self):
        # load params from config
        with open(os.path.join(ROOT, 'settings.yaml')) as config_file:
            params = yaml.load(config_file)

        # extract HTTP params
        self.headers = params['headers']
        self.max_requests = params['max_requests']

        # extract proxy params
        self.proxy_urls = params['proxy_urls']
        self.proxy_user = params['proxy_user']
        self.proxy_pass = params['proxy_pass']
        self.proxy_port = params['proxy_port']

        # extract Redis params
        self.redis_host = params['redis_host']
        self.redis_port = params['redis_port']
        self.redis_db = params['redis_db']

if __name__ == '__main__':
    # print params as disctionary
    settings = Settings()
    settings_dict = vars(settings)

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
