import eventlet
import logging
import random
import redis

from requests.exceptions import RequestException
from settings import Settings


log = logging.getLogger(__name__)


settings = Settings()


num_requests= 0


requests = eventlet.import_patched('requests.__init__')
time = eventlet.import_patched('time')


redis = redis.StrictRedis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db)


def make_request(url, return_json=True):
    '''makes http request to url via requests lib'''
    # check that num requests has not exceeded user limit
    global num_requests
    if num_requests > settings.max_requests:
        raise Exception("Reached max number of requests: {}".format(settings.max_requests))

    proxy_urls = get_proxy()
    try:
        r = requests.get(url, headers=settings.headers, proxies=proxy_urls)
    except RequestException as e:
        log.info("Request for %s failed." % url)
        return make_request(url)

    num_requests += 1

    if r.status_code != 200:
        log.warning("Got a %i response code for %s" % (r.status_code, url))
        return None

    return r.json()


def get_proxy():
    # choose a proxy server to use for this request, if we need one
    if not settings.proxy_urls or len(settings.proxy_urls) == 0:
        return None

    proxy_ip = random.choice(settings.proxy_urls)
    proxy_url = "http://{user}:{passwd}@{ip}:{port}/".format(
        user=settings.proxy_user,
        passwd=settings.proxy_pass,
        ip=proxy_ip,
        port=settings.proxy_port,

    )

    return {
        "http": proxy_url,
        "https": proxy_url
    }

def enqueue_url(queue, url):
    return redis.sadd(queue, url)

def dequeue_url(queue):
    return redis.spop(queue)


if __name__ == '__main__':
    # test make_request
    url = "https://shopee.co.id/api/v2/search_items/?by=pop&limit=100&match_id=33&newest=8000&order=desc&page_type=search"
    json_obj = make_request(url)
    print(json_obj)
    # test Redis
    print(enqueue_url(url, "test_queue"))
    print(dequeue_url("test_queue"))
