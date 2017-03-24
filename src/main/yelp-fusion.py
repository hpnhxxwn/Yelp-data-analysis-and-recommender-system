# -*- coding: utf-8 -*-
"""
Yelp Fusion API code sample.
This program demonstrates the capability of the Yelp Fusion API
by using the Search API to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query.
Please refer to http://www.yelp.com/developers/v3/documentation for the API
documentation.
This program requires the Python requests library, which you can install via:
`pip install -r requirements.txt`.
Sample usage of the program:
`python sample.py --term="bars" --location="San Francisco, CA"`
"""
from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode

import threading, logging, time

from kafka import KafkaConsumer, KafkaProducer


# OAuth credential placeholders that must be filled in by users.
# You can find them on
# https://www.yelp.com/developers/v3/manage_app
CLIENT_ID = "_tBguORzt1OpGKTutlkLNA"
CLIENT_SECRET = "RdqHXOvYj40uLr5N5H9gJSN1B9Ah8xEDyKpKxp4vi4sa0apP4phQDsUELnN6iiTq"


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 3

class Producer(threading.Thread):
    daemon = True
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    def run(self):
        parser = argparse.ArgumentParser()

        parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                            type=str, help='Search term (default: %(default)s)')
        parser.add_argument('-l', '--location', dest='location',
                            default=DEFAULT_LOCATION, type=str,
                            help='Search location (default: %(default)s)')

        input_values = parser.parse_args()

        try:
            self.query_api(input_values.term, input_values.location)
        except HTTPError as error:
            sys.exit(
                'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                    error.code,
                    error.url,
                    error.read(),
                )
            )

        #while True:
        #    producer.send('my-topic', b"test")
        #    producer.send('my-topic', b"\xc2Hola, mundo!")
        #    time.sleep(1)

    def obtain_bearer_token(self, host, path):
        """Given a bearer token, send a GET request to the API.
        Args:
            host (str): The domain host of the API.
            path (str): The path of the API after the domain.
            url_params (dict): An optional set of query parameters in the request.
        Returns:
            str: OAuth bearer token, obtained using client_id and client_secret.
        Raises:
            HTTPError: An error occurs from the HTTP request.
        """
        url = '{0}{1}'.format(host, quote(path.encode('utf8')))
        assert CLIENT_ID, "Please supply your client_id."
        assert CLIENT_SECRET, "Please supply your client_secret."
        data = urlencode({
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type': GRANT_TYPE,
        })
        headers = {
            'content-type': 'application/x-www-form-urlencoded',
        }
        response = requests.request('POST', url, data=data, headers=headers)
        bearer_token = response.json()['access_token']
        return bearer_token


    def request(self, host, path, bearer_token, url_params=None):
        """Given a bearer token, send a GET request to the API.
        Args:
            host (str): The domain host of the API.
            path (str): The path of the API after the domain.
            bearer_token (str): OAuth bearer token, obtained using client_id and client_secret.
            url_params (dict): An optional set of query parameters in the request.
        Returns:
            dict: The JSON response from the request.
        Raises:
            HTTPError: An error occurs from the HTTP request.
        """
        url_params = url_params or {}
        url = '{0}{1}'.format(host, quote(path.encode('utf8')))
        headers = {
            'Authorization': 'Bearer %s' % bearer_token,
        }

        print(u'Querying {0} ...'.format(url))

        response = requests.request('GET', url, headers=headers, params=url_params)

        return response.json()


    def search(self, bearer_token, term, location):
        """Query the Search API by a search term and location.
        Args:
            term (str): The search term passed to the API.
            location (str): The search location passed to the API.
        Returns:
            dict: The JSON response from the request.
        """

        url_params = {
            'term': term.replace(' ', '+'),
            'location': location.replace(' ', '+'),
        }
        #'limit': SEARCH_LIMIT
        return self.request(API_HOST, SEARCH_PATH, bearer_token, url_params=url_params)


    def get_business(self, bearer_token, business_id):
        """Query the Business API by a business ID.
        Args:
            business_id (str): The ID of the business to query.
        Returns:
            dict: The JSON response from the request.
        """
        business_path = BUSINESS_PATH + business_id

        return self.request(API_HOST, business_path, bearer_token)

    def get_reviews(self, bearer_token, business_id):

        business_path = BUSINESS_PATH + business_id + "/reviews".encode('utf8')

        return self.request(API_HOST, business_path, bearer_token)


    def query_api(self, term, location):
        """Queries the API by the input values from the user.
        Args:
            term (str): The search term to query.
            location (str): The location of the business to query.
        """
        bearer_token = self.obtain_bearer_token(API_HOST, TOKEN_PATH)

        response = self.search(bearer_token, term, location)

        businesses = response.get('businesses')

        if not businesses:
            print(u'No businesses for {0} in {1} found.'.format(term, location))
            return

        while (True):
            for biz in businesses:
                print(u'{0} businesses found, querying business info ' \
                'for the top result "{1}" ...'.format(
                    len(businesses), biz['id']))
                biz_id = biz['id']
                rsp = self.get_reviews(bearer_token, biz_id)
                reviews = rsp.get('reviews')
                print(u'{0} reviews found, querying review info ' \
                'for the business "{1}" ...'.format(len(reviews), biz_id))
                for r in reviews:
                    #print(r)
                    self.producer.send('reviews', bytes(r))
                #pprint.pprint(rsp, indent=2)
                #print(u"user ID {0} gives review {1} ".format(reviews))





        business_id = businesses[0]['id']

        print(u'{0} businesses found, querying business info ' \
            'for the top result "{1}" ...'.format(
                len(businesses), business_id))
        response = get_business(bearer_token, business_id)

        print(u'Result for business "{0}" found:'.format(business_id))
        pprint.pprint(response, indent=2)


        def main():
            parser = argparse.ArgumentParser()

            parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                                type=str, help='Search term (default: %(default)s)')
            parser.add_argument('-l', '--location', dest='location',
                                default=DEFAULT_LOCATION, type=str,
                                help='Search location (default: %(default)s)')

            input_values = parser.parse_args()

            try:
                query_api(input_values.term, input_values.location)
            except HTTPError as error:
                sys.exit(
                    'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                        error.code,
                        error.url,
                        error.read(),
                    )
                )
class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['reviews'])

        for message in consumer:
            print (message)

#if __name__ == '__main__':
#    main()
#Producer().run()
def main():
    threads = [
        Producer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()