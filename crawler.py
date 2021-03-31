# Script to scrape data off Twitter
import urllib
import firebase_admin
import tweepy as tw
import json
import helpers as helpers
import time

from requests import request
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from uuid import uuid4
from firebase_admin import credentials, firestore
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from concurrent.futures import ThreadPoolExecutor

credential_path = Path('firebase.json').resolve()
key_path = Path('key.json').resolve()
twitter_key_path = Path('twitter_keys.json').resolve()
cred = credentials.Certificate(str(credential_path))

with open(key_path) as f:
    keys = json.load(f)

# load json array of twitter keys
with open(twitter_key_path) as f:
    twitter_keys = json.load(f)

sid = SentimentIntensityAnalyzer()

# CONSUMER_KEY = twitter_keys[key_index]['consumer-key']
# CONSUMER_SECRET = twitter_keys[key_index]['consumer-secret']
# ACCESS_TOKEN = twitter_keys[key_index]['access-token']
# ACCESS_TOKEN_SECRET = twitter_keys[key_index]['access-secret']
API_KEY = keys['finance_key']
FINANCE_URL = 'https://financialmodelingprep.com/api/v3/'

firebase_app = firebase_admin.initialize_app(cred, name='twitter')
client = firestore.client(firebase_app)

class TwitterCrawler():
    def __init__(self):
        pass


    def limit_handled(self, cursor):
        while True:
            try:
                yield next(cursor)
            except tw.RateLimitError:
                time.sleep(15 * 60)


    # Search twitter for this stock
    # Will return list of tweets relating to stock symbol
    def search_stock(self, ticker, keys):
        key_index = 0
        error_status = true
        while (error_status):
            if (key_index >= len(keys)):
                print("Error: No valid keys")
                return

            # recompute keys
            CONSUMER_KEY = twitter_keys[key_index]['consumer-key']
            CONSUMER_SECRET = twitter_keys[key_index]['consumer-secret']
            ACCESS_TOKEN = twitter_keys[key_index]['access-token']
            ACCESS_TOKEN_SECRET = twitter_keys[key_index]['access-secret']

            try: 
                search_stock_helper(self, ticker, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
                error_status = false
            
            except Exception as e:
                ++key_index
                continue


    def search_stock_helper(self, ticker, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET):
        #setting up api
        auth = tw.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        api = tw.API(auth)
        
        tweets = self.limit_handled(tw.Cursor(api.search,
                            q='#{}'.format(ticker),
                            lang="en").items(125))
        tweet_data = []
        for tweet in tweets:
            date_time_obj = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            sentiment = sid.polarity_scores(tweet['text'])
            tweet_obj = {
                'ticker': ticker,
                'timestamp': int(date_time_obj.strftime('%s')),
                'date_crawled': int(time.time()),
                'image': '',
                'id': tweet.id_str,
                'title': '',
                'text': tweet.text,
                'type': 'twitter',
                'site': 'twitter.com',
                'sentiment': 0,
                'title_sentiment': {},
                'text_sentiment': {},
                'url': '',
                'twitter_data': {
                    'user_data': tweet['user'],
                    'reply_count': tweet['reply_count'],
                    'favorite_count': tweet['favorite_count'],
                    'quote_count': tweet['quote_count'],
                    'retweet_count': tweet['retweet_count']
                }
            }

            tweet_obj['text_sentiment']['positive'] = sentiment['pos']
            tweet_obj['text_sentiment']['neutral'] = sentiment['neu']
            tweet_obj['text_sentiment']['negative'] = sentiment['neg']
            tweet_obj['sentiment'] = sentiment['compound']

            tweet_data.append(tweet_obj)
        
        return tweet_data

    
    def crawl(self):
        print("Scraping data from Twitter")
        exchanges = ['Nasdaq Global Select', 'New York Stock Exchange']
        stocks = helpers.get_tickers(exchanges)
        stock_sentences = {} # Dict stock_smbl (str) -> ['',...] 
        with ThreadPoolExecutor() as ex:
            results = list(tqdm(ex.map(self._get_twitter_data, stocks), total=len(stocks)))

    def crawl_ticker(self, ticker):
        return self._get_twitter_data(ticker)

    def _get_twitter_data(self, ticker):
        tweets = self.search_stock(ticker)
        helpers.upload(tweets)
        return None