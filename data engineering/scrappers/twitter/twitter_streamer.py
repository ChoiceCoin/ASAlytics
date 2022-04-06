# Imports 
from base64 import encode
import datetime
import json
import time
import os
import logging
from google.cloud import  pubsub_v1
import tweepy
from tweepy.streaming import StreamListener

# Neccessary credentials
from twitterconfig import twitter as config, pubsub as pubsub_config


## GCP credentials
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath('real-time-key-1.json')

## Twitter credentials
CONSUMER_KEY = config['consumer_key']
CONSUMER_SECRET = config['consumer_secret']
ACCESS_KEY = config['access_key']
ACCESS_SECRET = config['access_secret']

# PubSub credentials
PROJECT_ID = pubsub_config['Project_ID']
TOPIC_ID = pubsub_config['Topic_ID']

## PubSub object
publisher  = pubsub_v1.PublisherClient()
pubsub_cloudsql = publisher.topic_path(PROJECT_ID, TOPIC_ID)

## Tweepy Authentication
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

## Tweepy API
api = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=False)


def write_to_pubsub(data:dict):
    """
    This function serializes data into pubsub form and publishes the data.
    data:[dict] json data to be serialized
    """
    try:
        if data["lang"] == "en": ## Only english tweets
            publisher.publish(pubsub_cloudsql, data=json.dumps({
                "id": data["id"],
                "user_id": data["user_id"],
                "text": data["text"],
                # "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S'),
                "retweets": data["retweet_count"],
                "likes": data["favorite_count"],
                "hashtags": data["hashtags"],

                }).encode("utf-8"))
    except Exception as e:
        logging.info("Streaming stopped")
        raise

# Method to format a tweet from tweepy
def reformat_tweet(tweet):

    """ Format tweets object into prefered Json schema """

    x = tweet

    processed_doc = {
        "id": x["id"],
        "lang": x["lang"],
        "retweeted_id": x["retweeted_status"]["id"] if "retweeted_status" in x else None,
        "favorite_count": x["favorite_count"] if "favorite_count" in x else 0,
        "retweet_count": x["retweet_count"] if "retweet_count" in x else 0,
        "coordinates_latitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "coordinates_longitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "place": x["place"]["country_code"] if x["place"] else None,
        "user_id": x["user"]["id"],
        "created_at": time.mktime(time.strptime(x["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
    }

    if x["entities"]["hashtags"]:
        processed_doc["hashtags"] = [{"text": y["text"], "startindex": y["indices"][0]} for y in
                                     x["entities"]["hashtags"]]
    else:
        processed_doc["hashtags"] = []

    if x["entities"]["user_mentions"]:
        processed_doc["usermentions"] = [{"screen_name": y["screen_name"], "startindex": y["indices"][0]} for y in
                                         x["entities"]["user_mentions"]]
    else:
        processed_doc["usermentions"] = []

    if "extended_tweet" in x:
        processed_doc["text"] = x["extended_tweet"]["full_text"]
    elif "full_text" in x:
        processed_doc["text"] = x["full_text"]
    else:
        processed_doc["text"] = x["text"]

    return processed_doc

# Custom listener class
class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to pubsub
    """

    def __init__(self):
        super(StdOutListener, self).__init__()
        self._counter = 0

    def on_status(self, data):
        write_to_pubsub(reformat_tweet(data._json))
        self._counter += 1
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False


logging.getLogger().setLevel(logging.INFO)
# Start listening

## List of key words
lst_hashtags = ['Binance', 'Crypto', 'crypto currency', 'Algorand', 'Algorand standard asset']
## Listerner object
l = StdOutListener()

## Streaming
stream = tweepy.Stream(auth, l, tweet_mode='extended')
stream.filter(track=lst_hashtags)
