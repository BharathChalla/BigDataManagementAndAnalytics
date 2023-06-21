# This is a sample Python script.
import json
import sys

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import tweepy
from kafka import KafkaProducer
import logging


"""API ACCESS KEYS"""


bearerToken = "AAAAAAAAAAAAAAAAAAAAAMhSjQEAAAAAaXenG8402z3phmHdTzcoAExU6uw%3DtHucwUNnhsp63TfP0bQXozyzl8nid9ZelZXpAWEUfhpsSqpbcm"


producer = KafkaProducer(bootstrap_servers=sys.argv[1])
search_term = sys.argv[2]
topic_name = sys.argv[3]
# Press the green button in the gutter to run the script.


class TweetListener(tweepy.StreamingClient):

    def on_tweet(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, bytes(str(raw_data.data), 'utf-8'))
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.add_rules(tweepy.StreamRule(search_term))
        self.filter()


if __name__ == '__main__':
    twitter_stream = TweetListener(bearerToken)
    twitter_stream.start_streaming_tweets(search_term)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
