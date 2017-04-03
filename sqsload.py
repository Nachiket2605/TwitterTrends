import tweepy, json, certifi
from elasticsearch import Elasticsearch
from datetime import datetime
import secretsAndSettings as sas
import geoTwitterSettings as gts
from random import randint
import boto.sqs
from boto.sqs.message import Message

auth = tweepy.OAuthHandler(sas.twitterKeys['consumer_key'],
                           sas.twitterKeys['consumer_secret'])
auth.set_access_token(sas.twitterKeys['access_token'],
                      sas.twitterKeys['access_token_secret'])
api = tweepy.API(auth)



conf = {"sqs-access-key": "",
        "sqs-secret-key": "",
        "sqs-queue-name": "tweet_queue",
        "sqs-region": "us-east-1",
        "sqs-path": "sqssend"

        }

conn = boto.sqs.connect_to_region(
    conf.get('sqs-region'),
    aws_access_key_id=conf.get('sqs-access-key'),
    aws_secret_access_key=conf.get('sqs-secret-key')
)

q = conn.get_queue('tweet_queue')

wordsToTrack = ['Baseball', 'Football', 'Darts', 'Soccer', 'Basketball', 'Cricket']
# def lowerCase(word): return str(word).lower()
# wordsToTrack.extend(map(lowerCase, wordsToTrack))

geoTweetIndexName = "geo-tweets"

def getGeoCode(tweet):
    try:
        bb = tweet['quoted_status']['place']['bounding_box']["coordinates"][0]
        averageCoordinates = {
            'lat': 0.,
            'lon': 0.,
        }

        for coordinate in bb:
            averageCoordinates['lat'] += float(bb[0])
            averageCoordinates['lon'] += float(bb[1])

        averageCoordinates['lat'] /= float(len(bb))
        averageCoordinates['lon'] /= float(len(bb))
        return averageCoordinates
    except:
        pass
    try:
        coordinatesArray = tweet["geo"]["coordinates"]
        coordinatesDict = {}
        coordinatesDict['lat'] = float(coordinatesArray[0])
        coordinatesDict['lon'] = float(coordinatesArray[1])
        return coordinatesDict
    except:
        pass
    try:
        coordinatesArray = tweet["coordinates"]["coordinates"]
        coordinatesDict = {}
        coordinatesDict['lat'] = float(coordinatesArray[0])
        coordinatesDict['lon'] = float(coordinatesArray[1])
        return coordinatesDict
    except:
        coordinatesDict = {}
        coordinatesDict['lat'] = float(randint(-90, 90));
        coordinatesDict['lon'] = float(randint(-180, 180));
        return coordinatesDict

class SQSStreamListener(tweepy.StreamListener):

    conn = boto.sqs.connect_to_region(
        conf.get('sqs-region'),
        aws_access_key_id=conf.get('sqs-access-key'),
        aws_secret_access_key=conf.get('sqs-secret-key')
    )

    q = conn.get_queue('tweet_queue')


    count = 0
    def on_data(self, tweet_data):
        try:
            tweet = json.loads(tweet_data)
            tweet["location"] = getGeoCode(tweet)
            m = RawMessage()
            m.set_body("Hi, how are you doing today?")
            #m.set_body(tweet)
            q.write(m)
            print (m)
        except:
            pass


SQSStreamListenerInstance = ()
myStream = tweepy.Stream(auth = api.auth, listener=SQSStreamListenerInstance)
# while (True):
#     try:
#         myStream.filter(track=wordsToTrack)
#     except:
#         continue
