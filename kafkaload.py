import tweepy, json, certifi
from elasticsearch import Elasticsearch
from datetime import datetime
import secretsAndSettings as sas
import geoTwitterSettings as gts
from random import randint
from pykafka import KafkaClient
from kafka import KafkaProducer

auth = tweepy.OAuthHandler(sas.twitterKeys['consumer_key'],
                           sas.twitterKeys['consumer_secret'])
auth.set_access_token(sas.twitterKeys['access_token'],
                      sas.twitterKeys['access_token_secret'])
api = tweepy.API(auth)

#client = KafkaClient(hosts="127.0.0.1:9092")
topic = 'test'
producer = KafkaProducer(bootstrap_servers="localhost:2181")



wordsToTrack = ['Baseball', 'Football', 'Darts', 'Soccer', 'Basketball', 'Cricket']
# def lowerCase(word): return str(word).lower()
# wordsToTrack.extend(map(lowerCase, wordsToTrack))

geoTweetIndexName = "geo-tweets"


#
def getGeoCode(tweet):
    #print (tweet)
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

class KafkaStreamListener(tweepy.StreamListener):


    count = 0
    def on_data(self, tweet_data):
        try:
            tweet = json.loads(tweet_data)
            tweet["location"] = getGeoCode(tweet)
            producer.send(topic, tweet)
            print (producer.send(topic, tweet))

            # with topic.get_sync_producer() as producer:
            #     producer.produce(tweet)

        except:
            pass

myStream = tweepy.Stream(auth = api.auth, listener=KafkaStreamListener(), timeout=30000)
while (True):
    try:
        myStream.filter(track=wordsToTrack)
    except:
        continue
