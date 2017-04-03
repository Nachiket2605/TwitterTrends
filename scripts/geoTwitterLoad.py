import tweepy, json, certifi
from elasticsearch import Elasticsearch
from datetime import datetime
import secretsAndSettings as sas
import geoTwitterSettings as gts
from random import randint

#Sign into Twitter
auth = tweepy.OAuthHandler(sas.twitterKeys['consumer_key'],
                           sas.twitterKeys['consumer_secret'])
auth.set_access_token(sas.twitterKeys['access_token'],
                      sas.twitterKeys['access_token_secret'])
api = tweepy.API(auth)

#Sign into Elasticsearch
elasticsearch = Elasticsearch(sas.elasticSearch['uri'],
                              port=sas.elasticSearch['port'],
                              use_ssl=sas.elasticSearch['use_ssl'])

#tracked workds
wordsToTrack = ['Baseball', 'Football', 'Darts', 'Soccer', 'Basketball', 'Cricket']
def lowerCase(word): return str(word).lower()
wordsToTrack.extend(map(lowerCase, wordsToTrack))

geoTweetIndexName = "geo-tweets"


if  elasticsearch.indices.exists(geoTweetIndexName):
    elasticsearch.indices.delete(index=geoTweetIndexName)
elasticsearch.indices.create(index=geoTweetIndexName,
                                    ignore=400,
                                    body=gts.geoTweetsSettings)


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


myStream = None
count = 0

class ElasticSearchFeederStreamListener(tweepy.StreamListener):
    count = 0
    def on_data(self, tweet_data):
        try:
            tweet = json.loads(tweet_data)
            tweet["location"] = getGeoCode(tweet)


            print (elasticsearch.index(index=geoTweetIndexName, doc_type="tweet", body=tweet))
            ElasticSearchFeederStreamListener.count += 1
            # if (ElasticSearchFeederStreamListener.count > 100):
            #     myStream.disconnect()

        except:
            pass

elasticSearchFeederStreamListenerInstance = ElasticSearchFeederStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=elasticSearchFeederStreamListenerInstance)
while (True):
    try:
        myStream.filter(track=wordsToTrack)
    except:
        continue
