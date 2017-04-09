from flask import send_file, jsonify, request
from flask import Flask
import certifi
from elasticsearch import Elasticsearch
import secretsAndSettings as sas
from pykafka import KafkaClient
import json


index = "geo-tweets"
#elasticsearch set up

elasticsearch = Elasticsearch(sas.elasticSearch['uri'],
                              port=sas.elasticSearch['port'],
                              use_ssl=sas.elasticSearch['use_ssl'])

application = Flask(__name__)

def getSimplifiedTweets(query):
    res = elasticsearch.search(index=index, doc_type="tweet", size=500, body=query)
    def getSource(result): return result['_source']
    resSources = list(map(getSource, res['hits']['hits']))
    simplifiedTweets = []
    username = None
    for tweet in resSources:
        if (tweet['user']):
            if (tweet['user']['name']):
                username = tweet['user']['name']
            elif (tweet['user']['screen_name']):
                username = tweet['user']['screen_name']

        simplifiedTweet = {
            'coordinates': {
                'lat': tweet['location']['lat'],
                'long' : tweet['location']['lon']
            },
            'text': tweet['text'],
            'sentiment': tweet['sentiment']
        }
        if (username):
            simplifiedTweet['user'] = username
        else:
            simplifiedTweet['user'] = "Unspecified User"
        simplifiedTweets.append(simplifiedTweet)
    return simplifiedTweets


@application.route('/')
def hello_world():
    #return send_from_directory('html', 'index.html') why doesnt this work??
    return send_file('static/html/index.html')

@application.route('/tweets')
def getTweets():
    query = {
        "sort": {"id": {"order": "desc"}},
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "text" : "soccer football basketball"
                    }
                },
            }
        }
    }
    simplifiedTweets = getSimplifiedTweets(query)
    return jsonify(simplifiedTweets)


@application.route('/sns', methods = ['GET', 'POST', 'PUT'])
def snsFunction():
    #print (request.data)
    notification = (request.data)
    # try:
    #
    # except:
    #     print("Unable to load request")
    #     pass

    headers = request.headers.get('X-Amz-Sns-Message-Type')
    print(notification)

    if headers == 'SubscriptionConfirmation' and 'SubscribeURL' in notification:
        print (notification['SubscribeURL'])
        url = notification['SubscribeURL']
        print(url)
    elif headers == 'Notification':
        getSimplifiedTweets(notification)
    else:
        print("Headers not specified")

    return (notification)





@application.route('/streamPoll', methods=['POST'])
def streamPoll():
    text = request.form["text"]
    query = {
        "sort": {"id": {"order": "desc"}},
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "text": text
                    }
                },
            }
        }
    }
    simplifiedTweets = getSimplifiedTweets(query)
    print("returning " + str(len(simplifiedTweets)) + " tweets.")
    return jsonify(simplifiedTweets)


@application.route('/geoSearch', methods=['POST'])
def searchTweetsByGeoLocation():

    query = {
        "sort": {"id": {"order": "desc"}},
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "text" : "soccer football basketball"
                    }
                },
                "filter": {
                    "geo_distance": {
                        "distance": request.form["distance"] + "km",
                        "location": {
                            "lat": request.form["lat"],
                            "lon": request.form["lng"]
                        }
                    }
                }
            }
        }
    }
    simplifiedTweets = getSimplifiedTweets(query)
    print("returning " + str(len(simplifiedTweets)) + " tweets.")
    return jsonify(simplifiedTweets)

@application.route('/unformattedTweets')
def getAllTweetsUnformatted():
    res = elasticsearch.search(index=index, size = 500, body={"query": {"match_all": {}}})
    def getSource(result): return result['_source']
    resSources = list(map(getSource, res['hits']['hits']))
    print("returning " + str(len(resSources)) + " tweets.")
    return(jsonify(resSources))

if __name__ == '__main__':
    application.debug = True
    application.run()
