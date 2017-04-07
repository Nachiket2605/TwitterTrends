import secretsAndSettings as sas
from time import sleep
import json
import boto.sqs
import boto.sns
from boto.sqs.message import Message
import ast
from alchemyapi import AlchemyAPI
from elasticsearch import Elasticsearch, RequestsHttpConnection
import sys
from concurrent.futures import ThreadPoolExecutor



conf = {"sqs-access-key": sas.awsKeys['access-key'],
        "sqs-secret-key": sas.awsKeys['secret-key'],
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


alchemy = AlchemyAPI()
sns = boto.sns.connect_to_region("us-east-1")


thread_pool = ThreadPoolExecutor(max_workers=4)



geoTweetIndexName = "geo-tweets"


#elasticsearch = Elasticsearch({'host': 'search-es-twitter-yarekxa5djp3rkj7kp735gvacy.us-west-2.es.amazonaws.com', 'port': 443})
elasticsearch = Elasticsearch(sas.elasticSearch['uri'],
                              port=sas.elasticSearch['port'],
                              use_ssl=sas.elasticSearch['use_ssl'])


# if  elasticsearch.indices.exists(geoTweetIndexName):
#     elasticsearch.indices.delete(index=geoTweetIndexName)
# elasticsearch.indices.create(index=geoTweetIndexName,
#                                     ignore=400,
#                                     body=gts.geoTweetsSettings)

def sentimentanalyze(m):
    error = False
    body = m.get_body()
    tweet= ast.literal_eval(body)

    #print(tweet['text'])
    response = alchemy.sentiment("text", tweet['text'])
    print (response)
    if(response['status']=='ERROR'):
        print('ERROR')
        #error = True
    if not error:
        tweet['sentiment'] = response["docSentiment"]["type"]
        #print("Sentiment: "+ tweet['sentiment'])
        print ('--------------------------------------')
        #print (tweet)
        index = "geo-tweets"
        try:
            elasticsearch.index(index="geo-tweets", doc_type="tweet", body=tweet)
            #print (elasticsearch.index(index="geo-tweets", doc_type="tweet", body=tweet))
        except Exception as e:
            print ("Could not index this shit")
            pass



        json_string = json.dumps(tweet)

        sns.publish(sas.arn['arn'], json_string, subject='TwitterStream')

        #delete notification when done

        print('Done')


while (True):
    newtweet = q.get_messages()
    #print (len(newtweet))
    if len(newtweet) > 0:
        for m in newtweet:
	           thread_pool.submit(sentimentanalyze, m)
