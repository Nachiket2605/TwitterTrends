import secretsAndSettings as sas
from time import sleep
import json
import boto.sqs
import boto.sns
from boto.sqs.message import Message
import ast
from alchemyapi import AlchemyAPI
from elasticsearch import Elasticsearch, RequestsHttpConnection
#from requests_aws4auth import AWS4Auth
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


def sentimentanalyze(m):
    error = False
    body = m.get_body()
    tweet= ast.literal_eval(body)

    print(tweet['text'])
    response = alchemy.sentiment("text", tweet['text'])
    if(response['status']=='ERROR'):
        print('ERROR')
        error = True
    if not error:
        tweet['sentiment'] = response["docSentiment"]["type"]
        print("Sentiment: "+ tweet['sentiment'])

        #add to Elasticsearch
        # try:
        #     self.es.index(index="tweets", doc_type="twitter_twp", body=tweet)
        # except Exception as e:
        #     print('Elasticserch indexing failed')
        #     print(e)


        json_string = json.dumps(tweet)
        #send processed tweet to SNS
        sns.publish(sas.arn['arn'], json_string, subject='TwitterStream')

        #delete notification when done
        q.delete_message(m)
        print('Done')


while (True):
    newtweet = q.get_messages()
    print (len(newtweet))
    if len(newtweet) > 0:
        for m in newtweet:
	           thread_pool.submit(sentimentanalyze, m)
