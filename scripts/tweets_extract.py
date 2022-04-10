import requests
from os import getenv
from time import sleep
from pandas import Timestamp
from kafka import KafkaProducer
from json import dumps
import logging

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BASE_URL = "https://api.twitter.com/"
ROUTE = "2/tweets/search/recent"

TOKEN = getenv("TWITTER_BEARER_TOKEN")
KAFKA_SERVER = getenv('KAFKA_SERVER')
KAFKA_TOPIC = getenv('KAFKA_TOPIC')
REMOVE_RETWEETS = getenv('REMOVE_RETWEETS', 'True').lower() == 'true'
MAX_RESULTS = int(getenv('MAX_RESULTS', '10'))
TWEET_LANG = getenv('TWEET_LANG', 'pt')
SLEEP_TIME = int(getenv('SLEEP_TIME', '60'))

# TODO: Remove this list from the script
teams = [
    'fortaleza',
    'ceara',
    '"america mg"',
    '"america mineiro"',
    'athletico',
    '"atletico go"',
    '"atletico goianiense"',
    '"atletico mg"',
    '"atletico mineiro"',
    'avai',
    'botafogo',
    'bragantino',
    'corinthians',
    'coritiba',
    'cuiaba',
    'flamengo',
    'fluminense',
    'goias',
    'internacional',
    'juventude',
    'palmeiras',
    'santos',
    '"sao paulo"'
]

query = f'({" OR ".join(teams)})'

if TWEET_LANG:
    query += f" lang:{TWEET_LANG}"

if REMOVE_RETWEETS:
    query += " -is:retweet"

logging.info(f"Query length: {len(query)}")

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}
params = {
    'start_time': Timestamp.now(tz='UTC').strftime(DATE_FORMAT),
    'query': query,
    'max_results': MAX_RESULTS,
    'tweet.fields': 'created_at',
    'user.fields': 'username'
}

# TODO: Create topic if not exists
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

session = requests.Session()

while True:

    sleep(SLEEP_TIME)

    with session.get(
        f"{BASE_URL}{ROUTE}",
        headers=headers,
        params=params
    ) as response:

        if response.status_code == 200:
            payload = response.json()

            if payload['meta']['result_count'] > 0:
                tweets = payload['data']
                logging.info(f"Tweets sent: {payload['meta']['result_count']}")

                for tweet in tweets:
                    producer.send(KAFKA_TOPIC, tweet)

                newest_id = payload['meta']['newest_id']
                params.update({
                    "since_id": newest_id
                })
                params.pop('start_time', None)

        else:
            logging.info(f"Request presented code: {response.status_code}")

