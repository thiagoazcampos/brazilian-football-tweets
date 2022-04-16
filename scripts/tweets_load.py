from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
import logging
from os import getenv

logging.basicConfig(
    level="INFO",
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

KAFKA_TOPIC = getenv('KAFKA_TOPIC')
KAFKA_SERVER = getenv("KAFKA_SERVER")
MONGO_SERVER = getenv("MONGO_SERVER")
MONGO_DATABASE = getenv("MONGO_DATABASE")
MONGO_COLLECTION = getenv("MONGO_COLLECTION")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

mongo_client = MongoClient(host=[MONGO_SERVER])
mongo_collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]

for message in consumer:
    mongo_collection.insert_one(message.value)
    logging.info("Document sent")