from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
import logging
from os import getenv

KAFKA_TOPIC = getenv('KAFKA_TOPIC')
KAFKA_SERVER = getenv("KAFKA_SERVER")
MONGO_SERVER = getenv("MONGO_SERVER")
MONGO_DATABASE = getenv("MONGO_DATABASE")
MONGO_COLLECTION = getenv("MONGO_COLLECTION")

# TODO: Create topic if not exists
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

mongo_client = MongoClient(host=[MONGO_SERVER])
mongo_db = mongo_client.get_database(MONGO_DATABASE)
mongo_collection = mongo_db.get_collection(MONGO_COLLECTION)

for message in consumer:
    mongo_collection.insert_one(message.value)
    logging.info("Document sent")