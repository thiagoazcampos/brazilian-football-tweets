import streamlit as st
from pymongo import MongoClient
from os import getenv
import section
import utils

MONGO_SERVER = getenv("MONGO_SERVER")
MONGO_DATABASE = getenv("MONGO_DATABASE")
MONGO_COLLECTION = getenv("MONGO_COLLECTION")
TWITTER_OEMBED_URL = 'https://publish.twitter.com/oembed'
TWEET_URL = "https://twitter.com/user/status/{tweet_id}"

st.set_page_config(layout='wide')

mongo_client = MongoClient(host=[MONGO_SERVER])
mongo_collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]

st.title("Brazilian Football Tweets")
st.markdown("""
    Selection of tweets related to Brazilian Football teams.
""")

col_filters, col_tweets, col_statistics = st.columns([1, 3, 2])

with col_filters:
    st.header("Filters")
    tweets_limit, start_date, end_date, team = section.filters()

mongo_filter_document = utils.build_mongo_filter_document(
    start_date, end_date, team
)

with col_tweets:
    st.header("Most recent tweets")
    section.recent_tweets(
        mongo_collection,
        mongo_filter_document,
        tweets_limit,
        TWEET_URL,
        TWITTER_OEMBED_URL
    )
    

with col_statistics:
    st.header("Statistics")
    section.tweets_count(mongo_collection, mongo_filter_document)
    section.most_used_words(mongo_collection, mongo_filter_document)
    section.most_used_contexts(mongo_collection, mongo_filter_document)
    if team == "All":
        section.most_tweeted_teams(mongo_collection, mongo_filter_document)
