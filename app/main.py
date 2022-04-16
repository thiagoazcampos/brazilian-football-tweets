import streamlit as st
from pymongo import MongoClient
from os import getenv
import requests
from datetime import datetime, timedelta
from config.teams import teams_pretty_mapping, teams_tag_mapping
from pandas import DataFrame

# TODO: Put streamlit inside the docker-compose
# TODO: Organize the code so main would be as simple as possible (use external files for config, pipeline queries, etc)

MONGO_SERVER = getenv("MONGO_SERVER")
MONGO_DATABASE = getenv("MONGO_DATABASE")
MONGO_COLLECTION = getenv("MONGO_COLLECTION")
TWITTER_OEMBED_URL = 'https://publish.twitter.com/oembed'
TWEET_URL = "https://twitter.com/user/status/{tweet_id}"
DATE_STR_FORMAT = "%Y-%m-%d"

st.set_page_config(layout='wide')

session = requests.Session()
params = {
    "url": '',
    "hide_media": "true",
    "hide_thread": "true"
}

mongo_client = MongoClient(host=[MONGO_SERVER])
mongo_db = mongo_client.get_database(MONGO_DATABASE)
mongo_collection = mongo_db.get_collection(MONGO_COLLECTION)

st.title("Brazilian Football Tweets")
st.markdown("""
    Selection of tweets related to Brazilian Football teams.
""")

col_params, col_tweets, col_statistics = st.columns([1, 3, 2])

with col_params:
    st.header("Parameters")
    tweets_limit = st.slider("Show tweets", min_value=0, max_value=20, value=5)

    start_date = st.date_input("Start Date", value=datetime.today())
    end_date = st.date_input("End Date", value=datetime.today()+timedelta(days=1))
    team = st.radio("Filter by team", options=['All'] + list(teams_pretty_mapping.keys()))

mongo_query = {
    "created_at": {
        "$gte": start_date.strftime(DATE_STR_FORMAT),
        "$lte": end_date.strftime(DATE_STR_FORMAT)
    }
}
if team != 'All':
    mongo_query.update({'team': teams_pretty_mapping[team]})


with col_tweets:
    st.header("Tweets")

    tweets = mongo_collection.find(mongo_query).sort([('created_at', -1)]).limit(tweets_limit)
    for tweet in tweets:
        params.update({"url": TWEET_URL.format(tweet_id=tweet['tweet_id'])})
        response = session.get(TWITTER_OEMBED_URL, params=params)

        try:
            html = response.json()['html']
            st.write(html, unsafe_allow_html=True)
        except:
            pass

with col_statistics:
    st.header("Statistics")

    # TODO: Barplot: most used words
    st.subheader("Most used words")
    
    
    st.subheader("Most used contexts")
    contexts_pipeline = [{"$match": mongo_query}, {"$unwind": "$contexts"}, {"$group": {"_id": "$contexts", "Tweets": {"$sum": 1}}}, {"$sort": {"Tweets": -1}}]
    result = mongo_collection.aggregate(contexts_pipeline)
    df = DataFrame(result)
    df.set_index('_id', inplace=True)
    st.bar_chart(df)

    st.subheader("Most tweeted teams")
    count_pipeline = [{"$match": mongo_query}, {"$group": {"_id": "$team", "Tweets": {"$sum": 1}}}, {"$sort": {"Tweets": -1}}]
    result = mongo_collection.aggregate(count_pipeline)
    df = DataFrame(result)
    df['_id'] = df['_id'].map(teams_tag_mapping)
    df.set_index('_id', inplace=True)
    st.bar_chart(df)