from requests import Session
import streamlit as st

def recent_tweets(mongo_collection, mongo_filter_document, tweets_limit, tweet_url, twitter_oembed_url):
    session = Session()
    params = {
        "url": '',
        "hide_media": "true",
        "hide_thread": "true"
    }

    tweets = mongo_collection.find(mongo_filter_document).sort([('created_at', -1)]).limit(tweets_limit)
    for tweet in tweets:
        params.update({"url": tweet_url.format(tweet_id=tweet['tweet_id'])})
        response = session.get(twitter_oembed_url, params=params)
        try:
            html = response.json()['html']
            st.write(html, unsafe_allow_html=True)
        except:
            pass