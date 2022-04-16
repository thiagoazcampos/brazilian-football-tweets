import streamlit as st

def tweets_count(mongo_collection, mongo_filter_document):
    tweets_count = mongo_collection.count_documents(mongo_filter_document)
    st.caption(f"Tweets count: {tweets_count}")