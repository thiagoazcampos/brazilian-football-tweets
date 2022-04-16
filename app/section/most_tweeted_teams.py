import streamlit as st
from pandas import DataFrame
from config.teams import teams_tag_mapping


def most_tweeted_teams(mongo_collection, mongo_filter_document):
    st.subheader("Most tweeted teams")
    count_pipeline = [
        {"$match": mongo_filter_document},
        {"$group": {"_id": "$team", "Tweets": {"$sum": 1}}},
        {"$sort": {"Tweets": -1}}
    ]
    result = mongo_collection.aggregate(count_pipeline)
    df = DataFrame(result)
    if not df.empty:
        df['_id'] = df['_id'].map(teams_tag_mapping)
        df.set_index('_id', inplace=True)
        st.bar_chart(df)