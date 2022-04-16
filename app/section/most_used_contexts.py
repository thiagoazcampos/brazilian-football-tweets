import streamlit as st
from pandas import DataFrame

def most_used_contexts(mongo_collection, mongo_filter_document):
    st.subheader("Most used contexts")
    contexts_pipeline = [
        {"$match": mongo_filter_document},
        {"$unwind": "$contexts"},
        {"$group": {"_id": "$contexts", "Tweets": {"$sum": 1}}},
        {"$sort": {"Tweets": -1}}
    ]
    result = mongo_collection.aggregate(contexts_pipeline)
    df = DataFrame(result)
    if not df.empty:
        df.set_index('_id', inplace=True)
        st.bar_chart(df)