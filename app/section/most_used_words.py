import streamlit as st
from pandas import DataFrame

def most_used_words(mongo_collection, mongo_filter_document):
    st.subheader("Most used words")
    words_pipeline = [
        {"$match": mongo_filter_document},
        {"$unwind": "$words_count"},
        {"$group" : {"_id": "$words_count.word", "count": {"$sum": {"$toInt": "$words_count.count"}}}},
        {"$sort": {"count": -1}},
        {"$limit": 50}
    ]
    result = mongo_collection.aggregate(words_pipeline)
    df = DataFrame(result)
    if not df.empty:
        df.set_index('_id', inplace=True)
        st.bar_chart(df)