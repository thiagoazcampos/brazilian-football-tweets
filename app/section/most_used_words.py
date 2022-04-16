import streamlit as st
from pandas import DataFrame
import plotly.express as px

def most_used_words(mongo_collection, mongo_filter_document):
    st.subheader("Most used words")
    words_pipeline = [
        {"$match": mongo_filter_document},
        {"$unwind": "$words_count"},
        {"$group" : {"_id": "$words_count.word", "Count": {"$sum": {"$toInt": "$words_count.count"}}}},
        {"$sort": {"Count": -1}},
        {"$limit": 50}
    ]
    result = mongo_collection.aggregate(words_pipeline)
    df = DataFrame(result)
    if not df.empty:
        df.rename(columns={'_id': 'Word'}, inplace=True)
        df.set_index('Word', inplace=True)
        chart = px.bar(df)
        chart.update_layout(
            height=250
        )
        st.plotly_chart(chart, use_container_width=True)