import streamlit as st
from pandas import DataFrame
import plotly.express as px

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
        df.rename(columns={"_id": "Context"}, inplace=True)
        df.set_index('Context', inplace=True)
        chart = px.bar(df)
        chart.update_layout(
            height=250
        )
        st.plotly_chart(chart, use_container_width=True)