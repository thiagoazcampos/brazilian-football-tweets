import streamlit as st
from pandas import DataFrame
from config.teams import teams_tag_mapping
import plotly.express as px

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
        df.rename(columns={"_id": "Team"}, inplace=True)
        df['Team'] = df['Team'].map(teams_tag_mapping)
        df.set_index('Team', inplace=True)
        chart = px.bar(df)
        chart.update_layout(
            height=250
        )
        st.plotly_chart(chart, use_container_width=True)