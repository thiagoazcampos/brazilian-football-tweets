import streamlit as st
from datetime import datetime, timedelta

def filters(teams_pretty_mapping):
    tweets_limit = st.slider("Show most recent tweets", min_value=0, max_value=20, value=5)
    start_date = st.date_input("Start Date", value=datetime.today())
    end_date = st.date_input("End Date", value=datetime.today()+timedelta(days=1))
    team = st.radio("Filter by team", options=['All'] + list(teams_pretty_mapping.keys()))

    return tweets_limit, start_date, end_date, team