import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(page_title="Real-Time Dashboard", layout="wide")
st.title("Real-Time Transaction Dashboard")

path = "data/serving/stream/city_agg"

if "last_df" not in st.session_state:
    st.session_state.last_df = None

placeholder = st.empty()

while True:
    try:
        if os.path.exists(path):
            df = pd.read_parquet(path)

            if not df.empty:
                st.session_state.last_df = df

        with placeholder.container():
            if st.session_state.last_df is not None:
                df = st.session_state.last_df

                st.dataframe(df)

                st.subheader("Revenue by City")
                st.bar_chart(df.set_index("city")["total_revenue"])

                st.subheader("Transactions by City")
                st.bar_chart(df.set_index("city")["total_transactions"])
            else:
                st.warning("No data yet...")

    except Exception as e:
        with placeholder.container():
            if st.session_state.last_df is not None:
                df = st.session_state.last_df

                st.dataframe(df)

                st.subheader("Revenue by City")
                st.bar_chart(df.set_index("city")["total_revenue"])

                st.subheader("Transactions by City")
                st.bar_chart(df.set_index("city")["total_transactions"])
            else:
                st.warning(f"Waiting for stream data... {e}")

    time.sleep(5)
    st.rerun()