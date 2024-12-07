import streamlit as st
import yaml

# st.set_page_config(page_title="demo", initial_sidebar_state="expanded")
st.set_page_config(layout="wide", initial_sidebar_state="auto")
if "query" not in st.session_state:
    st.session_state.query = ""






