import streamlit as st


pages = [
        st.Page("table.py", default=True, title="table"),
        st.Page("display.py", default=False, title="display")
        ]

pg = st.navigation(pages)
pg.run()
