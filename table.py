import streamlit as st
import numpy as np
import pandas as pd
import sqlalchemy
import config
import yaml
from streamlit.elements.image import UseColumnWith

st.set_page_config(layout="wide", initial_sidebar_state="auto")


left_column, right_column = st.columns(spec=[1,3], gap="small", vertical_alignment="top")
# avoid overall scrolling
left_column = left_column.container(height=config.page_height, border=config.show_all_edge)
right_column = right_column.container(height=config.page_height, border=config.show_all_edge)


# Query to get table names
table_names_query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'source';
"""

# Sample Query to get table columns
table_colmns_query = """
SELECT column_name
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'source'
AND TABLE_NAME = 'geolocation';
"""


connection = st.connection('source', type='sql')

with right_column:
    st.write("Table")
    st.dataframe(
            pd.DataFrame(
                np.random.randn(50,30), columns=("col %d" % i for i in range(30))
                ),
            # use_container_width=True,
            width=1400,
            height=930
            )

# Execute the query
table_names_df = connection.query(table_names_query)

# Convert to a list of table names
table_names = table_names_df['table_name'].tolist()

table_keys_df = connection.query(table_colmns_query)
right_column.write(table_keys_df)


# confirm or rollback
sql_change = left_column.container(border=True)
sql_change_left, sql_change_right = sql_change.columns(([1,1]))

change_confirm = sql_change_left.button(label="Confirm", use_container_width=True)
change_rollback = sql_change_right.button(label="Rollback", use_container_width=True)

# table list and data change
table_and_change = left_column.container(border=True)

table_change = table_and_change.container(border=True)
area_add, area_delete, area_update = table_change.columns(3)
buttom_table_add = area_add.button(label="Add", use_container_width=True)
buttom_table_delete = area_delete.button(label="Delete", use_container_width=True)
buttom_table_update = area_update.button(label="Update", use_container_width=True)

table_list = table_and_change.container(border=True)
table_list_checkbox = []
for name in table_names:
    the_first = False
    if name == table_names[0]:
        the_first=True
    tmp = table_list.checkbox(label=name, value = the_first)
    table_list_checkbox.append(tmp)

# for name, state in zip(table_names, table_list_checkbox):
#     if state:
#         st.write(name, "is selected")
        





















































