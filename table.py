from altair import Order
import streamlit as st
import numpy as np
import pandas as pd
import sqlalchemy
import config
import yaml
import utils

st.set_page_config(layout="wide", initial_sidebar_state="auto")
with open('config.yaml', 'r') as file:
    database_config = yaml.safe_load(file)

connection = st.connection('source', type='sql')
connector = utils.Connector(connection)

left_column, right_column = st.columns(spec=[1,3], gap="small", vertical_alignment="top")
# avoid overall scrolling
left_column = left_column.container(height=config.page_height, border=config.show_all_edge)
right_column = right_column.container(height=config.page_height, border=config.show_all_edge)


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
table_list_checkbox=[]
for name in database_config['table_name']:
    is_default = False
    if name == config.default_table:
        is_default = True
    tmp = table_list.checkbox(label=name, value = is_default)
    table_list_checkbox.append(tmp)

# filters
filter_area = left_column.form("filters", border=True, enter_to_submit=False, clear_on_submit=False)
filter_area.write("Filters")
filter_area_buttom = filter_area.form_submit_button(label="Submit")
# Store expanders/containers for the page
filters = {
        "geolocation": None,
        "sellers": None,
        "customers": None,
        "orders": None,
        "order_payments": None,
        "products": None,
        "order_items": None,
        "order_reviews": None
        }
# Store input widgets results
filters_geolocation = {
        "geolocation_zip_code_prefix": None,
        "geolocation_lat": None,
        "geolocation_lng": None,
        "geolocation_city": None,
        "geolocation_state": None
        }
filters_sellers = {
        "seller_id": None,
        "seller_zip_code_prefix": None,
        "seller_city": None,
        "seller_state": None
        }
for name, status in zip(database_config['table_name'], table_list_checkbox):
    if name == 'geolocation':
        geolocation_zip_code_prefix_list = connector.get_single_unique(name, "geolocation_zip_code_prefix").astype(int).sort_values().astype(str).tolist()

        geolocation_latitude_min, geolocation_latitude_max =connector.get_single_min_max(name, "geolocation_lat")
        geolocation_latitude_min = float(geolocation_latitude_min.iloc[0])
        geolocation_latitude_max = float(geolocation_latitude_max.iloc[0])
        geolocation_latitude_list = np.arange(geolocation_latitude_min, geolocation_latitude_max+config.accuracy, config.accuracy).tolist()
        geolocation_latitude_list = list(map(lambda x: f"{x:.4f}", geolocation_latitude_list))
        geolocation_latitude_min = f"{geolocation_latitude_min:.4f}"
        geolocation_latitude_max= f"{geolocation_latitude_max:.4f}"

        geolocation_longtitude_min, geolocation_longtitude_max =connector.get_single_min_max(name, "geolocation_lng")
        geolocation_longtitude_min = float(geolocation_longtitude_min.iloc[0])
        geolocation_longtitude_max = float(geolocation_longtitude_max.iloc[0])
        geolocation_longtitude_list = np.arange(geolocation_longtitude_min, geolocation_longtitude_max+config.accuracy, config.accuracy).tolist()
        geolocation_longtitude_list = list(map(lambda x: f"{x:.4f}", geolocation_longtitude_list))
        geolocation_longtitude_min = f"{geolocation_longtitude_min:.4f}"
        geolocation_longtitude_max = f"{geolocation_longtitude_max:.4f}"

        geolocation_city_list = connector.get_single_unique(name, 'geolocation_city').astype(str).tolist()

        geolocation_state_list = connector.get_single_unique(name, 'geolocation_state').astype(str).tolist()

        filters[name] = filter_area.expander(label = name)



        filters_geolocation['geolocation_zip_code_prefix'] = filters[name].selectbox(label="zip_code", options = geolocation_zip_code_prefix_list, index=None)
        filters_geolocation['geolocation_lat'] = filters[name].select_slider(label="Latitude",
                                                                             options=geolocation_latitude_list,
                                                                             value=(geolocation_latitude_min, geolocation_latitude_max))
        filters_geolocation['geolocation_lng'] = filters[name].select_slider(label="Longtitude",
                                                                             options = geolocation_longtitude_list,
                                                                             value=(geolocation_longtitude_min, geolocation_longtitude_max))
        filters_geolocation['geolocation_city'] = filters[name].selectbox(label="City", options = geolocation_city_list, index=None)
        filters_geolocation['geolocation_state'] = filters[name].selectbox(label="State", options = geolocation_state_list, index=None)
    elif name == 'sellers':
        filters[name] = filter_area.expander(label = name)
    elif name == 'customers':
        filters[name] = filter_area.expander(label = name)
    elif name == 'orders':
        filters[name] = filter_area.expander(label = name)
    elif name == 'order_payments':
        filters[name] = filter_area.expander(label = name)
    elif name == 'products':
        filters[name] = filter_area.expander(label = name)
    elif name == 'order_items':
        filters[name] = filter_area.expander(label = name)
    elif name == 'order_reviews':
        filters[name] = filter_area.expander(label = name)








































# # Query to get table names
# table_names_query = """
# SELECT table_name
# FROM information_schema.tables
# WHERE table_schema = 'source';
# """
# 
# # Sample Query to get table columns
# table_colmns_query = """
# SELECT column_name
# FROM INFORMATION_SCHEMA.COLUMNS
# WHERE TABLE_SCHEMA = 'source'
# AND TABLE_NAME = 'geolocation';
# """
# 
# 
# connection = st.connection('source', type='sql')
# 
# with right_column:
#     st.write("Table")
#     st.dataframe(
#             pd.DataFrame(
#                 np.random.randn(50,30), columns=("col %d" % i for i in range(30))
#                 ),
#             # use_container_width=True,
#             width=1400,
#             height=930
#             )
# 
# # Execute the query
# table_names_df = connection.query(table_names_query)
# 
# # Convert to a list of table names
# table_names = table_names_df['table_name'].tolist()
# 
# table_keys_df = connection.query(table_colmns_query)
# right_column.write(table_keys_df)




# table_list = table_and_change.container(border=True)
# table_list_checkbox = []
# for name in table_names:
#     the_first = False
#     if name == table_names[0]:
#         the_first=True
#     tmp = table_list.checkbox(label=name, value = the_first)
#     table_list_checkbox.append(tmp)
# 
# # for name, state in zip(table_names, table_list_checkbox):
# #     if state:
# #         st.write(name, "is selected")
        








