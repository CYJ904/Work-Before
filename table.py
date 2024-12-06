from contextlib import ContextDecorator
from os import sep, stat
from altair import Order
from pandas.core.methods.describe import select_describe_func
import streamlit as st
import numpy as np
import pandas as pd
import sqlalchemy
from toolz import count
import config
import yaml
import utils
from datetime import datetime, timedelta
import itertools
import string

#   - customer_city
#   - customer_id
#   - customer_state
#   - customer_unique_id
#   - customer_zip_code_prefix
#   - freight_value
#   - geolocation_city
#   - geolocation_lat
#   - geolocation_lng
#   - geolocation_state
#   - geolocation_zip_code_prefix
#   - order_approved_at
#   - order_delivered_carrier_date
#   - order_delivered_customer_date
#   - order_estimated_delivery_date
#   - order_id
#   - order_item_id
#   - order_purchase_timestamp
#   - order_status
#   - payment_installments
#   - payment_sequential
#   - payment_type
#   - payment_value
#   - price
#   - product_category_name
#   - product_description_length
#   - product_height_cm
#   - product_id
#   - product_length_cm
#   - product_name_length
#   - product_photos_qty
#   - product_weight_g
#   - product_width_cm
#   - review_creation_date
#   - review_id
#   - review_score
#   - seller_city
#   - seller_id
#   - seller_state
#   - seller_zip_code_prefix
#   - shipping_limit_date
# customers:
# geolocation:
# order_items:
# order_payments:
# order_reviews:
# orders:
# products:
# sellers:

st.set_page_config(layout="wide", initial_sidebar_state="auto")
with open('config.yaml', 'r') as file:
    database_config = yaml.safe_load(file)


if "connector" not in st.session_state:
    connection = st.connection('source', type='sql')
    st.session_state.connector = utils.Connector(connection)
    st.session_state.connector.start_transaction()

if "query" not in st.session_state:
    st.session_state.query = ""

if "result" not in st.session_state:
    st.session_state.result = ""

if "stack" not in st.session_state:
    st.session_state.stack = []

if "data_changed" not in st.session_state:
    st.session_state.data_changed = False


if "map" not in st.session_state:
    with open('config.yaml', 'r') as file:
        st.session_state.map = yaml.safe_load(file)

database_config = st.session_state.map

connector = st.session_state.connector

left_column, right_column = st.columns(spec=config.page_split, gap="small", vertical_alignment="top")
# avoid overall scrolling
left_column = left_column.container(height=config.page_height, border=config.show_all_edge)
right_column = right_column.container(height=config.page_height, border=config.show_all_edge)
right_column.write("Table")

# right_column.write(st.session_state.query)


# confirm or rollback
sql_change = left_column.container(border=True)
sql_change_left, sql_change_right = sql_change.columns(([1,1]))

change_confirm = sql_change_left.button(label="Confirm", use_container_width=True)
cannot_rollback = False if len(st.session_state.stack) != 0 else True
change_rollback = sql_change_right.button(label="Rollback", use_container_width=True, disabled=cannot_rollback)

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
# if no selected table, no submission
has_selected_table = False
for status in table_list_checkbox:
    if status:
        has_selected_table = True
filter_area_buttom = filter_area.form_submit_button(label="Submit", disabled=not(has_selected_table))
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
filters_geolocation = None
filters_sellers = None
filters_customers = None
filters_orders = None
filters_order_payments = None
filters_products = None
filters_order_items = None
filters_order_reviews = None
if has_selected_table:
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
    filters_customers = { 
                         "customer_id": None,
                         "customer_unique_id": None,
                         "customer_zip_code_prefix": None,
                         "customer_city": None,
                         "customer_state": None}
    filters_orders = {
            "order_id": None,
            "customer_id": None,
            "order_status": None,
            "order_purchase_timestamp": None,
            "order_approved_at": None,
            "order_delivered_carrier_date": None,
            "order_delivered_customer_date": None,
            "order_estimated_delivery_date": None
            }
    filters_order_payments = {
            "order_id": None,
            "payment_sequential": None,
            "payment_type": None,
            "payment_installments": None,
            "payment_value": None
            }
    filters_products = {
            "product_id": None,
            "product_category_name": None,
            "product_name_length": None,
            "product_description_length": None,
            "product_photos_qty": None,
            "product_weight_g": None,
            "product_length_cm": None,
            "product_height_cm": None,
            "product_width_cm": None
            }
    filters_order_items = {
            "order_id": None,
            "order_item_id": None,
            "product_id": None,
            "seller_id": None,
            "shipping_limit_date": None,
            "price": None,
            "freight_value": None
            }
    filters_order_reviews = {
            "review_id": None,
            "order_id": None,
            "review_score": None,
            "review_creation_date": None
            }

    for name, status in zip(database_config['table_name'], table_list_checkbox):
        if name == 'geolocation' and status:
            geolocation_zip_code_prefix_list = connector.get_single_unique(name, "geolocation_zip_code_prefix").astype(int).sort_values().astype(str).tolist()

            geolocation_latitude_min, geolocation_latitude_max = connector.get_single_min_max(name, "geolocation_lat")
            geolocation_latitude_min = float(geolocation_latitude_min.iloc[0])
            geolocation_latitude_max = float(geolocation_latitude_max.iloc[0])

            geolocation_longtitude_min, geolocation_longtitude_max =connector.get_single_min_max(name, "geolocation_lng")
            geolocation_longtitude_min = float(geolocation_longtitude_min.iloc[0])
            geolocation_longtitude_max = float(geolocation_longtitude_max.iloc[0])

            geolocation_city_list = connector.get_single_unique(name, 'geolocation_city').astype(str).tolist()

            geolocation_state_list = connector.get_single_unique(name, 'geolocation_state').astype(str).tolist()


            filters[name] = filter_area.expander(label = name)


            filters_geolocation['geolocation_zip_code_prefix'] = filters[name].selectbox(label="zip_code", options = geolocation_zip_code_prefix_list, index=None)
            filters_geolocation['geolocation_lat'] = filters[name].slider(label="Latitude", min_value=geolocation_latitude_min, max_value=geolocation_latitude_max, value=(geolocation_latitude_min, geolocation_latitude_max), step=config.accuracy)
            filters_geolocation['geolocation_lng'] = filters[name].slider(label="Longtitude", min_value=geolocation_longtitude_min, max_value=geolocation_longtitude_max, value=(geolocation_longtitude_min, geolocation_longtitude_max), step=config.accuracy)
            filters_geolocation['geolocation_city'] = filters[name].selectbox(label="City", options = geolocation_city_list, index=None)
            filters_geolocation['geolocation_state'] = filters[name].selectbox(label="State", options = geolocation_state_list, index=None)
        elif name == 'sellers' and status:
            seller_id_list = connector.get_single_unique(name, "seller_id").astype(str).tolist()

            seller_zip_code_prefix_list = connector.get_single_unique(name, "seller_zip_code_prefix").astype(str).tolist()

            seller_city_list = connector.get_single_unique(name, 'seller_city').astype(str).tolist()

            seller_state_list = connector.get_single_unique(name, 'seller_state').astype(str).tolist()

            filters[name] = filter_area.expander(label = name)

            filters_sellers['seller_id'] = filters[name].selectbox(label="ID", options=seller_id_list, index = None)
            filters_sellers['seller_zip_code_prefix'] = filters[name].selectbox(label="zip_code", options=seller_zip_code_prefix_list, index = None)
            filters_sellers['seller_city'] = filters[name].selectbox(label="City", options=seller_city_list, index = None)
            filters_sellers['seller_state'] = filters[name].selectbox(label="State", options=seller_state_list, index = None)
        elif name == 'customers' and status:
            customer_id_list = connector.get_single_unique(name, "customer_id").astype(str).tolist()

            customer_unique_id_list = connector.get_single_unique(name, "customer_unique_id").astype(str).tolist()

            customer_zip_code_prefix_list = connector.get_single_unique(name, "customer_zip_code_prefix").astype(str).tolist()

            customer_city_list = connector.get_single_unique(name, "customer_city").astype(str).tolist()

            customer_state_list = connector.get_single_unique(name, "customer_state").astype(str).tolist()


            filters[name] = filter_area.expander(label = name)


            filters_customers["customer_id"] = filters[name].selectbox(label="ID", options=customer_id_list, index = None)
            filters_customers["customer_unique_id"] = filters[name].selectbox(label="Unique_ID", options=customer_unique_id_list, index = None)
            filters_customers["customer_zip_code_prefix"] = filters[name].selectbox(label="zip_code", options=customer_zip_code_prefix_list, index = None)
            filters_customers["customer_city"] = filters[name].selectbox(label="City", options=customer_city_list, index = None)
            filters_customers["customer_state"] = filters[name].selectbox(label="State", options=customer_state_list, index = None)
        elif name == 'orders' and status:
            order_id_list = connector.get_single_unique(name, "order_id").astype(str).tolist()

            customer_id_list = connector.get_single_unique(name, "customer_id").astype(str).tolist()

            order_status_list = connector.get_single_unique(name, 'order_status').astype(str).tolist()

            order_purchase_timestamp_min, order_purchase_timestamp_max = connector.get_single_min_max(name, 'order_purchase_timestamp')
            order_purchase_timestamp_min = pd.to_datetime(order_purchase_timestamp_min.loc[0]).to_pydatetime()
            order_purchase_timestamp_max= pd.to_datetime(order_purchase_timestamp_max.loc[0]).to_pydatetime()

            order_approved_at_min, order_approved_at_max = connector.get_single_min_max(name, 'order_approved_at')
            order_approved_at_min = pd.to_datetime(order_approved_at_min.loc[0]).to_pydatetime()
            order_approved_at_max = pd.to_datetime(order_approved_at_max.loc[0]).to_pydatetime()

            order_delivered_carrier_date_min, order_delivered_carrier_date_max = connector.get_single_min_max(name, "order_delivered_carrier_date")
            order_delivered_carrier_date_min = pd.to_datetime(order_delivered_carrier_date_min.loc[0]).to_pydatetime()
            order_delivered_carrier_date_max = pd.to_datetime(order_delivered_carrier_date_max.loc[0]).to_pydatetime()

            order_delivered_customer_date_min, order_delivered_customer_date_max = connector.get_single_min_max(name, "order_delivered_customer_date")
            order_delivered_customer_date_min = pd.to_datetime(order_delivered_customer_date_min.loc[0]).to_pydatetime()
            order_delivered_customer_date_max = pd.to_datetime(order_delivered_customer_date_max.loc[0]).to_pydatetime()

            order_estimated_delivery_date_min, order_estimated_delivery_date_max = connector.get_single_min_max(name, "order_estimated_delivery_date")
            order_estimated_delivery_date_min = pd.to_datetime(order_estimated_delivery_date_min.loc[0]).to_pydatetime()
            order_estimated_delivery_date_max = pd.to_datetime(order_estimated_delivery_date_max.loc[0]).to_pydatetime()


            filters[name] = filter_area.expander(label = name)

            
            filters_orders['order_id'] = filters[name].selectbox(label="Order ID", options=order_id_list, index=None)

            filters_orders['customer_id'] = filters[name].selectbox(label="Customer ID", options=customer_id_list, index=None)

            filters_orders['order_status'] = filters[name].selectbox(label="Status", options=order_status_list, index=None)

            filters_orders['order_purchase_timestamp'] = filters[name].slider("Purchase time", min_value=order_purchase_timestamp_min, max_value=order_purchase_timestamp_max, value=(order_purchase_timestamp_min, order_purchase_timestamp_max), step=config.time['step']['second'], format=config.time['format']['long'])

            filters_orders['order_approved_at'] = filters[name].slider("Approved time", min_value=order_approved_at_min, max_value=order_approved_at_max, value=(order_approved_at_min, order_approved_at_max), step=config.time['step']['second'], format=config.time['format']['long'])

            filters_orders['order_delivered_carrier_date'] = filters[name].slider("Delivered Carrier Date", min_value=order_delivered_carrier_date_min, max_value=order_delivered_carrier_date_max, value=(order_delivered_carrier_date_min, order_delivered_carrier_date_max), step=config.time['step']['day'], format=config.time['format']['short'])

            filters_orders['order_delivered_customer_date'] = filters[name].slider("Delivered Customer Date", min_value=order_delivered_customer_date_min, max_value=order_delivered_customer_date_max, value=(order_delivered_customer_date_min, order_delivered_customer_date_max), step=config.time['step']['day'], format=config.time['format']['short'])

            filters_orders['order_estimated_delivery_date'] = filters[name].slider("Estimated Delivery Date", min_value=order_estimated_delivery_date_min, max_value=order_estimated_delivery_date_max, value=(order_estimated_delivery_date_min, order_estimated_delivery_date_max), step=config.time['step']['day'], format=config.time['format']['short'])
        elif name == 'order_payments' and status:
            order_id_list = connector.get_single_unique(name, 'order_id').astype(str).tolist()

            payment_sequential_list = connector.get_single_unique(name, 'payment_sequential').astype(str).tolist()

            payment_type_list = connector.get_single_unique(name, 'payment_type').astype(str).tolist()

            payment_installments_list = connector.get_single_unique(name, 'payment_installments').astype(int).sort_values().astype(str).tolist()

            payment_value_min, payment_value_max = connector.get_single_min_max(name, 'payment_value')
            payment_value_min = float(payment_value_min.iloc[0])
            payment_value_max = float(payment_value_max.iloc[0])


            filters[name] = filter_area.expander(label = name)


            filters_order_payments['order_id'] = filters[name].selectbox(label = "Order ID", options = order_id_list, index = None)

            filters_order_payments['payment_sequential'] = filters[name].selectbox(label = "Payment Sequential", options=payment_sequential_list, index=None)

            filters_order_payments['payment_type'] = filters[name].selectbox(label="Payment Type", options=payment_type_list, index = None)

            filters_order_payments['payment_installments'] = filters[name].selectbox(label="Payment Installments", options=payment_installments_list, index = None)

            filters_order_payments['payment_value'] = filters[name].slider("Payment Value", min_value=payment_value_min, max_value=payment_value_max, value=(payment_value_min, payment_value_max), step=config.concurrency)
        elif name == 'products' and status:
            product_id_list = connector.get_single_unique(name, "product_id").astype(str).tolist()

            product_category_name_list = connector.get_single_unique(name, "product_category_name").astype(str).tolist()

            product_name_length_min, product_name_length_max = connector.get_single_min_max(name, "product_name_length")
            product_name_length_min = int(product_name_length_min.loc[0])
            product_name_length_max= int(product_name_length_max.loc[0])

            product_description_length_min, product_description_length_max = connector.get_single_min_max(name, "product_description_length")
            product_description_length_min = int(product_description_length_min.loc[0])
            product_description_length_max = int(product_description_length_max.loc[0])

            product_photos_qty_min, product_photos_qty_max = connector.get_single_min_max(name,"product_photos_qty")
            product_photos_qty_min = int(product_photos_qty_min.loc[0])
            product_photos_qty_max = int(product_photos_qty_max.loc[0])

            product_weight_g_min, product_weight_g_max = connector.get_single_min_max(name,"product_weight_g")
            product_weight_g_min = int(product_weight_g_min.loc[0])
            product_weight_g_max = int(product_weight_g_max.loc[0])

            product_length_cm_min, product_length_cm_max = connector.get_single_min_max(name, "product_length_cm")
            product_length_cm_min = int(product_length_cm_min.loc[0])
            product_length_cm_max = int(product_length_cm_max.loc[0])

            product_height_cm_min, product_height_cm_max = connector.get_single_min_max(name, "product_height_cm")
            product_height_cm_min = int(product_height_cm_min.loc[0])
            product_height_cm_max = int(product_height_cm_max .loc[0])

            product_width_cm_min, product_width_cm_max = connector.get_single_min_max(name, "product_width_cm")
            product_width_cm_min = int(product_width_cm_min.loc[0])
            product_width_cm_max = int(product_width_cm_max.loc[0])


            filters[name] = filter_area.expander(label = name)

            filters_products['product_id'] = filters[name].selectbox(label="Product ID", options = product_id_list, index=None)

            filters_products['product_category_name'] = filters[name].selectbox(label="Product Category", options=product_category_name_list, index = None)

            filters_products['product_name_length'] = filters[name].slider(label="Name Length", min_value=product_name_length_min, max_value=product_name_length_max, value=(product_name_length_min, product_name_length_max), step=config.integer)

            filters_products['product_description_length'] = filters[name].slider(label="Description Length", min_value=product_description_length_min, max_value=product_description_length_max, value=(product_description_length_min, product_description_length_max), step=config.integer)

            filters_products['product_photos_qty'] = filters[name].slider(label="Photo Quantity", min_value=product_photos_qty_min, max_value=product_photos_qty_max, value=(product_photos_qty_min,product_photos_qty_max), step=config.integer)

            filters_products['product_weight_g'] = filters[name].slider(label="Weight in gram", min_value=product_weight_g_min, max_value=product_weight_g_max, value=(product_weight_g_min,product_weight_g_max), step=config.integer)

            filters_products['product_length_cm'] = filters[name].slider(label="Length in cm", min_value=product_length_cm_min, max_value=product_length_cm_max, value=(product_length_cm_min,product_length_cm_max), step=config.integer)

            filters_products['product_height_cm'] = filters[name].slider(label="Height in cm", min_value=product_height_cm_min, max_value=product_height_cm_max, value=(product_height_cm_min,product_height_cm_max), step=config.integer)

            filters_products['product_width_cm'] = filters[name].slider(label="Width in cm", min_value=product_width_cm_min, max_value=product_width_cm_max, value=(product_width_cm_min,product_width_cm_max), step=config.integer)
        elif name == 'order_items' and status:
            order_id_list = connector.get_single_unique(name, "order_id").astype(str).tolist()

            order_item_id_list = connector.get_single_unique(name, "order_item_id").astype(str).tolist()

            product_id_list = connector.get_single_unique(name, "product_id").astype(str).tolist()

            seller_id_list = connector.get_single_unique(name, "seller_id").astype(str).tolist()

            shipping_limit_date_min, shipping_limit_date_max = connector.get_single_min_max(name, "shipping_limit_date")
            shipping_limit_date_min = pd.to_datetime(shipping_limit_date_min.loc[0]).to_pydatetime()
            shipping_limit_date_max = pd.to_datetime(shipping_limit_date_max.loc[0]).to_pydatetime()

            price_min, price_max = connector.get_single_min_max(name, "price")
            price_min = float(price_min.loc[0])
            price_max = float(price_max.loc[0])

            freight_value_min, freight_value_max = connector.get_single_min_max(name, "freight_value")
            freight_value_min = float(freight_value_min.loc[0])
            freight_value_max = float(freight_value_max.loc[0])


            filters[name] = filter_area.expander(label = name)


            filters_order_items['order_id'] = filters[name].selectbox(label="Order ID", options = order_id_list, index=None)

            filters_order_items['order_item_id'] = filters[name].selectbox(label="Order Item ID", options=order_item_id_list, index=None)

            filters_order_items['product_id'] = filters[name].selectbox(label="Product ID", options=product_id_list, index=None, key="order_items-product_id")

            filters_order_items['seller_id'] = filters[name].selectbox(label="Seller ID", options =seller_id_list, index=None)

            filters_order_items['shipping_limit_date'] = filters[name].slider("Shipping Limit Date", min_value=shipping_limit_date_min, max_value=shipping_limit_date_max, value=(shipping_limit_date_min, shipping_limit_date_max), step=config.time['step']['day'], format=config.time['format']['short'])

            filters_order_items['price'] = filters[name].slider("Price", min_value=price_min, max_value=price_max, value=(price_min, price_max), step=config.concurrency)

            filters_order_items['freight_value'] = filters[name].slider("Freight Value", min_value=freight_value_min, max_value=freight_value_max, value=(freight_value_min, freight_value_max), step=config.concurrency)
        elif name == 'order_reviews' and status:
            review_id_list = connector.get_single_unique(name, "review_id").astype(str).tolist()

            order_id_list = connector.get_single_unique(name, "order_id").astype(str).tolist()

            review_score_min, review_score_max = connector.get_single_min_max(name, "review_score")
            review_score_min = int(review_score_min.loc[0])
            review_score_max = int(review_score_max.loc[0])

            review_creation_date_min, review_creation_date_max = connector.get_single_min_max(name, "review_creation_date")
            review_creation_date_min = pd.to_datetime(review_creation_date_min.loc[0]).to_pydatetime()
            review_creation_date_max = pd.to_datetime(review_creation_date_max.loc[0]).to_pydatetime()


            filters[name] = filter_area.expander(label = name)

            
            filters_order_reviews['review_id'] = filters[name].selectbox("Review ID", options=review_id_list, index=None)
            filters_order_reviews['order_id'] = filters[name].selectbox("order_id", options=order_id_list, index=None)
            filters_order_reviews['review_score'] = filters[name].slider("Review Score", min_value=review_score_min, max_value=review_score_max, value = (review_score_min, review_score_max), step=config.integer)
            filters_order_reviews['review_creation_date'] = filters[name].slider("Review Creation Date", min_value=review_creation_date_min, max_value=review_creation_date_max, value=(review_creation_date_min, review_creation_date_max), step=config.time['step']['day'], format=config.time['format']['short'])



used_table = []
used_filters = {}
for name, status in zip(database_config['table_name'], table_list_checkbox):
    if status:
        used_table.append(name)
        if name == "geolocation":
            used_filters[name] = filters_geolocation
        elif name == "sellers":
            used_filters[name] = filters_sellers
        elif name == "customers":
            used_filters[name] = filters_customers
        elif name == "orders":
            used_filters[name] = filters_orders
        elif name == "order_payments":
            used_filters[name] = filters_order_payments
        elif name == "products":
            used_filters[name] = filters_products
        elif name == "order_items":
            used_filters[name] = filters_order_items
        elif name == "order_reviews":
            used_filters[name] = filters_order_reviews

if len(used_table) == 0:
    right_column.write("You haven't select any table yet.")
else:
    query = ""
    # SELECT
    # IF geolocation in used_table, create new table customer_geolocation and sellers_geolocation
    # for replacing the city and state in the customer or sellers with the city and state in geolcoation
    query += "SELECT "
    if 'geolocation' in used_table:
        if len(used_table) == 1:
            query += "geolocation.geolocation_zip_code_prefix AS geolocation_zip_code_prefix, "
            query += "geolocation.geolocation_lat AS geolocation_lat, "
            query += "geolocation.geolocation_city AS geolocation_city, "
            query += "geolocation.geolocation_state AS geolocation_state, "
    if 'customers' in used_table:
        query += "customer.customer_id AS customer_id, "
        query += "customer.customer_unique_id AS customer_unique_id, "
        query += "customer.customer_zip_code_prefix AS customer_zip_code_prefix, "
        if 'geolocation' not in used_table:
            query += "customer.customer_city AS customer_city, "
            query += "customer.customer_state AS customer_state, "
        else:
            query += "customer.customer_city AS customer_city, "
            query += "customer.customer_state AS customer_state, "
            query += "customer.customer_lat AS customer_latitude, "
            query += "customer.customer_lng AS customer_longitude, "
    if 'sellers' in used_table:
        query += "seller.seller_id AS seller_id, "
        query += "seller.seller_zip_code_prefix AS seller_zip_code_prefix, "
        if 'geolocation' not in used_table:
            query += "seller.seller_city AS seller_city, "
            query += "seller.seller_state AS seller_state, "
        else:
            query += "seller.seller_city AS seller_city, "
            query += "seller.seller_state AS seller_state, "
            query += "seller.seller_lat AS seller_latitude, "
            query += "seller.seller_lng AS seller_longitude, "
    if 'orders' in used_table:
        query += "orders.order_id AS order_id, "
        query += "orders.customer_id AS order_customer_id, "
        query += "orders.order_status AS order_status, "
        query += "orders.order_purchase_timestamp AS order_purchase_timestamp, "
        query += "orders.order_approved_at AS order_approved_at, "
        query += "orders.order_delivered_carrier_date AS order_delivered_carrier_date, "
        query += "orders.order_delivered_customer_date AS order_delivered_customer_date, "
        query += "orders.order_estimated_delivery_date AS order_estimated_delivery_date, "
    if "order_payments" in used_table:
        query += "order_payments.order_id AS order_payments_order_id, "
        query += "order_payments.payment_sequential AS payment_sequential, "
        query += "order_payments.payment_type AS payment_type, "
        query += "order_payments.payment_installments AS payment_installments, "
        query += "order_payments.payment_value AS payment_value, "
    if 'products' in used_table:
        query += "products.product_id AS product_id, "
        query += "products.product_category_name AS product_category_name, "
        query += "products.product_name_length AS product_name_length, "
        query += "products.product_description_length AS product_description_length, "
        query += "products.product_photos_qty AS product_photos_qty, "
        query += "products.product_weight_g AS product_weight_g, "
        query += "products.product_length_cm AS product_length_cm, "
        query += "products.product_height_cm AS product_height_cm, "
        query += "products.product_width_cm AS product_width_cm, "
    if 'order_items' in used_table:
        query += "order_items.order_id AS order_items_order_id, "
        query += "order_items.order_item_id AS order_item_id, "
        query += "order_items.product_id AS order_items_product_id, "
        query += "order_items.seller_id AS order_items_seller_id, "
        query += "order_items.shipping_limit_date AS shipping_limit_date, "
        query += "order_items.price AS price, "
        query += "order_items.freight_value AS freight_value, "
    if 'order_reviews' in used_table:
        query += "order_reviews.review_id AS review_id, "
        query += "order_reviews.order_id AS order_reviews_order_id, "
        query += "order_reviews.review_score AS review_score, "
        query += "order_reviews.review_creation_date AS review_creation_date, "

    # clean last comma
    query = query[:-2]
    query += " "


    # FROM
    query += "FROM "
    # Add missed table back for building the bridge
    missed_tables = []
    used_table_full = used_table.copy()
    used_table_pairs = list(itertools.combinations(used_table, 2))
    for start_table, end_table in used_table_pairs:
        missed_table = utils.bridge_tables(start_table, end_table)
        if missed_table is not None:
            for table in missed_table:
                if (table not in missed_tables) and (table not in used_table):
                    missed_tables.append(table)

    for table in missed_tables:
        if table not in used_table:
            used_table_full.append(table)

    added_tables = []
    special = ['geolocation', 'sellers', 'customers']

    if len(used_table_full) == 1:
        tmp_table = f"{used_table_full[0]}" if used_table_full[0] not in special[1:] else f"{used_table_full[0]} AS {used_table_full[0][:-1]}"
        query += tmp_table
        query += " "
    elif (len(used_table_full) == 2): 
        if 'geolocation' in used_table_full:
            if 'sellers' in used_table_full:
                query += "(SELECT geolocation.geolocation_zip_code_prefix AS seller_zip_code_prefix, geolocation.geolocation_city AS seller_city, geolocation.geolocation_state AS seller_state, geolocation.geolocation_lat AS seller_lat, geolocation.geolocation_lng AS seller_lng, sellers.seller_id AS seller_id FROM geolocation INNER JOIN sellers ON geolocation.geolocation_zip_code_prefix = sellers.seller_zip_code_prefix) AS seller "
            elif 'customers' in used_table_full:
                query += " (SELECT geolocation.geolocation_zip_code_prefix AS customer_zip_code_prefix, geolocation.geolocation_city AS customer_city, geolocation.geolocation_state AS customer_state, geolocation.geolocation_lat AS customer_lat, geolocation.geolocation_lng AS customer_lng, customers.customer_id AS customer_id, customers.customer_unique_id AS customer_unique_id FROM geolocation INNER JOIN customers ON geolocation.geolocation_zip_code_prefix = customers.customer_zip_code_prefix) AS customer "
        else:
            first = f"{used_table_full[0]}" if used_table_full[0] not in special else f"{used_table_full[0]} AS {used_table_full[0][:-1]}"
            first_table = f"{used_table_full[0]}" if used_table_full[0] not in special else f"{used_table_full[0][:-1]}"
            second = f"{used_table_full[1]}" if used_table_full[1] not in special else f"{used_table_full[1]} AS {used_table_full[1][:-1]}"
            second_table = f"{used_table_full[1]}" if used_table_full[1] not in special else f"{used_table_full[1][:-1]}"
            joint_point = database_config['foreign_keys'][used_table_full[0]][used_table_full[1]]

            query += f"{first} INNER JOIN {second} ON {first_table}.{joint_point['local']} = {second_table}.{joint_point['remote']} "
    else:
        if "sellers" in used_table_full and "geolocation" in used_table_full:
            query += "(SELECT geolocation.geolocation_zip_code_prefix AS seller_zip_code_prefix, geolocation.geolocation_city AS seller_city, geolocation.geolocation_state AS seller_state, geolocation.geolocation_lat AS seller_lat, geolocation.geolocation_lng AS seller_lng, sellers.seller_id AS seller_id FROM geolocation INNER JOIN sellers ON geolocation.geolocation_zip_code_prefix = sellers.seller_zip_code_prefix) AS seller "
            added_tables.append('sellers')
            added_tables.append('geolocation')
        elif "customers" in used_table_full and "geolocation" in used_table_full:
            query += " (SELECT geolocation.geolocation_zip_code_prefix AS customer_zip_code_prefix, geolocation.geolocation_city AS customer_city, geolocation.geolocation_state AS customer_state, geolocation.geolocation_lat AS customer_lat, geolocation.geolocation_lng AS customer_lng, customers.customer_id AS customer_id, customers.customer_unique_id AS customer_unique_id FROM geolocation INNER JOIN customers ON geolocation.geolocation_zip_code_prefix = customers.customer_zip_code_prefix) AS customer "
            added_tables.append('customers')
            added_tables.append('geolocation')
        else:
            first_table = used_table_full[0]
            start = f"{first_table}" if first_table not in special else f"{first_table} AS {first_table[:-1]}"
            query += f"{start} "
            added_tables.append(first_table)

        # print(added_tables, used_table, used_table_full)

        all_added = False
        loop_limit = 20
        while not(all_added):
            for i in used_table_full:
                is_added = False
                if i not in added_tables:
                    for j in added_tables:
                        bridge = utils.bridge_tables(i, j)
                        if len(bridge) == 0 and j != "geolocation":
                            if i == "customers" and "geolocation" in added_tables:
                                query += "INNER JOIN (SELECT geolocation.geolocation_zip_code_prefix AS customer_zip_code_prefix, geolocation.geolocation_city AS customer_city, geolocation.geolocation_state AS customer_state, geolocation.geolocation_lat AS customer_lat, geolocation.geolocation_lng AS customer_lng, customers.customer_id AS customer_id, customers.customer_unique_id AS customer_unique_id FROM geolocation INNER JOIN customers ON geolocation.geolocation_zip_code_prefix = customers.customer_zip_code_prefix) AS customer "
                                # new_table = f"{i}" if i not in special else f"{i} AS {i[:-1]}"
                                # added_table  = f"{j}" if j not in special else f"{j} AS {j[:-1]}"
                                joint_point = database_config['foreign_keys'][i][j]

                                query += f"ON customer.{joint_point['local']} = {j}.{joint_point['remote']} "
                                added_tables.append(i)
                            else:
                                new_long = f"{i}" if i not in special else f"{i} AS {i[:-1]}"
                                new_table = f"{i}" if i not in special else f"{i[:-1]}"
                                added_long = f"{j}" if j not in special else f"{j} AS {j[:-1]}"
                                added_table  = f"{j}" if j not in special else f"{j[:-1]}"
                                joint_point = database_config['foreign_keys'][i][j]
                                query += f"INNER JOIN {new_long} ON {new_table}.{joint_point['local']} = {added_table}.{joint_point['remote']} "
                                added_tables.append(i)
                            is_added = True
                            break 
                        else:
                            pass
                if is_added:
                    break 

            counter = 0
            problem = []
            for i in used_table_full:
                if i in added_tables:
                    counter += 1
                else:
                    problem.append(i)
            # print(counter, len(used_table_full), problem, used_table_full)
            if counter == len(used_table_full):
                all_added=True

            loop_limit -= 1
            if loop_limit == 0:
                break
                
                



    # WHERE 
    query += "WHERE " 

            
    # Add filter
    for name in used_table:
        if name == "geolocation":
            if "customers" not in used_table_full and "sellers" not in used_table_full:
                key = used_filters[name]['geolocation_zip_code_prefix']
                if key is not None:
                    query += f"geolocation.geolocation_zip_code_prefix = '{key}' AND "
                key = used_filters[name]['geolocation_lat']
                if key[0] <= key[1]:
                    query += f"geolocation.geolocation_lat BETWEEN {key[0]} AND {key[1]} AND "
                elif key[0] > key[1]:
                    query += f"geolocation.geolocation_lat BETWEEN {key[1]} AND {key[0]} AND "
                key = used_filters[name]['geolocation_lng']
                if key[0] <= key[1]:
                    query += f"geolocation.geolocation_lng BETWEEN {key[0]} AND {key[1]} AND "
                elif key[0] > key[1]:
                    query += f"geolocation.geolocation_lng BETWEEN {key[1]} AND {key[0]} AND "
                key = used_filters[name]['geolocation_city']
                if key is not None:
                    query += f"geolocation.geolocation_city = '{key}' AND "
                key = used_filters[name]['geolocation_state']
                if key is not None:
                    query += f"geolocation.geolocation_state = '{key}' AND "
            else:
                if "customers" in used_table_full:
                    key = used_filters[name]['geolocation_zip_code_prefix']
                    if key is not None:
                        query += f"customer.customer_zip_code_prefix = '{key}' AND "
                    key = used_filters[name]['geolocation_lat']
                    if key[0] <= key[1]:
                        query += f"customer.customer_lat BETWEEN {key[0]} AND {key[1]} AND "
                    elif key[0] > key[1]:
                        query += f"customer.customer_lat BETWEEN {key[1]} AND {key[0]} AND "
                    key = used_filters[name]['geolocation_lng']
                    if key[0] <= key[1]:
                        query += f"customer.customer_lng BETWEEN {key[0]} AND {key[1]} AND "
                    elif key[0] > key[1]:
                        query += f"customer.customer_lng BETWEEN {key[1]} AND {key[0]} AND "
                    key = used_filters[name]['geolocation_city']
                    if key is not None:
                        query += f"customer.customer_city = '{key}' AND "
                    key = used_filters[name]['geolocation_state']
                    if key is not None:
                        query += f"customer.customer_state = '{key}' AND "
                if "sellers" in used_table_full:
                    key = used_filters[name]['geolocation_zip_code_prefix']
                    if key is not None:
                        query += f"seller.seller_zip_code_prefix = '{key}' AND "
                    key = used_filters[name]['geolocation_lat']
                    if key[0] <= key[1]:
                        query += f"seller.seller_lat BETWEEN {key[0]} AND {key[1]} AND "
                    elif key[0] > key[1]:
                        query += f"seller.seller_lat BETWEEN {key[1]} AND {key[0]} AND "
                    key = used_filters[name]['geolocation_lng']
                    if key[0] <= key[1]:
                        query += f"seller.seller_lng BETWEEN {key[0]} AND {key[1]} AND "
                    elif key[0] > key[1]:
                        query += f"seller.seller_lng BETWEEN {key[1]} AND {key[0]} AND "
                    key = used_filters[name]['geolocation_city']
                    if key is not None:
                        query += f"seller.seller_city = '{key}' AND "
                    key = used_filters[name]['geolocation_state']
                    if key is not None:
                        query += f"seller.seller_state = '{key}' AND "
        if name == "sellers":
            key = used_filters[name]['seller_id']
            if key is not None:
                query += f"seller.seller_id = '{key}' AND "
            key = used_filters[name]['seller_zip_code_prefix']
            if key is not None:
                query += f"seller.seller_zip_code_prefix = '{key}' AND "
            key = used_filters[name]['seller_city']
            if key is not None:
                query += f"seller.seller_zip_code_prefix = '{key}' AND "
            key = used_filters[name]['seller_state']
            if key is not None:
                query += f"seller.seller_state = '{key}' AND "
        if name == "customers":
            key = used_filters[name]['customer_id']
            if key is not None:
                query += f"customer.customer_id = '{key}' AND "
            key = used_filters[name]['customer_unique_id']
            if key is not None:
                query += f"customer.customer_unique_id = '{key}' AND "
            key = used_filters[name]['customer_zip_code_prefix']
            if key is not None:
                query += f"customer.customer_zip_code_prefix = '{key}' AND "
            key = used_filters[name]['customer_city']
            if key is not None:
                query += f"customer.customer_city = '{key}' AND "
            key = used_filters[name]['customer_state']
            if key is not None:
                query += f"customer.customer_state = '{key}' AND "
        if name == "orders":
            key = used_filters[name]['order_id']
            if key is not None:
                query += f"orders.order_id = '{key}' AND "
            key = used_filters[name]["customer_id"]
            if key is not None:
                query += f"orders.customer_id = '{key}' AND "
            key = used_filters[name]["order_status"]
            if key is not None:
                query += f"orders.order_status = '{key}' AND "
            key = used_filters[name]["order_purchase_timestamp"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"orders.order_purchase_timestamp BETWEEN '{key[0].strftime(config.timefstr['long'])}' AND '{key[1].strftime(config.timefstr['long'])}' AND "
                elif key[1] < key[0]:
                    query += f"orders.order_purchase_timestamp BETWEEN '{key[1].strftime(config.timefstr['long'])}' AND '{key[0].strftime(config.timefstr['long'])}' AND "
            key = used_filters[name]["order_approved_at"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"orders.order_approved_at BETWEEN '{key[0].strftime(config.timefstr['long'])}' AND '{key[1].strftime(config.timefstr['long'])}' AND "
                elif key[1] < key[0]:
                    query += f"orders.order_approved_at BETWEEN '{key[1].strftime(config.timefstr['long'])}' AND '{key[0].strftime(config.timefstr['long'])}' AND "
            key = used_filters[name]["order_delivered_carrier_date"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"orders.order_delivered_carrier_date BETWEEN '{key[0].strftime(config.timefstr['long'])}' AND '{key[1].strftime(config.timefstr['long'])}' AND "
                elif key[1] < key[0]:
                    query += f"orders.order_delivered_carrier_date BETWEEN '{key[1].strftime(config.timefstr['long'])}' AND '{key[0].strftime(config.timefstr['long'])}' AND "
            key = used_filters[name]["order_delivered_customer_date"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"orders.order_delivered_customer_date BETWEEN '{key[0].strftime(config.timefstr['long'])}' AND '{key[1].strftime(config.timefstr['long'])}' AND "
                elif key[1] < key[0]:
                    query += f"orders.order_delivered_customer_date BETWEEN '{key[1].strftime(config.timefstr['long'])}' AND '{key[0].strftime(config.timefstr['long'])}' AND "
            key = used_filters[name]["order_estimated_delivery_date"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"orders.order_estimated_delivery_date BETWEEN '{key[0].strftime(config.timefstr['short'])}' AND '{key[1].strftime(config.timefstr['short'])}' AND "
                elif key[1] < key[0]:
                    query += f"orders.order_estimated_delivery_date BETWEEN '{key[1].strftime(config.timefstr['short'])}' AND '{key[0].strftime(config.timefstr['short'])}' AND "
        if name == "order_payments":
            key = used_filters[name]["order_id"]
            if key is not None:
                query += f"order_payments.order_id = '{key}' AND "
            key = used_filters[name]["payment_sequential"]
            if key is not None:
                query += f"order_payments.payment_sequential = '{key}' AND "
            key = used_filters[name]["payment_type"]
            if key is not None:
                query += f"order_payments.payment_type = '{key}' AND "
            key = used_filters[name]["payment_installments"]
            if key is not None:
                query += f"order_payments.payment_installments = '{key}' AND "
            key = used_filters[name]["payment_value"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"order_payments.payment_value BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"order_payments.payment_value BETWEEN {key[1]} AND {key[0]} AND "
        if name == "products":
            key = used_filters[name]["product_id"]
            if key is not None:
                query += f"products.order_id = '{key}' AND "
            key = used_filters[name]["product_category_name"]
            if key is not None:
                query += f"products.product_category_name = '{key}' AND "
            key = used_filters[name]["product_name_length"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_name_length BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_name_length BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["product_description_length"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_description_length BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_description_length BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["product_photos_qty"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_photos_qty BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_photos_qty BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["product_weight_g"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_photos_qty BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_photos_qty BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["product_length_cm"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_length_cm BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_length_cm BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["product_height_cm"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_height_cm BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_height_cm BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["product_width_cm"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"products.product_width_cm BETWEEN {key[0]} AND {key[1]} AND "
                elif key[1] < key[0]:
                    query += f"products.product_width_cm BETWEEN {key[1]} AND {key[0]} AND "
        if name == "order_items":
            key = used_filters[name]["order_id"]
            if key is not None:
                query += f"order_items.order_id = '{key}' AND "
            key = used_filters[name]["order_item_id"]
            if key is not None:
                query += f"order_items.order_item_id = '{key}' AND "
            key = used_filters[name]["product_id"]
            if key is not None:
                query += f"order_items.product_id = '{key}' AND "
            key = used_filters[name]["seller_id"]
            if key is not None:
                query += f"order_items.seller_id = '{key}' AND "
            key = used_filters[name]["shipping_limit_date"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"order_items.shipping_limit_date BETWEEN '{key[0].strftime(config.timefstr['long'])}' AND '{key[1].strftime(config.timefstr['long'])}' AND "
                elif key[1] < key[0]:
                    query += f"order_items.shipping_limit_date BETWEEN '{key[1].strftime(config.timefstr['long'])}' AND '{key[0].strftime(config.timefstr['long'])}' AND "
            key = used_filters[name]["price"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"order_items.price BETWEEN {key[0]} AND {key[1]} AND "
                elif key[0] > key[1]:
                    query += f"order_items.price BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["freight_value"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"order_items.freight_value BETWEEN {key[0]} AND {key[1]} AND "
                elif key[0] > key[1]:
                    query += f"order_items.freight_value BETWEEN {key[1]} AND {key[0]} AND "
        if name == "order_reviews":
            key = used_filters[name]["review_id"]
            if key is not None:
                query += f"order_items.order_id = '{key}' AND "
            key = used_filters[name]["order_id"]
            if key is not None:
                query += f"order_items.order_id = '{key}' AND "
            key = used_filters[name]["review_score"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"order_reviews.review_score BETWEEN {key[0]} AND {key[1]} AND "
                elif key[0] > key[1]:
                    query += f"order_reviews.review_score BETWEEN {key[1]} AND {key[0]} AND "
            key = used_filters[name]["review_creation_date"]
            if key is not None:
                if key[0] <= key[1]:
                    query += f"order_reviews.review_creation_date BETWEEN '{key[0].strftime(config.timefstr['short'])}' AND '{key[1].strftime(config.timefstr['short'])}' AND "
                elif key[1] < key[0]:
                    query += f"order_reviews.review_creation_date BETWEEN '{key[1].strftime(config.timefstr['short'])}' AND '{key[0].strftime(config.timefstr['short'])}' AND "

    # clean last AND
    # query = query[:-4]
    if query.strip().endswith("AND"):
        query = query.rstrip()[:-3].rstrip()
    if query.strip().endswith("WHERE"):
        query = query.rstrip()[:-5].rstrip()

    # end
    query += ";"


    if filter_area_buttom:
        result = connector.query(query)
        st.session_state.result = result
        # st.session_state.result = query
        st.session_state.query = query

    if st.session_state.data_changed == True:
        st.session_state.data_changed = False
        st.session_state.result = st.session_state.connector.query(st.session_state.query)

    if st.session_state.query == "":
        right_column.write("Please click submit for search data.")
    else:
        right_column.dataframe(st.session_state.result, height = config.table_height)

# Solving behavior: Add 

# st.dialog
# st.tab

stack = []

@st.dialog("Add", width="large")
def add_data():
    # Preparation
    connector = st.session_state.connector
    prepared_name = utils.generate_random_string()
    disable_button = False # If data illegal, disable_button = True
    while prepared_name in st.session_state.stack:
        prepared_name = utils.generate_random_string()

    table_name_list = [
            'geolocation',
            'sellers', 
            'customers', 
            'orders', 
            'order_payments', 
            'products', 
            'order_items', 
            'order_reviews'
            ]
    # tab_geolocation, tab_sellers, tab_customers, tab_orders, tab_order_payments, tab_products, tab_order_items, tab_order_reviews = st.tabs(table_name_list)
    selected_dataset = st.radio(
            "Choose a table:",
            table_name_list
            )
    data = None

    # Get Input
    # with tab_geolocation:
    if selected_dataset == "geolocation":
        # Prepare storage variable
        data = {
                "name": "geolocation",
                "geolocation_zip_code_prefix": {"input": None, "limit": None},
                "geolocation_lat": {"input": None, "limit": None},
                "geolocation_lng": {"input": None, "limit": None},
                "geolocation_city": {"input": None, "limit": None},
                "geolocation_state": {"input": None, "limit": None},
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['geolocation_zip_code_prefix']['limit'] = connector.get_single_unique(data['name'], 'geolocation_zip_code_prefix').astype(int).sort_values().astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lat')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['geolocation_lat']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lng')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['geolocation_lng']['limit'] = (tmp_min, tmp_max)
        data['geolocation_city']['limit'] = connector.get_single_unique(data['name'], 'geolocation_city').astype(str).tolist()
        data['geolocation_state']['limit'] = connector.get_single_unique(data['name'], 'geolocation_state').astype(str).tolist()

        # Input widgets # no wrong input type handling
        data['geolocation_zip_code_prefix']['input'] = st.text_input(label="zip_code_prefix", max_chars=5)
        data['geolocation_lat']['input'] = st.number_input(label="Latitude", step=0.01)
        data['geolocation_lng']['input'] = st.number_input(label="Longtitude", step=0.01)
        data['geolocation_city']['input'] = st.text_input(label="City", max_chars=32)
        data['geolocation_state']['input'] = st.text_input(label="State", max_chars=2)
    # with tab_sellers:
    if selected_dataset == "sellers":
        data = {
                "name": "sellers",
                "seller_id": {"input": None, "limit": None},
                "seller_zip_code_prefix": {"input": None, "limit": None},
                "seller_city": {"input": None, "limit": None},
                "seller_state": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['seller_id']['limit'] = connector.get_single_unique(data['name'], "seller_id").astype(str).tolist()
        data['seller_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['seller_city']['limit'] = connector.get_single_unique(data['name'], 'seller_city').astype(str).tolist()
        data['seller_state']['limit'] = connector.get_single_unique(data['name'], 'seller_state').astype(str).tolist()

        # Input widgets # no wrong input type handling
        data['seller_id']['input'] = st.text_input(label="ID", max_chars=32)
        data['seller_zip_code_prefix']['input'] = st.text_input(label="zip_code_prefix", key = "seller_zip_code_prefix", max_chars=5)
        data['seller_city']['input'] = st.text_input(label="City", key = "seller_city", max_chars=32)
        data['seller_state']['input'] = st.text_input(label="State", key = "seller_state", max_chars=2)
    # with tab_customers:
    if selected_dataset == "customers":
        data = { 
                "name": "customers",
                "customer_id": {"input": None, "limit": None},
                "customer_unique_id": {"input": None, "limit": None},
                "customer_zip_code_prefix": {"input": None, "limit": None},
                "customer_city": {"input": None, "limit": None},
                "customer_state": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['customer_id']['limit'] = connector.get_single_unique(data['name'], "customer_id").astype(str).tolist()
        data['customer_unique_id']['limit'] = connector.get_single_unique(data['name'], "customer_unique_id").astype(str).tolist()
        data['customer_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['customer_city']['limit'] = connector.get_single_unique(data['name'], 'customer_city').astype(str).tolist()
        data['customer_state']['limit'] = connector.get_single_unique(data['name'], 'customer_state').astype(str).tolist()

        # Input widgets # no wrong input type handling
        data['customer_id']['input'] = st.text_input(label="Account ID", max_chars=32)
        data['customer_unique_id']['input'] = st.text_input(label="Unique ID", max_chars=32)
        data['customer_zip_code_prefix']['input'] = st.text_input(label="zip_code_prefix", key = "customer_zip_code_prefix", max_chars=5)
        data['customer_city']['input'] = st.text_input(label="City", key = "customer_city", max_chars=32)
        data['customer_state']['input'] = st.text_input(label="State", key = "customer_state", max_chars=2)
    # with tab_orders:
    if selected_dataset == "orders":
        data = {
                "name": "orders",
                "order_id": {"input": None, "limit": None},
                "customer_id": {"input": None, "limit": None},
                "order_status": {"input": None, "limit": None},
                "order_purchase_timestamp": {"input": None, "limit": None},
                "order_approved_at": {"input": None, "limit": None},
                "order_delivered_carrier_date": {"input": None, "limit": None},
                "order_delivered_customer_date": {"input": None, "limit": None},
                "order_estimated_delivery_date": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(data['name'], "order_id").astype(str).tolist()
        data['customer_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['order_status']['limit'] = connector.get_single_unique(data['name'], "order_status").astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_purchase_timestamp')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_purchase_timestamp']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_approved_at')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_approved_at']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_carrier_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_delivered_carrier_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_customer_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_delivered_customer_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_estimated_delivery_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_estimated_delivery_date']['limit'] = (tmp_min, tmp_max)


        # Input widgets # no wrong input type handling
        data['order_id']['input'] = st.text_input(label="Order ID", max_chars=32)
        data['customer_id']['input'] = st.text_input(label="Customer ID", max_chars=32)
        data['order_status']['input'] = st.selectbox(label="Status", options=data['order_status']['limit'], index=None)
        key = "Purchase At"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = st.time_input(f"{key} - Time")
        data['order_purchase_timestamp']['input'] = (tmp_date, tmp_time)
        key = "Approved At"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = st.time_input(f"{key} - Time")
        data['order_approved_at']['input'] = (tmp_date, tmp_time)
        key = "Carrier's Delivered"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = st.time_input(f"{key} - Time")
        data['order_delivered_carrier_date']['input'] = (tmp_date, tmp_time)
        key = "Customer's Delivered"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = st.time_input(f"{key} - Time")
        data['order_delivered_customer_date']['input'] = (tmp_date, tmp_time)
        key = "Estimated Delivered"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = None
        data['order_estimated_delivery_date']['input'] = (tmp_date, tmp_time)
    # with tab_order_payments:
    if selected_dataset == "order_payments":
        data = {
                "name": "order_payments", 
                "order_id": {"input": None, "limit": None}, 
                "payment_sequential": {"input": None, "limit": None}, 
                "payment_type": {"input": None, "limit": None},
                "payment_installments": {"input": None, "limit": None},
                "payment_value": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['payment_sequential']['limit'] = connector.get_single_unique(data['name'], 'payment_sequential').astype(str).tolist()
        data['payment_type']['limit'] = connector.get_single_unique(data['name'], 'payment_type').astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_installments')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['payment_installments']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_value')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['payment_value']['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None)
        data['payment_sequential']['input'] = st.number_input(label="Payment Sequential", step=1, min_value=1)
        data['payment_type']['input'] = st.text_input(label="Payment Type", max_chars=20)
        data['payment_installments']['input'] = st.number_input(label="Payment Installments", step=1, min_value=0)
        data['payment_value']['input'] = st.number_input(label="payment_value", step=0.01, min_value=0.01)
    # with tab_products:
    if selected_dataset == "products":
        data = {
                "name": "products",
                "product_id": {"input": None, "limit": None},
                "product_category_name": {"input": None, "limit": None},
                "product_name_length": {"input": None, "limit": None},
                "product_description_length": {"input": None, "limit": None},
                "product_photos_qty": {"input": None, "limit": None},
                "product_weight_g": {"input": None, "limit": None},
                "product_length_cm": {"input": None, "limit": None},
                "product_height_cm": {"input": None, "limit": None},
                "product_width_cm": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['product_id']['limit'] = connector.get_single_unique(data['name'], 'product_id').astype(str).tolist()
        data['product_category_name']['limit'] = connector.get_single_unique(data['name'], 'product_category_name').astype(str).tolist()
        key = 'product_name_length'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_description_length'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_photos_qty'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_weight_g'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_length_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_height_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_width_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['product_id']['input'] = st.text_input(label="Product ID", max_chars=32)
        data['product_category_name']['input'] = st.text_input(label="Category Name", max_chars=64)
        data['product_name_length']['input'] = st.number_input(label="Product Name Length", step=1, min_value=1)
        data['product_description_length']['input'] = st.number_input(label="Product Description Length", step=1, min_value=1)
        data['product_photos_qty']['input'] = st.number_input(label="Photo qty", step=1, min_value=1)
        data['product_weight_g']['input'] = st.number_input(label="Weight (g)", step=1, min_value=1)
        data['product_length_cm']['input'] = st.number_input(label="Length (cm)", step=1, min_value=1)
        data['product_height_cm']['input'] = st.number_input(label="Height (cm)", step=1, min_value=1)
        data['product_width_cm']['input'] = st.number_input(label="Width (cm)", step=1, min_value=1)
    # with tab_order_items:
    if selected_dataset == "order_items":
        data = {
                "name": "order_items",
                "order_id": {"input": None, "limit": None},
                "order_item_id": {"input": None, "limit": None},
                "product_id": {"input": None, "limit": None},
                "seller_id": {"input": None, "limit": None},
                "shipping_limit_date": {"input": None, "limit": None},
                "price": {"input": None, "limit": None},
                "freight_value": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['order_item_id']['limit'] = connector.get_single_unique(data['name'], 'order_item_id').astype(str).tolist()
        data['product_id']['limit'] = connector.get_single_unique(referencing_information['table'][1], referencing_information['key'][1]).astype(str).tolist()
        data['seller_id']['limit'] = connector.get_single_unique(referencing_information['table'][2], referencing_information['key'][2]).astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'shipping_limit_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['shipping_limit_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'price')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['price']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'freight_value')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['freight_value']['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None, key = "order_items_order_id")
        data['order_item_id']['input'] = st.text_input(label="Item ID", max_chars=32)
        data['product_id']['input'] = st.selectbox(label="Product ID", options=data['product_id']['limit'], index=None)
        data['seller_id']['input'] = st.selectbox(label="Seller ID", options=data['seller_id']['limit'], index=None)
        key = "Shipping Limit"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = st.time_input(f"{key} - Time")
        data['shipping_limit_date']['input'] = (tmp_date, tmp_time)
        data['price']['input'] = st.number_input(label="Order Price", step=0.01, min_value=0.01)
        data['freight_value']['input'] = st.number_input(label="Freight Price", step=0.01, min_value=0.01)
    # with tab_order_reviews:
    if selected_dataset == "order_reviews":
        data = {
                "name": "order_reviews",
                "review_id": {"input": None, "limit": None},
                "order_id": {"input": None, "limit": None},
                "review_score": {"input": None, "limit": None},
                "review_creation_date": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0


        data['review_id']['limit'] = connector.get_single_unique(data['name'], 'review_id').astype(str).tolist()
        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_score')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['review_score']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_creation_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['review_creation_date']['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['review_id']['input'] = st.text_input(label="Review ID", max_chars=32)
        data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None, key = "order_reviews_order_id")
        data['review_score']['input'] = st.number_input(label="Review Score", step=1, min_value=1, max_value=5)
        key = "Review Creation"
        tmp_date = st.date_input(f"{key} - Date")
        tmp_time = None
        data['review_creation_date']['input'] = (tmp_date, tmp_time)

    # Search target
    if data is None:
        disable_button = True
    else:
        special = ['name']
        keys = [key for key in data.keys() if key not in special]
        foreign_keys = st.session_state.map['referencing_keys'][data['name']] # dict with parallel list
        primary_keys = st.session_state.map['primary_keys'][data['name']] # list

        # search whether value for foreign key exists
        exist_foreign_key = True
        if foreign_keys is not None:
            for idx in range(len(foreign_keys['local'])):
                foreign_query = f"SELECT * FROM {foreign_keys['table'][idx]} WHERE {foreign_keys['key'][idx]} = '{data[foreign_keys['local'][idx]]['input']}';" # foreign keys are zip_code or xx_id

                foreign_result = connector.query(foreign_query)
                if len(foreign_result) == 0:
                    exist_foreign_key = False;
                    st.error(f"You didn't input an existed value about {foreign_keys['local'][idx]}")

        # search whether value for primary key exists
        exist_primary_key = False
        inputted_primary_key = True
        for key in primary_keys:
            if (data[key]['input'] is None) or (data[key]['input'] == ""):
                inputted_primary_key = False
        if not(inputted_primary_key):
            exist_primary_key = True
            st.error(f"Unique information you need to input: ({', '.join(primary_keys)})")
        else:
            primary_query = f"SELECT * FROM {data['name']} WHERE "
            for key in primary_keys:
                primary_query += f"{key} = '{data[key]['input']}' AND "
            primary_query = primary_query[:-4]
            primary_query += ";"
            primary_result = connector.query(primary_query)
            if len(primary_result) != 0:
                exist_primary_key = True
                st.error(f"You input some value or value pair that exists in table {data['name']} over primary key ({', '.join(primary_keys)}). ")


        if not(exist_foreign_key):
            disable_button = True
        elif (exist_primary_key):
            disable_button = True

        # Special check
        if data['name'] == "orders":
            time_purchase= data['order_purchase_timestamp']['input']
            time_purchase= datetime.combine(time_purchase[0], time_purchase[1])
            time_approved= data['order_approved_at']['input']
            time_approved= datetime.combine(time_approved[0],time_approved[1])
            time_delivered_carrier= data['order_delivered_carrier_date']['input']
            time_delivered_carrier= datetime.combine(time_delivered_carrier[0],time_delivered_carrier[1])
            time_delivered_customer= data['order_delivered_customer_date']['input']
            time_delivered_customer= datetime.combine(time_delivered_customer[0],time_delivered_customer[1])
            time_estimated= data['order_estimated_delivery_date']['input']
            time_estimated= datetime.combine(time_estimated[0],datetime.min.time())
            if not ((time_purchase < time_approved and time_approved < time_delivered_carrier and time_delivered_carrier < time_delivered_customer) and (time_purchase < time_approved < time_estimated)):
                st.error("Your input about multiple recorded time is impossible generally.")
                disable_button = True

        for key in keys:
            if "_id" in key:
                if len(data[key]['input'].strip()) != 32:
                    st.error("You must make ID 32 characters long")
                    disable_button = True
                if " " in data[key]['input']:
                    st.error("No space in ID")
                    disable_button = True

                for i in range(len(data[key]['input'])):
                    if (data[key]['input'][i] not in string.ascii_lowercase) and (data[key]['input'][i] not in string.digits):
                        st.error("ID can only be constructed by letters in lower case or digits")
                        disable_button = True
                        break

        for key in keys:
            if "state" in key:
                if len(data[key]['input'].strip()) != 2:
                    st.error("You must make state 2 characters long without space")
                    disable_button = True

                for i in range(len(data[key]['input'])):
                    if data[key]['input'][i] not in string.ascii_uppercase:
                        st.error("Sate Code can only be constructed by letters in upper case")
                        disable_button = True
                        break

        for key in keys:
            if "city" in key:
                for i in range(len(data[key]['input'])):
                    if data[key]['input'][i] in string.digits:
                        st.error("City name shouldn't includes digit.")
                        disable_button = True
                        break

        for key in keys:
            if "zip" in key:
                if ((len(data[key]['input'].strip()) != 4) or (len(data[key]['input'].strip()) != 5)):
                    st.error("You must make Zip Code 4 characters or 5 characters long without space")
                    disable_button = True

                for i in range(len(data[key]['input'])):
                    if data[key]['input'][i] not in string.digits:
                        st.error("zip code can only be constructed by digits")
                        disable_button = True
                        break





    dialog_submit = st.button("Submit", disabled=disable_button)
    if dialog_submit:
        key_side = ""
        value_side = ""
        for key in keys:
            tmp_key = key
            tmp_value = data[key]['input']
            if tmp_value is not None:
                key_side += key
                key_side += ", "
                value_side += utils.input_preprocessing(tmp_value)
                value_side += ", "
        # remove last comma
        key_side = key_side[:-2]
        value_side = value_side[:-2]
        final_query = f"INSERT INTO {data['name']} ({key_side}) VALUES ({value_side});"
        connector.execute(final_query)
        connector.checkpoint_add(checkpoint="prepared_name")
        st.session_state.stack.append(prepared_name)
        st.session_state.data_changed = True
        st.rerun()

@st.dialog("Delete", width="large")
def delete_data():
    # Preparation
    connector = st.session_state.connector
    prepared_name = utils.generate_random_string()
    disable_button = False # If data illegal, disable_button = True
    while prepared_name in st.session_state.stack:
        prepared_name = utils.generate_random_string()

    table_name_list = [
            'geolocation',
            'sellers', 
            'customers', 
            'orders', 
            'order_payments', 
            'products', 
            'order_items', 
            'order_reviews'
            ]
    selected_dataset = st.radio(
            "Choose a table:",
            table_name_list
            )
    data = None

    # Get Input
    if selected_dataset == "geolocation":
        # Prepare storage variable
        data = {
                "name": "geolocation",
                "geolocation_zip_code_prefix": {"input": None, "limit": None},
                "geolocation_lat": {"input": None, "limit": None},
                "geolocation_lng": {"input": None, "limit": None},
                "geolocation_city": {"input": None, "limit": None},
                "geolocation_state": {"input": None, "limit": None},
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['geolocation_zip_code_prefix']['limit'] = connector.get_single_unique(data['name'], 'geolocation_zip_code_prefix').astype(int).sort_values().astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lat')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['geolocation_lat']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lng')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['geolocation_lng']['limit'] = (tmp_min, tmp_max)
        data['geolocation_city']['limit'] = connector.get_single_unique(data['name'], 'geolocation_city').astype(str).tolist()
        data['geolocation_state']['limit'] = connector.get_single_unique(data['name'], 'geolocation_state').astype(str).tolist()

        # Input widgets # no wrong input type handling
        data['geolocation_zip_code_prefix']['input'] = st.selectbox(label="Zip Code", options=data['geolocation_zip_code_prefix']['limit'], index=None)
        data['geolocation_lat']['input'] = st.slider(label="Latitude", min_value=data['geolocation_lat']['limit'][0], max_value=data['geolocation_lat']['limit'][1], value=data['geolocation_lat']['limit'], step=config.accuracy)
        data['geolocation_lng']['input'] = st.slider(label="Longtitude", min_value=data['geolocation_lng']['limit'][0], max_value=data['geolocation_lng']['limit'][1], value=data['geolocation_lng']['limit'], step=config.accuracy)
        data['geolocation_city']['input'] = st.selectbox(label="City", options = data['geolocation_city']['limit'], index=None)
        data['geolocation_state']['input'] = st.selectbox(label="State", options = data['geolocation_state']['limit'], index=None)
    if selected_dataset == "sellers":
        data = {
                "name": "sellers",
                "seller_id": {"input": None, "limit": None},
                "seller_zip_code_prefix": {"input": None, "limit": None},
                "seller_city": {"input": None, "limit": None},
                "seller_state": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['seller_id']['limit'] = connector.get_single_unique(data['name'], "seller_id").astype(str).tolist()
        data['seller_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['seller_city']['limit'] = connector.get_single_unique(data['name'], 'seller_city').astype(str).tolist()
        data['seller_state']['limit'] = connector.get_single_unique(data['name'], 'seller_state').astype(str).tolist()

        # Input widgets # no wrong input type handling
        data['seller_id']['input'] = st.selectbox(label="ID", options=data['seller_id']['limit'], index = None)
        data['seller_zip_code_prefix']['input'] = st.selectbox(label="zip_code", options=data['seller_zip_code_prefix']['limit'], index = None)
        data['seller_city']['input'] = st.selectbox(label="City", options=data['seller_city']['limit'], index = None)
        data['seller_state']['input'] = st.selectbox(label="State", options=data['seller_state']['limit'], index = None)
    if selected_dataset == "customers":
        data = { 
                "name": "customers",
                "customer_id": {"input": None, "limit": None},
                "customer_unique_id": {"input": None, "limit": None},
                "customer_zip_code_prefix": {"input": None, "limit": None},
                "customer_city": {"input": None, "limit": None},
                "customer_state": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['customer_id']['limit'] = connector.get_single_unique(data['name'], "customer_id").astype(str).tolist()
        data['customer_unique_id']['limit'] = connector.get_single_unique(data['name'], "customer_unique_id").astype(str).tolist()
        data['customer_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['customer_city']['limit'] = connector.get_single_unique(data['name'], 'customer_city').astype(str).tolist()
        data['customer_state']['limit'] = connector.get_single_unique(data['name'], 'customer_state').astype(str).tolist()

        # Input widgets # no wrong input type handling
        data['customer_id']['input'] = st.selectbox(label="ID", options=data['customer_id']['limit'], index = None)
        data['customer_unique_id']['input'] = st.selectbox(label="Unique_ID", options=data['customer_unique_id']['limit'], index = None)
        data['customer_zip_code_prefix']['input'] = st.selectbox(label="zip_code", options=data['customer_zip_code_prefix']['limit'], index = None, key="delete_customer_zip_code_prefix")
        data['customer_city']['input'] = st.selectbox(label="City", options=data['customer_city']['limit'], index = None)
        data['customer_city']['input'] = st.selectbox(label="State", options=data['customer_state']['limit'], index = None)
    if selected_dataset == "orders":
        data = {
                "name": "orders",
                "order_id": {"input": None, "limit": None},
                "customer_id": {"input": None, "limit": None},
                "order_status": {"input": None, "limit": None},
                "order_purchase_timestamp": {"input": None, "limit": None},
                "order_approved_at": {"input": None, "limit": None},
                "order_delivered_carrier_date": {"input": None, "limit": None},
                "order_delivered_customer_date": {"input": None, "limit": None},
                "order_estimated_delivery_date": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(data['name'], "order_id").astype(str).tolist()
        data['customer_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['order_status']['limit'] = connector.get_single_unique(data['name'], "order_status").astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_purchase_timestamp')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_purchase_timestamp']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_approved_at')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_approved_at']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_carrier_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_delivered_carrier_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_customer_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_delivered_customer_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_estimated_delivery_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_estimated_delivery_date']['limit'] = (tmp_min, tmp_max)


        # Input widgets # no wrong input type handling
        data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None)
        data['customer_id']['input'] = st.selectbox(label="Customer ID", options=data['customer_id']['limit'], index=None)
        data['order_status']['input'] = st.selectbox(label="Status", options=data['order_status']['limit'], index=None)
        data['order_purchase_timestamp']['input'] = st.slider("Purchase time", min_value=data['order_purchase_timestamp']['limit'][0], max_value=data['order_purchase_timestamp']['limit'][1], value=data['order_purchase_timestamp']['limit'], step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_approved_at']['input'] = st.slider("Approved time", min_value=data['order_approved_at']['limit'][0], max_value=data['order_approved_at']['limit'][1], value=data['order_approved_at']['limit'], step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_delivered_carrier_date']['input'] = st.slider("Delivered Carrier Date", min_value=data['order_delivered_carrier_date']['limit'][0], max_value=data['order_delivered_carrier_date']['limit'][1], value=data['order_delivered_carrier_date']['limit'], step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_delivered_customer_date']['input'] = st.slider("Delivered Customer Date", min_value=order_delivered_customer_date_min, max_value=order_delivered_customer_date_max, value=(order_delivered_customer_date_min, order_delivered_customer_date_max), step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_estimated_delivery_date']['input'] = st.slider("Estimated Delivery Date", min_value=data['order_estimated_delivery_date']['limit'][0], max_value=data['order_estimated_delivery_date']['limit'][1], value=data['order_estimated_delivery_date']['limit'], step=config.time['step']['day'], format=config.time['format']['short'])
    if selected_dataset == "order_payments":
        data = {
                "name": "order_payments", 
                "order_id": {"input": None, "limit": None}, 
                "payment_sequential": {"input": None, "limit": None}, 
                "payment_type": {"input": None, "limit": None},
                "payment_installments": {"input": None, "limit": None},
                "payment_value": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['payment_sequential']['limit'] = connector.get_single_unique(data['name'], 'payment_sequential').astype(str).tolist()
        data['payment_type']['limit'] = connector.get_single_unique(data['name'], 'payment_type').astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_installments')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['payment_installments']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_value')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['payment_value']['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['order_id']['input'] = st.selectbox(label = "Order ID", options = data['order_id']['limit'], index = None, key="update_order_payments_order_id")
        data['payment_sequential']['input'] = st.selectbox(label = "Payment Sequential", options=data['payment_sequential']['limit'], index=None)
        data['payment_type']['input'] = st.selectbox(label="Payment Type", options=data['payment_type']['limit'], index = None)
        data['payment_installments']['input'] = st.selectbox(label="Payment Installments", options=data['payment_installments']['limit'], index = None)
        data['payment_value']['input'] = st.slider("Payment Value", min_value=data['payment_value']['limit'][0], max_value=data['payment_value']['limit'][1], value=data['payment_value']['limit'], step=config.concurrency)
    if selected_dataset == "products":
        data = {
                "name": "products",
                "product_id": {"input": None, "limit": None},
                "product_category_name": {"input": None, "limit": None},
                "product_name_length": {"input": None, "limit": None},
                "product_description_length": {"input": None, "limit": None},
                "product_photos_qty": {"input": None, "limit": None},
                "product_weight_g": {"input": None, "limit": None},
                "product_length_cm": {"input": None, "limit": None},
                "product_height_cm": {"input": None, "limit": None},
                "product_width_cm": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['product_id']['limit'] = connector.get_single_unique(data['name'], 'product_id').astype(str).tolist()
        data['product_category_name']['limit'] = connector.get_single_unique(data['name'], 'product_category_name').astype(str).tolist()
        key = 'product_name_length'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_description_length'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_photos_qty'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_weight_g'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_length_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_height_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_width_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['product_id']['input'] = st.selectbox(label="Product ID", options = data['product_id']['limit'], index=None)
        data['product_category_name']['input'] = st.selectbox(label="Product Category", options=data['product_category_name']['limit'], index = None)
        data['product_name_length']['input'] = st.slider(label="Name Length", min_value=data['product_name_length']['limit'][0], max_value=data['product_name_length']['limit'][1], value=data['product_name_length']['limit'], step=config.integer)
        data['product_description_length']['input'] = st.slider(label="Description Length", min_value=data['product_description_length']['limit'][0], max_value=data['product_description_length']['limit'][1], value=data['product_description_length']['limit'], step=config.integer)
        data['product_photos_qty']['input'] = st.slider(label="Photo Quantity", min_value=data['product_photos_qty']['limit'][0], max_value=data['product_photos_qty']['limit'][1], value=data['product_photos_qty']['limit'], step=config.integer)
        data['product_weight_g']['input'] = st.slider(label="Weight in gram", min_value=data['product_weight_g']['limit'][0], max_value=data['product_weight_g']['limit'][1], value=data['product_weight_g']['limit'], step=config.integer)
        data['product_length_cm']['input'] = st.slider(label="Length in cm", min_value=data['product_length_cm']['limit'][0], max_value=data['product_length_cm']['limit'][1], value=data['product_length_cm']['limit'], step=config.integer)
        data['product_height_cm']['input'] = st.slider(label="Height in cm", min_value=data['product_height_cm']['limit'][0], max_value=data['product_height_cm']['limit'][1], value=data['product_height_cm']['limit'], step=config.integer)
        data['product_width_cm']['input'] = st.slider(label="Width in cm", min_value=data['product_width_cm']['limit'][0], max_value=data['product_width_cm']['limit'][1], value=data['product_width_cm']['limit'], step=config.integer)
    if selected_dataset == "order_items":
        data = {
                "name": "order_items",
                "order_id": {"input": None, "limit": None},
                "order_item_id": {"input": None, "limit": None},
                "product_id": {"input": None, "limit": None},
                "seller_id": {"input": None, "limit": None},
                "shipping_limit_date": {"input": None, "limit": None},
                "price": {"input": None, "limit": None},
                "freight_value": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['order_item_id']['limit'] = connector.get_single_unique(data['name'], 'order_item_id').astype(str).tolist()
        data['product_id']['limit'] = connector.get_single_unique(referencing_information['table'][1], referencing_information['key'][1]).astype(str).tolist()
        data['seller_id']['limit'] = connector.get_single_unique(referencing_information['table'][2], referencing_information['key'][2]).astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'shipping_limit_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['shipping_limit_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'price')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['price']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'freight_value')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['freight_value']['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['order_id']['input'] = st.selectbox(label="Order ID", options = data['order_id']['limit'], index=None, key="update_order_items_order_id")
        data['order_item_id']['input'] = st.selectbox(label="Order Item ID", options=data['order_item_id']['limit'], index=None)
        data['product_id']['input'] = st.selectbox(label="Product ID", options=data['product_id']['limit'], index=None, key="order_items-product_id")
        data['seller_id']['input'] = st.selectbox(label="Seller ID", options=data['seller_id']['limit'], index=None)
        data['shipping_limit_date']['input'] = st.slider("Shipping Limit Date", min_value=data['shipping_limit_date']['limit'][0], max_value=data['shipping_limit_date']['limit'][1], value=data['shipping_limit_date']['limit'], step=config.time['step']['day'], format=config.time['format']['short'])
        data['price']['input'] = st.slider("Price", min_value=data['price']['limit'][0], max_value=data['price']['limit'][1], value=data['price']['limit'], step=config.concurrency)
        data['freight_value']['input'] = st.slider("Freight Value", min_value=data['freight_value']['limit'][0], max_value=data['freight_value']['limit'][1], value=data['freight_value']['limit'], step=config.concurrency)
    if selected_dataset == "order_reviews":
        data = {
                "name": "order_reviews",
                "review_id": {"input": None, "limit": None},
                "order_id": {"input": None, "limit": None},
                "review_score": {"input": None, "limit": None},
                "review_creation_date": {"input": None, "limit": None}
                }

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0


        data['review_id']['limit'] = connector.get_single_unique(data['name'], 'review_id').astype(str).tolist()
        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_score')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['review_score']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_creation_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['review_creation_date']['limit'] = (tmp_min, tmp_max)

        # Input widgets # no wrong input type handling
        data['review_id']['input'] = st.selectbox("Review ID", options=data['review_id']['limit'], index=None)
        data['order_id']['input'] = st.selectbox("order_id", options=data['order_id']['limit'], index=None)
        data['review_score']['input'] = st.slider("Review Score", min_value=data['review_score']['limit'][0], max_value=data['review_score']['limit'][1], value = data['review_score']['limit'], step=config.integer)
        data['review_creation_date']['input'] = st.slider("Review Creation Date", min_value=data['review_creation_date']['limit'][0], max_value=data['review_creation_date']['limit'][1], value=data['review_creation_date']['limit'], step=config.time['step']['day'], format=config.time['format']['short'])

    if data is None:
        disable_button = True
    else:
        special = ['name']
        keys = [key for key in data.keys() if key not in special]
        foreign_keys = st.session_state.map['referencing_keys'][data['name']] # dict with parallel list
        primary_keys = st.session_state.map['primary_keys'][data['name']] # list

        # search target exists
        inputted_primary_key = True
        for key in primary_keys:
            if (data[key]['input'] is None) or (data[key]['input'] == ""):
                inputted_primary_key = False

        if not(inputted_primary_key):
            disable_button = True
            st.error(f"Please at least select value for ({', '.join(primary_keys)})")
        else:
            record_query = f"SELECT * FROM {data['name']} WHERE "
            for key in keys:
                value = data[key]['input']
                if value is not None:
                    slice = utils.search_preprocessing(key, value)
                    if slice is None:
                        st.error(f"Meet an unexpected value {key}:{value}")
                    else:
                        record_query += slice
                        record_query += " AND "
            record_query = record_query[:-4]
            record_query += ";"

            record_result = connector.query(record_query)
            if len(record_result) == 0:
                disable_button = True
                st.error("Searched record doesn't exists with filters")
            else:
                delete_query = "DELETE FROM {data['name']} WHERE "
                for key in primary_keys:
                    slice = utils.search_preprocessing(key, data[key]['input'])
                    # no protection for search exact ID only.
                    if slice is None:
                        disable_button = True
                        st.error("Error! Target primary key disappeared")
                    delete_query += slice
                    delete_query += " AND "
                # delete last end
                delete_query = delete_query[:-4]
                delete_query += ";"




    dialog_submit = st.button("Submit", disabled=disable_button)
    if dialog_submit:
        connector.execute(delete_query)
        connector.checkpoint_add(prepared_name)
        st.session_state.stack.append(prepared_name)
        st.session_state.data_changed = True
        st.rerun()

@st.dialog("Update", width="large")
def update_data():

    # Preparation
    connector = st.session_state.connector
    prepared_name = utils.generate_random_string()
    disable_button = False # If data illegal, disable_button = True
    while prepared_name in st.session_state.stack:
        prepared_name = utils.generate_random_string()

    table_name_list = [
            'geolocation',
            'sellers', 
            'customers', 
            'orders', 
            'order_payments', 
            'products', 
            'order_items', 
            'order_reviews'
            ]
    # tab_geolocation, tab_sellers, tab_customers, tab_orders, tab_order_payments, tab_products, tab_order_items, tab_order_reviews = st.tabs(table_name_list)
    selected_dataset = st.radio(
            "Choose a table:",
            table_name_list
            )
    data = None

    # Get Input
    # with tab_geolocation:
    if selected_dataset == "geolocation":
        # Prepare storage variable
        data = {
                "name": "geolocation",
                "geolocation_zip_code_prefix": {"changed": None, "input": None, "limit": None},
                "geolocation_lat": {"changed": None, "input": None, "limit": None},
                "geolocation_lng": {"changed": None, "input": None, "limit": None},
                "geolocation_city": {"changed": None, "input": None, "limit": None},
                "geolocation_state": {"changed": None, "input": None, "limit": None},
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['geolocation_zip_code_prefix']['limit'] = connector.get_single_unique(data['name'], 'geolocation_zip_code_prefix').astype(int).sort_values().astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lat')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['geolocation_lat']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lng')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['geolocation_lng']['limit'] = (tmp_min, tmp_max)
        data['geolocation_city']['limit'] = connector.get_single_unique(data['name'], 'geolocation_city').astype(str).tolist()
        data['geolocation_state']['limit'] = connector.get_single_unique(data['name'], 'geolocation_state').astype(str).tolist()

        # Original data
        data['geolocation_zip_code_prefix']['input'] = left_column.selectbox(label="Zip Code", options=data['geolocation_zip_code_prefix']['limit'], index=None)
        data['geolocation_lat']['input'] = left_column.slider(label="Latitude", min_value=data['geolocation_lat']['limit'][0], max_value=data['geolocation_lat']['limit'][1], value=data['geolocation_lat']['limit'], step=config.accuracy)
        data['geolocation_lng']['input'] = left_column.slider(label="Longtitude", min_value=data['geolocation_lng']['limit'][0], max_value=data['geolocation_lng']['limit'][1], value=data['geolocation_lng']['limit'], step=config.accuracy)
        data['geolocation_city']['input'] = left_column.selectbox(label="City", options = data['geolocation_city']['limit'], index=None)
        data['geolocation_state']['input'] = left_column.selectbox(label="State", options = data['geolocation_state']['limit'], index=None)

        # New data
        data['geolocation_zip_code_prefix']['changed'] = right_column.text_input(label="zip_code_prefix", max_chars=5)
        data['geolocation_lat']['changed'] = right_column.number_input(label="Latitude", step=0.01)
        data['geolocation_lng']['changed'] = right_column.number_input(label="Longtitude", step=0.01)
        data['geolocation_city']['changed'] = right_column.text_input(label="City", max_chars=32)
        data['geolocation_state']['changed'] = right_column.text_input(label="State", max_chars=2)
    # with tab_sellers:
    if selected_dataset == "sellers":
        data = {
                "name": "sellers",
                "seller_id": {"changed": None, "input": None, "limit": None},
                "seller_zip_code_prefix": {"changed": None, "input": None, "limit": None},
                "seller_city": {"changed": None, "input": None, "limit": None},
                "seller_state": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['seller_id']['limit'] = connector.get_single_unique(data['name'], "seller_id").astype(str).tolist()
        data['seller_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['seller_city']['limit'] = connector.get_single_unique(data['name'], 'seller_city').astype(str).tolist()
        data['seller_state']['limit'] = connector.get_single_unique(data['name'], 'seller_state').astype(str).tolist()

        # Original data
        data['seller_id']['input'] = left_column.selectbox(label="ID", options=data['seller_id']['limit'], index = None)
        data['seller_zip_code_prefix']['input'] = left_column.selectbox(label="zip_code", options=data['seller_zip_code_prefix']['limit'], index = None)
        data['seller_city']['input'] = left_column.selectbox(label="City", options=data['seller_city']['limit'], index = None)
        data['seller_state']['input'] = left_column.selectbox(label="State", options=data['seller_state']['limit'], index = None)


        # New data
        data['seller_id']['changed'] = right_column.text_input(label="ID", max_chars=32)
        data['seller_zip_code_prefix']['changed'] = right_column.text_input(label="zip_code_prefix", key = "seller_zip_code_prefix", max_chars=5)
        data['seller_city']['changed'] = right_column.text_input(label="City", key = "seller_city", max_chars=32)
        data['seller_state']['changed'] = right_column.text_input(label="State", key = "seller_state", max_chars=2)
    # with tab_customers:
    if selected_dataset == "customers":
        data = { 
                "name": "customers",
                "customer_id": {"changed": None, "input": None, "limit": None},
                "customer_unique_id": {"changed": None, "input": None, "limit": None},
                "customer_zip_code_prefix": {"changed": None, "input": None, "limit": None},
                "customer_city": {"changed": None, "input": None, "limit": None},
                "customer_state": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['customer_id']['limit'] = connector.get_single_unique(data['name'], "customer_id").astype(str).tolist()
        data['customer_unique_id']['limit'] = connector.get_single_unique(data['name'], "customer_unique_id").astype(str).tolist()
        data['customer_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['customer_city']['limit'] = connector.get_single_unique(data['name'], 'customer_city').astype(str).tolist()
        data['customer_state']['limit'] = connector.get_single_unique(data['name'], 'customer_state').astype(str).tolist()

        # Original data
        data['customer_id']['input'] = left_column.selectbox(label="ID", options=data['customer_id']['limit'], index = None)
        data['customer_unique_id']['input'] = left_column.selectbox(label="Unique_ID", options=data['customer_unique_id']['limit'], index = None)
        data['customer_zip_code_prefix']['input'] = left_column.selectbox(label="zip_code", options=data['customer_zip_code_prefix']['limit'], index = None, key="delete_customer_zip_code_prefix")
        data['customer_city']['input'] = left_column.selectbox(label="City", options=data['customer_city']['limit'], index = None)
        data['customer_city']['input'] = left_column.selectbox(label="State", options=data['customer_state']['limit'], index = None)

        # New data
        data['customer_id']['changed'] = right_column.text_input(label="Account ID", max_chars=32)
        data['customer_unique_id']['changed'] = right_column.text_input(label="Unique ID", max_chars=32)
        data['customer_zip_code_prefix']['changed'] = right_column.text_input(label="zip_code_prefix", key = "customer_zip_code_prefix", max_chars=5)
        data['customer_city']['changed'] = right_column.text_input(label="City", key = "customer_city", max_chars=32)
        data['customer_state']['changed'] = right_column.text_input(label="State", key = "customer_state", max_chars=2)
    # with tab_orders:
    if selected_dataset == "orders":
        data = {
                "name": "orders",
                "order_id": {"changed": None, "input": None, "limit": None},
                "customer_id": {"changed": None, "input": None, "limit": None},
                "order_status": {"changed": None, "input": None, "limit": None},
                "order_purchase_timestamp": {"changed": None, "input": None, "limit": None},
                "order_approved_at": {"changed": None, "input": None, "limit": None},
                "order_delivered_carrier_date": {"changed": None, "input": None, "limit": None},
                "order_delivered_customer_date": {"changed": None, "input": None, "limit": None},
                "order_estimated_delivery_date": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(data['name'], "order_id").astype(str).tolist()
        data['customer_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['order_status']['limit'] = connector.get_single_unique(data['name'], "order_status").astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_purchase_timestamp')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_purchase_timestamp']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_approved_at')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_approved_at']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_carrier_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_delivered_carrier_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_customer_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_delivered_customer_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_estimated_delivery_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['order_estimated_delivery_date']['limit'] = (tmp_min, tmp_max)


        # Original data
        data['order_id']['input'] = left_column.selectbox(label="Order ID", options=data['order_id']['limit'], index=None)
        data['customer_id']['input'] = left_column.selectbox(label="Customer ID", options=data['customer_id']['limit'], index=None)
        data['order_status']['input'] = left_column.selectbox(label="Status", options=data['order_status']['limit'], index=None)
        data['order_purchase_timestamp']['input'] = left_column.slider("Purchase time", min_value=data['order_purchase_timestamp']['limit'][0], max_value=data['order_purchase_timestamp']['limit'][1], value=data['order_purchase_timestamp']['limit'], step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_approved_at']['input'] = left_column.slider("Approved time", min_value=data['order_approved_at']['limit'][0], max_value=data['order_approved_at']['limit'][1], value=data['order_approved_at']['limit'], step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_delivered_carrier_date']['input'] = left_column.slider("Delivered Carrier Date", min_value=data['order_delivered_carrier_date']['limit'][0], max_value=data['order_delivered_carrier_date']['limit'][1], value=data['order_delivered_carrier_date']['limit'], step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_delivered_customer_date']['input'] = left_column.slider("Delivered Customer Date", min_value=order_delivered_customer_date_min, max_value=order_delivered_customer_date_max, value=(order_delivered_customer_date_min, order_delivered_customer_date_max), step=config.time['step']['second'], format=config.time['format']['long'])
        data['order_estimated_delivery_date']['input'] = left_column.slider("Estimated Delivery Date", min_value=data['order_estimated_delivery_date']['limit'][0], max_value=data['order_estimated_delivery_date']['limit'][1], value=data['order_estimated_delivery_date']['limit'], step=config.time['step']['day'], format=config.time['format']['short'])

        # New data
        data['order_id']['changed'] = right_column.text_input(label="Order ID", max_chars=32)
        data['customer_id']['changed'] = right_column.text_input(label="Customer ID", max_chars=32)
        data['order_status']['changed'] = right_column.selectbox(label="Status", options=data['order_status']['limit'], index=None)
        key = "Purchase At"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = right_column.time_input(f"{key} - Time")
        data['order_purchase_timestamp']['changed'] = (tmp_date, tmp_time)
        key = "Approved At"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = right_column.time_input(f"{key} - Time")
        data['order_approved_at']['changed'] = (tmp_date, tmp_time)
        key = "Carrier's Delivered"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = right_column.time_input(f"{key} - Time")
        data['order_delivered_carrier_date']['changed'] = (tmp_date, tmp_time)
        key = "Customer's Delivered"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = right_column.time_input(f"{key} - Time")
        data['order_delivered_customer_date']['changed'] = (tmp_date, tmp_time)
        key = "Estimated Delivered"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = None
        data['order_estimated_delivery_date']['changed'] = (tmp_date, tmp_time)
    # with tab_order_payments:
    if selected_dataset == "order_payments":
        data = {
                "name": "order_payments", 
                "order_id": {"changed": None, "input": None, "limit": None}, 
                "payment_sequential": {"changed": None, "input": None, "limit": None}, 
                "payment_type": {"changed": None, "input": None, "limit": None},
                "payment_installments": {"changed": None, "input": None, "limit": None},
                "payment_value": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['payment_sequential']['limit'] = connector.get_single_unique(data['name'], 'payment_sequential').astype(str).tolist()
        data['payment_type']['limit'] = connector.get_single_unique(data['name'], 'payment_type').astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_installments')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['payment_installments']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_value')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['payment_value']['limit'] = (tmp_min, tmp_max)

        # Original data
        data['order_id']['input'] = left_column.selectbox(label = "Order ID", options = data['order_id']['limit'], index = None, key="update_order_payments_order_id")
        data['payment_sequential']['input'] = left_column.selectbox(label = "Payment Sequential", options=data['payment_sequential']['limit'], index=None)
        data['payment_type']['input'] = left_column.selectbox(label="Payment Type", options=data['payment_type']['limit'], index = None)
        data['payment_installments']['input'] = left_column.selectbox(label="Payment Installments", options=data['payment_installments']['limit'], index = None)
        data['payment_value']['input'] = left_column.slider("Payment Value", min_value=data['payment_value']['limit'][0], max_value=data['payment_value']['limit'][1], value=data['payment_value']['limit'], step=config.concurrency)

        # New data
        data['order_id']['changed'] = right_column.selectbox(label="Order ID", options=data['order_id']['limit'], index=None)
        data['payment_sequential']['changed'] = right_column.number_input(label="Payment Sequential", step=1, min_value=1)
        data['payment_type']['changed'] = right_column.text_input(label="Payment Type", max_chars=20)
        data['payment_installments']['changed'] = right_column.number_input(label="Payment Installments", step=1, min_value=0)
        data['payment_value']['changed'] = right_column.number_input(label="payment_value", step=0.01, min_value=0.01)
    # with tab_products:
    if selected_dataset == "products":
        data = {
                "name": "products",
                "product_id": {"changed": None, "input": None, "limit": None},
                "product_category_name": {"changed": None, "input": None, "limit": None},
                "product_name_length": {"changed": None, "input": None, "limit": None},
                "product_description_length": {"changed": None, "input": None, "limit": None},
                "product_photos_qty": {"changed": None, "input": None, "limit": None},
                "product_weight_g": {"changed": None, "input": None, "limit": None},
                "product_length_cm": {"changed": None, "input": None, "limit": None},
                "product_height_cm": {"changed": None, "input": None, "limit": None},
                "product_width_cm": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['product_id']['limit'] = connector.get_single_unique(data['name'], 'product_id').astype(str).tolist()
        data['product_category_name']['limit'] = connector.get_single_unique(data['name'], 'product_category_name').astype(str).tolist()
        key = 'product_name_length'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_description_length'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_photos_qty'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_weight_g'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_length_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_height_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)
        key = 'product_width_cm'
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data[key]['limit'] = (tmp_min, tmp_max)

        # Original data
        data['product_id']['input'] = left_column.selectbox(label="Product ID", options = data['product_id']['limit'], index=None)
        data['product_category_name']['input'] = left_column.selectbox(label="Product Category", options=data['product_category_name']['limit'], index = None)
        data['product_name_length']['input'] = left_column.slider(label="Name Length", min_value=data['product_name_length']['limit'][0], max_value=data['product_name_length']['limit'][1], value=data['product_name_length']['limit'], step=config.integer)
        data['product_description_length']['input'] = left_column.slider(label="Description Length", min_value=data['product_description_length']['limit'][0], max_value=data['product_description_length']['limit'][1], value=data['product_description_length']['limit'], step=config.integer)
        data['product_photos_qty']['input'] = left_column.slider(label="Photo Quantity", min_value=data['product_photos_qty']['limit'][0], max_value=data['product_photos_qty']['limit'][1], value=data['product_photos_qty']['limit'], step=config.integer)
        data['product_weight_g']['input'] = left_column.slider(label="Weight in gram", min_value=data['product_weight_g']['limit'][0], max_value=data['product_weight_g']['limit'][1], value=data['product_weight_g']['limit'], step=config.integer)
        data['product_length_cm']['input'] = left_column.slider(label="Length in cm", min_value=data['product_length_cm']['limit'][0], max_value=data['product_length_cm']['limit'][1], value=data['product_length_cm']['limit'], step=config.integer)
        data['product_height_cm']['input'] = left_column.slider(label="Height in cm", min_value=data['product_height_cm']['limit'][0], max_value=data['product_height_cm']['limit'][1], value=data['product_height_cm']['limit'], step=config.integer)
        data['product_width_cm']['input'] = left_column.slider(label="Width in cm", min_value=data['product_width_cm']['limit'][0], max_value=data['product_width_cm']['limit'][1], value=data['product_width_cm']['limit'], step=config.integer)

        # New data
        data['product_id']['changed'] = right_column.text_input(label="Product ID", max_chars=32)
        data['product_category_name']['changed'] = right_column.text_input(label="Category Name", max_chars=64)
        data['product_name_length']['changed'] = right_column.number_input(label="Product Name Length", step=1, min_value=1)
        data['product_description_length']['changed'] = right_column.number_input(label="Product Description Length", step=1, min_value=1)
        data['product_photos_qty']['changed'] = right_column.number_input(label="Photo qty", step=1, min_value=1)
        data['product_weight_g']['changed'] = right_column.number_input(label="Weight (g)", step=1, min_value=1)
        data['product_length_cm']['changed'] = right_column.number_input(label="Length (cm)", step=1, min_value=1)
        data['product_height_cm']['changed'] = right_column.number_input(label="Height (cm)", step=1, min_value=1)
        data['product_width_cm']['changed'] = right_column.number_input(label="Width (cm)", step=1, min_value=1)
    # with tab_order_items:
    if selected_dataset == "order_items":
        data = {
                "name": "order_items",
                "order_id": {"changed": None, "input": None, "limit": None},
                "order_item_id": {"changed": None, "input": None, "limit": None},
                "product_id": {"changed": None, "input": None, "limit": None},
                "seller_id": {"changed": None, "input": None, "limit": None},
                "shipping_limit_date": {"changed": None, "input": None, "limit": None},
                "price": {"changed": None, "input": None, "limit": None},
                "freight_value": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0

        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        data['order_item_id']['limit'] = connector.get_single_unique(data['name'], 'order_item_id').astype(str).tolist()
        data['product_id']['limit'] = connector.get_single_unique(referencing_information['table'][1], referencing_information['key'][1]).astype(str).tolist()
        data['seller_id']['limit'] = connector.get_single_unique(referencing_information['table'][2], referencing_information['key'][2]).astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'shipping_limit_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['shipping_limit_date']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'price')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['price']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'freight_value')
        tmp_min = float(tmp_min.iloc[0])
        tmp_max = float(tmp_max.iloc[0])
        data['freight_value']['limit'] = (tmp_min, tmp_max)

        # Original data
        data['order_id']['input'] = left_column.selectbox(label="Order ID", options = data['order_id']['limit'], index=None, key="update_order_items_order_id")
        data['order_item_id']['input'] = left_column.selectbox(label="Order Item ID", options=data['order_item_id']['limit'], index=None)
        data['product_id']['input'] = left_column.selectbox(label="Product ID", options=data['product_id']['limit'], index=None, key="order_items-product_id")
        data['seller_id']['input'] = left_column.selectbox(label="Seller ID", options=data['seller_id']['limit'], index=None)
        data['shipping_limit_date']['input'] = left_column.slider("Shipping Limit Date", min_value=data['shipping_limit_date']['limit'][0], max_value=data['shipping_limit_date']['limit'][1], value=data['shipping_limit_date']['limit'], step=config.time['step']['day'], format=config.time['format']['short'])
        data['price']['input'] = left_column.slider("Price", min_value=data['price']['limit'][0], max_value=data['price']['limit'][1], value=data['price']['limit'], step=config.concurrency)
        data['freight_value']['input'] = left_column.slider("Freight Value", min_value=data['freight_value']['limit'][0], max_value=data['freight_value']['limit'][1], value=data['freight_value']['limit'], step=config.concurrency)

        # New data
        data['order_id']['changed'] = right_column.selectbox(label="Order ID", options=data['order_id']['limit'], index=None, key = "order_items_order_id")
        data['order_item_id']['changed'] = right_column.text_input(label="Item ID", max_chars=32)
        data['product_id']['changed'] = right_column.selectbox(label="Product ID", options=data['product_id']['limit'], index=None)
        data['seller_id']['changed'] = right_column.selectbox(label="Seller ID", options=data['seller_id']['limit'], index=None)
        key = "Shipping Limit"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = right_column.time_input(f"{key} - Time")
        data['shipping_limit_date']['changed'] = (tmp_date, tmp_time)
        data['price']['changed'] = right_column.number_input(label="Order Price", step=0.01, min_value=0.01)
        data['freight_value']['changed'] = right_column.number_input(label="Freight Price", step=0.01, min_value=0.01)
    # with tab_order_reviews:
    if selected_dataset == "order_reviews":
        data = {
                "name": "order_reviews",
                "review_id": {"changed": None, "input": None, "limit": None},
                "order_id": {"changed": None, "input": None, "limit": None},
                "review_score": {"changed": None, "input": None, "limit": None},
                "review_creation_date": {"changed": None, "input": None, "limit": None}
                }
        left_column, right_column = st.columns(2)
        left_column.write("Original Data")
        right_column.write("Adjusted Data")

        # Load limitation
        referencing_information = st.session_state.map['referencing_keys'][data['name']]
        referencing_information_length = len(referencing_information) if referencing_information is not None else 0


        data['review_id']['limit'] = connector.get_single_unique(data['name'], 'review_id').astype(str).tolist()
        data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_score')
        tmp_min = int(tmp_min.iloc[0])
        tmp_max = int(tmp_max.iloc[0])
        data['review_score']['limit'] = (tmp_min, tmp_max)
        tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_creation_date')
        tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
        tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
        data['review_creation_date']['limit'] = (tmp_min, tmp_max)

        # Original data
        data['review_id']['input'] = left_column.selectbox("Review ID", options=data['review_id']['limit'], index=None)
        data['order_id']['input'] = left_column.selectbox("order_id", options=data['order_id']['limit'], index=None)
        data['review_score']['input'] = left_column.slider("Review Score", min_value=data['review_score']['limit'][0], max_value=data['review_score']['limit'][1], value = data['review_score']['limit'], step=config.integer)
        data['review_creation_date']['input'] = left_column.slider("Review Creation Date", min_value=data['review_creation_date']['limit'][0], max_value=data['review_creation_date']['limit'][1], value=data['review_creation_date']['limit'], step=config.time['step']['day'], format=config.time['format']['short'])

        # New data
        data['review_id']['changed'] = right_column.text_input(label="Review ID", max_chars=32)
        data['order_id']['changed'] = right_column.selectbox(label="Order ID", options=data['order_id']['limit'], index=None, key = "order_reviews_order_id")
        data['review_score']['changed'] = right_column.number_input(label="Review Score", step=1, min_value=1, max_value=5)
        key = "Review Creation"
        tmp_date = right_column.date_input(f"{key} - Date")
        tmp_time = None
        data['review_creation_date']['changed'] = (tmp_date, tmp_time)

    # Search target
    if data is None:
        disable_button = True
    else:
        special = ['name']
        keys = [key for key in data.keys() if key not in special]
        foreign_keys = st.session_state.map['referencing_keys'][data['name']] # dict with parallel list
        primary_keys = st.session_state.map['primary_keys'][data['name']] # list

        # Over original data
        # search target exists
        inputted_primary_key = True
        for key in primary_keys:
            if (data[key]['input'] is None) or (data[key]['input'] == ""):
                inputted_primary_key = False

        if not(inputted_primary_key):
            disable_button = True
            left_column.error(f"Please at least select value for ({', '.join(primary_keys)})")
        else:
            record_query = f"SELECT * FROM {data['name']} WHERE "
            for key in keys:
                value = data[key]['input']
                if value is not None:
                    slice = utils.search_preprocessing(key, value)
                    if slice is None:
                        left_column.error(f"Meet an unexpected value {key}:{value}")
                    else:
                        record_query += slice
                        record_query += " AND "
            record_query = record_query[:-4]
            record_query += ";"

            record_result = connector.query(record_query)
            if len(record_result) == 0:
                disable_button = True
                left_column.error("Searched record doesn't exists with filters")

        # Over New data
        # search whether value for foreign key exists
        exist_foreign_key = True
        if foreign_keys is not None:
            for idx in range(len(foreign_keys['local'])):
                foreign_query = f"SELECT * FROM {foreign_keys['table'][idx]} WHERE {foreign_keys['key'][idx]} = '{data[foreign_keys['local'][idx]]['changed']}';" # foreign keys are zip_code or xx_id

                foreign_result = connector.query(foreign_query)
                if len(foreign_result) == 0:
                    exist_foreign_key = False;
                    right_column.error(f"You didn't input an existed value about {foreign_keys['local'][idx]}")

        # search whether value for primary key exists
        exist_primary_key = False
        inputted_primary_key = True
        for key in primary_keys:
            if (data[key]['changed'] is None) or (data[key]['changed'] == ""):
                inputted_primary_key = False
        if not(inputted_primary_key):
            exist_primary_key = True
            right_column.error(f"Unique information you need to input: ({', '.join(primary_keys)})")
        else:
            primary_query = f"SELECT * FROM {data['name']} WHERE "
            for key in primary_keys:
                primary_query += f"{key} = '{data[key]['changed']}' AND "
            primary_query = primary_query[:-4]
            primary_query += ";"
            primary_result = connector.query(primary_query)
            if len(primary_result) != 0:
                exist_primary_key = True
                right_column.error(f"You input some value or value pair that exists in table {data['name']} over primary key ({', '.join(primary_keys)}). ")


        if not(exist_foreign_key):
            disable_button = True
        elif (exist_primary_key):
            disable_button = True

        # Special check
        if data['name'] == "orders":
            time_purchase= data['order_purchase_timestamp']['changed']
            time_purchase= datetime.combine(time_purchase[0], time_purchase[1])
            time_approved= data['order_approved_at']['changed']
            time_approved= datetime.combine(time_approved[0],time_approved[1])
            time_delivered_carrier= data['order_delivered_carrier_date']['changed']
            time_delivered_carrier= datetime.combine(time_delivered_carrier[0],time_delivered_carrier[1])
            time_delivered_customer= data['order_delivered_customer_date']['changed']
            time_delivered_customer= datetime.combine(time_delivered_customer[0],time_delivered_customer[1])
            time_estimated= data['order_estimated_delivery_date']['changed']
            time_estimated= datetime.combine(time_estimated[0],datetime.min.time())
            if not ((time_purchase < time_approved and time_approved < time_delivered_carrier and time_delivered_carrier < time_delivered_customer) and (time_purchase < time_approved < time_estimated)):
                right_column.error("Your input about multiple recorded time is impossible generally.")
                disable_button = True

        for key in keys:
            if "_id" in key:
                if len(data[key]['changed'].strip()) != 32:
                    right_column.error("You must make ID 32 characters long")
                    disable_button = True
                if " " in data[key]['changed']:
                    right_column.error("No space in ID")
                    disable_button = True

                for i in range(len(data[key]['changed'])):
                    if (data[key]['changed'][i] not in string.ascii_lowercase) and (data[key]['changed'][i] not in string.digits):
                        right_column.error("ID can only be constructed by letters in lower case or digits")
                        disable_button = True
                        break

        for key in keys:
            if "state" in key:
                if len(data[key]['changed'].strip()) != 2:
                    right_column.error("You must make state 2 characters long without space")
                    disable_button = True

                for i in range(len(data[key]['changed'])):
                    if data[key]['changed'][i] not in string.ascii_uppercase:
                        right_column.error("Sate Code can only be constructed by letters in upper case")
                        disable_button = True
                        break

        for key in keys:
            if "city" in key:
                for i in range(len(data[key]['changed'])):
                    if data[key]['changed'][i] in string.digits:
                        right_column.error("City name shouldn't includes digit.")
                        disable_button = True
                        break

        for key in keys:
            if "zip" in key:
                if ((len(data[key]['changed'].strip()) != 4) or (len(data[key]['changed'].strip()) != 5)):
                    right_column.error("You must make Zip Code 4 characters or 5 characters long without space")
                    disable_button = True

                for i in range(len(data[key]['changed'])):
                    if data[key]['changed'][i] not in string.digits:
                        right_column.error("zip code can only be constructed by digits")
                        disable_button = True
                        break





    dialog_submit = st.button("Submit", disabled=disable_button)
    if dialog_submit:
        changed_side = ""
        for key in keys:
            tmp_key = key
            tmp_value = data[key]['changed']
            if tmp_value is not None:
                changed_side += "{tmp_key} = {utils.input_preprocessing(tmp_value)}, "
        # remove last comma
        changed_side = changed_side[:-2]

        original_side = ""
        for key in primary_keys:
            slice = utils.search_preprocessing(key, data[key]['input'])
            # no protection for search exact ID only.
            if slice is None:
                disable_button = True
                st.error("Error! Target primary key disappeared")
            original_side += slice
            original_side += " AND "
        # delete last end
        original_side = original_side [:-4]



        final_query = f"UPDATE {data['name']} SET {changed_side} WHERE {original_side};"
        connector.execute(final_query)
        connector.checkpoint_add(checkpoint="prepared_name")
        st.session_state.stack.append(prepared_name)
        st.session_state.data_changed = True
        st.rerun()

if buttom_table_add:
    add_data()


# Solving behavior: Delete
if buttom_table_delete:
    delete_data()

# Solving behavior: Update
if buttom_table_update:
    update_data()

if change_confirm:
    st.session_state.stack = []
    st.session_state.connector.commit()
    st.session_state.connector.start_transaction()

if change_rollback:
    checkpoint = st.session_state.stack.pop()
    st.session_state.connector.checkpoint_rollback(checkpoint)
























# # Load input data
#     # Get Input
#     with tab_geolocation:
#         # Prepare storage variable
#         data = {
#                 "name": "geolocation",
#                 "geolocation_zip_code_prefix": {"input": None, "limit": None},
#                 "geolocation_lat": {"input": None, "limit": None},
#                 "geolocation_lng": {"input": None, "limit": None},
#                 "geolocation_city": {"input": None, "limit": None},
#                 "geolocation_state": {"input": None, "limit": None},
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['geolocation_zip_code_prefix']['limit'] = connector.get_single_unique(data['name'], 'geolocation_zip_code_prefix').astype(int).sort_values().astype(str).tolist()
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lat')
#         tmp_min = float(tmp_min.iloc[0])
#         tmp_max = float(tmp_max.iloc[0])
#         data['geolocation_lat']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'geolocation_lng')
#         tmp_min = float(tmp_min.iloc[0])
#         tmp_max = float(tmp_max.iloc[0])
#         data['geolocation_lng']['limit'] = (tmp_min, tmp_max)
#         data['geolocation_city']['limit'] = connector.get_single_unique(data['name'], 'geolocation_city').astype(str).tolist()
#         data['geolocation_state']['limit'] = connector.get_single_unique(data['name'], 'geolocation_state').astype(str).tolist()
# 
#         # Input widgets # no wrong input type handling
#         data['geolocation_zip_code_prefix']['input'] = st.text_input(label="zip_code_prefix")
#         data['geolocation_lat']['input'] = st.number_input(label="Latitude", step=0.01)
#         data['geolocation_lng']['input'] = st.number_input(label="Longtitude", step=0.01)
#         data['geolocation_city']['input'] = st.text_input(label="City")
#         data['geolocation_state']['input'] = st.text_input(label="State")
#     with tab_sellers:
#         data = {
#                 "name": "sellers",
#                 "seller_id": {"input": None, "limit": None},
#                 "seller_zip_code_prefix": {"input": None, "limit": None},
#                 "seller_city": {"input": None, "limit": None},
#                 "seller_state": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['seller_id']['limit'] = connector.get_single_unique(data['name'], "seller_id").astype(str).tolist()
#         data['seller_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
#         data['seller_city']['limit'] = connector.get_single_unique(data['name'], 'seller_city').astype(str).tolist()
#         data['seller_state']['limit'] = connector.get_single_unique(data['name'], 'seller_state').astype(str).tolist()
# 
#         # Input widgets # no wrong input type handling
#         data['seller_id']['input'] = st.text_input(label="ID")
#         data['seller_zip_code_prefix']['input'] = st.text_input(label="zip_code_prefix", key = "seller_zip_code_prefix")
#         data['seller_city']['input'] = st.text_input(label="City", key = "seller_city")
#         data['seller_state']['input'] = st.text_input(label="State", key = "seller_state")
#     with tab_customers:
#         data = { 
#                 "name": "customers",
#                 "customer_id": {"input": None, "limit": None},
#                 "customer_unique_id": {"input": None, "limit": None},
#                 "customer_zip_code_prefix": {"input": None, "limit": None},
#                 "customer_city": {"input": None, "limit": None},
#                 "customer_state": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['customer_id']['limit'] = connector.get_single_unique(data['name'], "customer_id").astype(str).tolist()
#         data['customer_unique_id']['limit'] = connector.get_single_unique(data['name'], "customer_unique_id").astype(str).tolist()
#         data['customer_zip_code_prefix']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
#         data['customer_city']['limit'] = connector.get_single_unique(data['name'], 'customer_city').astype(str).tolist()
#         data['customer_state']['limit'] = connector.get_single_unique(data['name'], 'customer_state').astype(str).tolist()
# 
#         # Input widgets # no wrong input type handling
#         data['customer_id']['input'] = st.text_input(label="Account ID")
#         data['customer_unique_id']['input'] = st.text_input(label="Unique ID")
#         data['customer_zip_code_prefix']['input'] = st.text_input(label="zip_code_prefix", key = "customer_zip_code_prefix")
#         data['customer_city']['input'] = st.text_input(label="City", key = "customer_city")
#         data['customer_state']['input'] = st.text_input(label="State", key = "customer_state")
#     with tab_orders:
#         data = {
#                 "name": "orders",
#                 "order_id": {"input": None, "limit": None},
#                 "customer_id": {"input": None, "limit": None},
#                 "order_status": {"input": None, "limit": None},
#                 "order_purchase_timestamp": {"input": None, "limit": None},
#                 "order_approved_at": {"input": None, "limit": None},
#                 "order_delivered_carrier_date": {"input": None, "limit": None},
#                 "order_delivered_customer_date": {"input": None, "limit": None},
#                 "order_estimated_delivery_date": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['order_id']['limit'] = connector.get_single_unique(data['name'], "order_id").astype(str).tolist()
#         data['customer_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
#         data['order_status']['limit'] = connector.get_single_unique(data['name'], "order_status").astype(str).tolist()
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_purchase_timestamp')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['order_purchase_timestamp']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_approved_at')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['order_approved_at']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_carrier_date')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['order_delivered_carrier_date']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_delivered_customer_date')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['order_delivered_customer_date']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'order_estimated_delivery_date')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['order_estimated_delivery_date']['limit'] = (tmp_min, tmp_max)
# 
# 
#         # Input widgets # no wrong input type handling
#         data['order_id']['input'] = st.text_input(label="Order ID")
#         data['customer_id']['input'] = st.text_input(label="Customer ID")
#         data['order_status']['input'] = st.selectbox(label="Status", options=data['order_status']['limit'], index=None)
#         key = "Purchase At"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = st.time_input(f"{key} - Time")
#         data['order_purchase_timestamp']['input'] = (tmp_date, tmp_time)
#         key = "Approved At"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = st.time_input(f"{key} - Time")
#         data['order_approved_at']['input'] = (tmp_date, tmp_time)
#         key = "Carrier's Delivered"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = st.time_input(f"{key} - Time")
#         data['order_delivered_carrier_date']['input'] = (tmp_date, tmp_time)
#         key = "Customer's Delivered"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = st.time_input(f"{key} - Time")
#         data['order_delivered_customer_date']['input'] = (tmp_date, tmp_time)
#         key = "Estimated Delivered"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = None
#         data['order_delivered_customer_date']['input'] = (tmp_date, tmp_time)
#     with tab_order_payments:
#         data = {
#                 "name": "order_payments", 
#                 "order_id": {"input": None, "limit": None}, 
#                 "payment_sequential": {"input": None, "limit": None}, 
#                 "payment_type": {"input": None, "limit": None},
#                 "payment_installments": {"input": None, "limit": None},
#                 "payment_value": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
#         data['payment_sequential']['limit'] = connector.get_single_unique(data['name'], 'payment_sequential').astype(str).tolist()
#         data['payment_type']['limit'] = connector.get_single_unique(data['name'], 'payment_type').astype(str).tolist()
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_installments')
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data['payment_installments']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'payment_value')
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data['payment_value']['limit'] = (tmp_min, tmp_max)
# 
#         # Input widgets # no wrong input type handling
#         data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None)
#         data['payment_sequential']['input'] = st.text_input(label="Payment Sequential")
#         data['payment_type']['input'] = st.text_input(label="Payment Type")
#         data['payment_installments']['input'] = st.number_input(label="Payment Installments", step=1, min_value=0)
#         data['payment_value']['input'] = st.number_input(label="payment_value", step=0.01)
#     with tab_products:
#         data = {
#                 "name": "products",
#                 "product_id": {"input": None, "limit": None},
#                 "product_category_name": {"input": None, "limit": None},
#                 "product_name_length": {"input": None, "limit": None},
#                 "product_description_length": {"input": None, "limit": None},
#                 "product_photos_qty": {"input": None, "limit": None},
#                 "product_weight_g": {"input": None, "limit": None},
#                 "product_length_cm": {"input": None, "limit": None},
#                 "product_height_cm": {"input": None, "limit": None},
#                 "product_width_cm": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['product_id']['limit'] = connector.get_single_unique(data['name'], 'product_id').astype(str).tolist()
#         data['product_category_name']['limit'] = connector.get_single_unique(data['name'], 'product_category_name').astype(str).tolist()
#         key = 'product_name_length'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
#         key = 'product_description_length'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
#         key = 'product_photos_qty'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
#         key = 'product_weight_g'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
#         key = 'product_length_cm'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
#         key = 'product_height_cm'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
#         key = 'product_width_cm'
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], key)
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data[key]['limit'] = (tmp_min, tmp_max)
# 
#         # Input widgets # no wrong input type handling
#         data['product_id']['input'] = st.text_input(label="Product ID")
#         data['product_category_name']['input'] = st.text_input(label="Category Name")
#         data['product_name_length']['input'] = st.number_input(label="Product Name Length", step=1, min_value=1)
#         data['product_description_length']['input'] = st.number_input(label="Product Description Length", step=1, min_value=1)
#         data['product_photos_qty']['input'] = st.number_input(label="Photo qty", step=1, min_value=1)
#         data['product_weight_g']['input'] = st.number_input(label="Weight (g)", step=1, min_value=1)
#         data['product_length_cm']['input'] = st.number_input(label="Length (cm)", step=1, min_value=1)
#         data['product_height_cm']['input'] = st.number_input(label="Height (cm)", step=1, min_value=1)
#         data['product_width_cm']['input'] = st.number_input(label="Width (cm)", step=1, min_value=1)
#     with tab_order_items:
#         data = {
#                 "name": "order_items",
#                 "order_id": {"input": None, "limit": None},
#                 "order_item_id": {"input": None, "limit": None},
#                 "product_id": {"input": None, "limit": None},
#                 "seller_id": {"input": None, "limit": None},
#                 "shipping_limit_date": {"input": None, "limit": None},
#                 "price": {"input": None, "limit": None},
#                 "freight_value": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
#         data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
#         data['order_item_id']['limit'] = connector.get_single_unique(data['name'], 'order_item_id').astype(str).tolist()
#         data['product_id']['limit'] = connector.get_single_unique(referencing_information['table'][1], referencing_information['key'][1]).astype(str).tolist()
#         data['seller_id']['limit'] = connector.get_single_unique(referencing_information['table'][2], referencing_information['key'][2]).astype(str).tolist()
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'shipping_limit_date')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['shipping_limit_date']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'price')
#         tmp_min = float(tmp_min.iloc[0])
#         tmp_max = float(tmp_max.iloc[0])
#         data['price']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'freight_value')
#         tmp_min = float(tmp_min.iloc[0])
#         tmp_max = float(tmp_max.iloc[0])
#         data['freight_value']['limit'] = (tmp_min, tmp_max)
# 
#         # Input widgets # no wrong input type handling
#         data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None, key = "order_items_order_id")
#         data['order_item_id']['input'] = st.text_input(label="Item ID")
#         data['product_id']['input'] = st.selectbox(label="Product ID", options=data['product_id']['limit'], index=None)
#         data['seller_id']['input'] = st.selectbox(label="Seller ID", options=data['seller_id']['limit'], index=None)
#         key = "Shipping Limit"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = st.time_input(f"{key} - Time")
#         data['shipping_limit_date']['input'] = (tmp_date, tmp_time)
#         data['price']['input'] = st.number_input(label="Order Price", step=0.01, min_value=0.01)
#         data['freight_value']['input'] = st.number_input(label="Freight Price", step=0.01, min_value=0.01)
#     with tab_order_reviews:
#         data = {
#                 "name": "order_reviews",
#                 "review_id": {"input": None, "limit": None},
#                 "order_id": {"input": None, "limit": None},
#                 "review_score": {"input": None, "limit": None},
#                 "review_creation_date": {"input": None, "limit": None}
#                 }
# 
#         # Load limitation
#         referencing_information = st.session_state.map['referencing_keys'][data['name']]
#         referencing_information_length = len(referencing_information) if referencing_information is not None else 0
# 
# 
#         data['review_id']['limit'] = connector.get_single_unique(data['name'], 'review_id').astype(str).tolist()
#         data['order_id']['limit'] = connector.get_single_unique(referencing_information['table'][0], referencing_information['key'][0]).astype(str).tolist()
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_score')
#         tmp_min = int(tmp_min.iloc[0])
#         tmp_max = int(tmp_max.iloc[0])
#         data['review_score']['limit'] = (tmp_min, tmp_max)
#         tmp_min, tmp_max = connector.get_single_min_max(data['name'], 'review_creation_date')
#         tmp_min = pd.to_datetime(tmp_min.loc[0]).to_pydatetime()
#         tmp_max = pd.to_datetime(tmp_max.loc[0]).to_pydatetime()
#         data['review_creation_date']['limit'] = (tmp_min, tmp_max)
# 
#         # Input widgets # no wrong input type handling
#         data['review_id']['input'] = st.text_input(label="Review ID")
#         data['order_id']['input'] = st.selectbox(label="Order ID", options=data['order_id']['limit'], index=None, key = "order_reviews_order_id")
#         data['review_score']['input'] = st.number_input(label="Review Score", step=1, min_value=1, max_value=5)
#         key = "Review Creation"
#         tmp_date = st.date_input(f"{key} - Date")
#         tmp_time = None
#         data['review_creation_date']['input'] = (tmp_date, tmp_time)





















