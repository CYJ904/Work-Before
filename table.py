from contextlib import ContextDecorator
from os import sep, stat
from altair import Order
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

# Solve error about conflict renames in SELECT

# Error pairs:
# geolocation, sellers
# geolocation, customers
# geolocation, orders
# geolocation, order_payments
# geolocation, products
# geolocation, order_items
# geolocation, order_reviews
# sellers, customers
# sellers, orders
# sellers, order_payments
# sellers, products
# sellers, order_reviews
# customers, order_payments
# customers, products
# customers, order_items
# customers, order_reviews
# order_payments, products
# order_payments, order_items
# order_payments, order_reviews
# products, order_reviews

st.set_page_config(layout="wide", initial_sidebar_state="auto")
with open('config.yaml', 'r') as file:
    database_config = yaml.safe_load(file)


if "connector" not in st.session_state:
    connection = st.connection('source', type='sql')
    st.session_state.connector = utils.Connector(connection)

connector = st.session_state.connector

left_column, right_column = st.columns(spec=config.page_split, gap="small", vertical_alignment="top")
# avoid overall scrolling
left_column = left_column.container(height=config.page_height, border=config.show_all_edge)
right_column = right_column.container(height=config.page_height, border=config.show_all_edge)
right_column.write("Table")


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

            geolocation_latitude_min, geolocation_latitude_max =connector.get_single_min_max(name, "geolocation_lat")
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


# Needs a way to pass filter and data change between table page and display page
# SQL search with filter
# if filter_area_buttom:
#     right_column.write(filters)
#     right_column.write(filters_geolocation)
#     right_column.write(filters_sellers)
#     right_column.write(filters_customers)
#     right_column.write(filters_orders)
#     right_column.write(filters_order_payments)
#     right_column.write(filters_products)
#     right_column.write(filters_order_items)
#     right_column.write(filters_order_reviews)

# right_column.write(filters)
# right_column.write(filters_geolocation)
# right_column.write(filters_sellers)
# right_column.write(filters_customers)
# right_column.write(filters_orders)
# right_column.write(filters_order_payments)
# right_column.write(filters_products)
# right_column.write(filters_order_items)
# right_column.write(filters_order_reviews)

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

    if "result" not in st.session_state:
        st.session_state.result = ""

    if filter_area_buttom:
        # result = connector.query(query)
        # st.session_state.result = result
        st.session_state.result = query

    right_column.write(st.session_state.result)

# Solving behavior: Add 

# st.dialog
# st.tab

stack = []

@st.dialog("Test", width="large")
def add_item(stack):
    st.tabs(['select1','select2'])
    dialog_submit = st.button("Submit")
    if dialog_submit:
        return stack.append("test")

if buttom_table_add:
    stack = add_item(stack)

right_column.write(stack)

# Solving behavior: Delete

# Solving behavior: Update




























