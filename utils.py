# a method load all table names in database
# a method load all key names in a table
# a method load all foreign key over a table
import streamlit as st
import yaml
# connection = st.connection('source', type='sql')





# +-------------------+-----------------------------+-----------------------+------------------+-----------------------------+
# | referencing_table | referencing_column          | foreign_key_name      | referenced_table | referenced_column           |
# +-------------------+-----------------------------+-----------------------+------------------+-----------------------------+
# | order_payments    | order_id                    | PRIMARY               | NULL             | NULL                        |
# | order_payments    | payment_sequential          | PRIMARY               | NULL             | NULL                        |
# | order_payments    | order_id                    | order_payments_ibfk_1 | orders           | order_id                    |
# | sellers           | seller_id                   | PRIMARY               | NULL             | NULL                        |
# | sellers           | seller_zip_code_prefix      | sellers_ibfk_1        | geolocation      | geolocation_zip_code_prefix |
# | products          | product_id                  | PRIMARY               | NULL             | NULL                        |
# | order_items       | order_id                    | PRIMARY               | NULL             | NULL                        |
# | order_items       | order_item_id               | PRIMARY               | NULL             | NULL                        |
# | order_items       | order_id                    | order_items_ibfk_1    | orders           | order_id                    |
# | order_items       | product_id                  | order_items_ibfk_2    | products         | product_id                  |
# | order_reviews     | order_id                    | PRIMARY               | NULL             | NULL                        |
# | order_reviews     | review_id                   | PRIMARY               | NULL             | NULL                        |
# | order_reviews     | order_id                    | order_reviews_ibfk_1  | orders           | order_id                    |
# | orders            | order_id                    | PRIMARY               | NULL             | NULL                        |
# | customers         | customer_id                 | PRIMARY               | NULL             | NULL                        |
# | customers         | customer_zip_code_prefix    | customers_ibfk_1      | geolocation      | geolocation_zip_code_prefix |
# | geolocation       | geolocation_zip_code_prefix | PRIMARY               | NULL             | NULL                        |
# +-------------------+-----------------------------+-----------------------+------------------+-----------------------------+




class Connector():
    def __init__(self, connection):
        self.connection = connection
        self.table_names = self.connection.query(table_names_query)['table_name'].tolist()

    def get_tables(self):
        return self.table_names

    def get_keys(self, table_name):
        return self.key_names[table_name]



connector = Connector(connection=connection)

