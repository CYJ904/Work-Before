# a method load all table names in database
# a method load all key names in a table
# a method load all foreign key over a table
import streamlit as st
connection = st.connection('source', type='sql')

table_names_query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'source';
"""


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
        self.key_names = {}
        for name in self.table_names:
            table_colmns_query = f"""
            SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'source'
            AND TABLE_NAME = '{name}';
            """
            self.key_names[name] = self.connection.query(table_colmns_query)['column_name'].tolist()

        self.table_reference = {}
        for name in self.table_names:
            for reference_name in (self.table_names - name):
                self.table_reference[name][reference_name] = {"local":"", "remote":""}
        for name in self.table_names:
            table_reference_query = f"""
            SELECT
                TABLE_NAME AS referencing_table,
                COLUMN_NAME AS referencing_column,
                CONSTRAINT_NAME AS foreign_key_name,
                REFERENCED_TABLE_NAME AS referenced_table,
                REFERENCED_COLUMN_NAME AS referenced_column
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                TABLE_SCHEMA = 'source'
                AND TABLE_NAME = '{name}'
            """
            content = self.connection.query(table_reference_query)
            referenced_table_names = content['referenced_table'].tolist()
            local_keys = content['referencing_column'].tolist()
            remote_keys = content['referenced_column'].tolist()
            for reference_name, local_key, remote_key in zip(referenced_table_names, local_keys, remote_keys):
                self.table_reference[name][reference_name]['local'] = local_key
                self.table_reference[name][reference_name]['remote'] = remote_key
                self.table_reference[reference_name][name]['local'] = remote_key
                self.table_reference[reference_name][name]['remote'] = local_key


        self.query = {}
        self.query['basic'] = {"select": "", "from": "", "where":""} 
        self.query['extend'] = None

    def get_tables(self):
        return self.table_names

    def get_keys(self, table_name):
        return self.key_names[table_name]



connector = Connector(connection=connection)

