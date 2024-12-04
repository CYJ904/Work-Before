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
        with open('config.yaml', 'r') as file:
            self.database_config = yaml.safe_load(file)

        self.database_name = self.database_config['database_name']
        self.table_names = self.database_config['table_name']

        self.table_keys = {}
        for name in self.table_names:
            self.table_keys[name] = self.database_config[name]

        self.table_primary_keys = {}
        for name in self.table_names:
            self.table_primary_keys[name] = self.database_config['primary_keys'][name]

        # type of neighbour: dict
        # type of other: str
        self.relations = {}
        for name in self.table_names:
            self.relations[name] = self.database_config['foreign_keys'][name]

    # requests some information
    def run(self):
        pass

    def get_single_min(self, table_name, key_name, filters=None):
        query=f"""
        SELECT MIN({key_name}) AS min_amount
        FROM {table_name};
        """
        result = self.connection.query(query)
        return result['min_amount']

    def get_single_max(self, table_name, key_name, filters=None):
        query=f"""
        SELECT MAX({key_name}) AS max_amount
        FROM {table_name};
        """
        result = self.connection.query(query)
        return result['max_amount']

    def get_single_min_max(self, table_name, key_name, filters=None):
        query=f"""
        SELECT MIN({key_name}) AS min_amount,
               MAX({key_name}) AS max_amount
        FROM {table_name};
        """
        result = self.connection.query(query)
        return result['min_amount'], result['max_amount']

    def get_single_unique(self, table_name, key_name, filters=None):
        query = f"""
        SELECT UNIQUE({key_name}) AS unique_key
        FROM {table_name};
        """

        result = self.connection.query(query)
        return result['unique_key']


    def get_tables(self):
        return self.table_names

    def get_keys(self, table_name):
        if table_name in self.table_names:
            return self.table_keys[table_name]
        else:
            return None

    def query(self, sql_query):
        return self.connection.query(sql_query)

    def data_add(self, table_name, data):
        pass

    def data_delete(self, table_name, data):
        pass

    def data_update(self, table_name, original_data, updated_data):
        pass


def bridge_tables(start_table, end_table):
    if start_table == end_table:
        return None
    with open('config.yaml', 'r') as file:
        dictionary = yaml.safe_load(file)
    dictionary = dictionary['foreign_keys']
    needed_tables = []
    start = start_table
    end = end_table
    connected = False
    while not connected:
        bridge = dictionary[start][end]
        if type(bridge) == dict:
            connected = True
        else: # type(bridge) == str
            start = bridge
            needed_tables.append(start)

    return needed_tables





# connection = st.connection('source', type='sql')
# connector = Connector(connection=connection)

