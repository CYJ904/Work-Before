# a method load all table names in database
# a method load all key names in a table
# a method load all foreign key over a table
import streamlit as st
import yaml
import random
import string
import config
import datetime
import sqlalchemy


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

    def execute(self, sql_query):
        return self.connection.session.execute(sqlalchemy.text(sql_query))

    def start_transaction(self):
        self.connection.session.execute(sqlalchemy.text("BEGIN"))

    def rollback_full(self):
        self.connection.session.execute(sqlalchemy.text("ROLLBACK"))

    def checkpoint_rollback(self, checkpoint):
        self.connection.session.execute(sqlalchemy.text(f"ROLLBACK TO {checkpoint}"))

    def checkpoint_add(self, checkpoint):
        self.connection.session.execute(sqlalchemy.text(f"SAVEPOINT {checkpoint}"))

    def commit(self):
        self.connection.session.execute(sqlalchemy.text("COMMIT"))

    def run(self):
        pass

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

def generate_random_string(length=config.length):
    characters = string.ascii_letters + string.digits + "_"
    return "".join(random.choices(characters, k=length))


def input_preprocessing(input):
    # no process over string
    if isinstance(input, str):
        return f"'{input}'"
    elif isinstance(input, int):
        return str(input)
    elif isinstance(input, float):
        return str(input)
    elif isinstance(input, tuple):
        if input[1] is None:
            # short version
            return f"'{input[0].strftime(config.time['form']['short'])}'"
        else:
            #  long version
            tmp = datetime.datetime.combine(input[0], input[1])
            return f"'{tmp.strftime(config.time['form']['long'])}'"

def search_preprocessing(name, input):
    # no process over string
    if isinstance(input, str):
        return f"{name} = '{input}'"
    elif isinstance(input, int):
        return f"{name} = {input}"
    elif isinstance(input, float):
        return f"{name} = {input}"
    elif isinstance(input, tuple):
        if isinstance(input[0], str):
            if input[0] == input[1]:
                return f"{name} = '{input[0]}'"
            else:
                return f"{name} BETWEEN '{input[0]}' AND '{input[1]}'"
        elif isinstance(input[0], int):
            if input[0] == input[1]:
                return f"{name} = {input[0]}"
            else:
                return f"{name} BETWEEN {input[0]} AND {input[1]}"
        elif isinstance(input[0], float):
            if input[0] == input[1]:
                return f"{name} = {input[0]}"
            else:
                return f"{name} BETWEEN {input[0]} AND {input[1]}"
        elif isinstance(input[0], datetime.date):
            if input[0] == input[1]:
                return f"{name} = {input[0].strftime(config.timefstr['short'])}"
            else:
                return f"{name} BETWEEN {input[0].strftime(config.timefstr['short'])} AND {input[1].strftime(config.timefstr['short'])}"
        elif isinstance(input[0], datetime.datetime):
            if input[0] == input[1]:
                return f"{name} = {input[0].strftime(config.timefstr['long'])}"
            else:
                return f"{name} BETWEEN {input[0].strftime(config.timefstr['long'])} AND {input[1].strftime(config.timefstr['long'])}"
        else:
            return None
    else:
        return None



# connection = st.connection('source', type='sql')
# connector = Connector(connection=connection)

