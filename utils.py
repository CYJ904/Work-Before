import streamlit as st
import yaml
import random
import string
import config
import datetime
import pandas as pd
import mariadb


class NewConnector:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None
        self.connect()
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

        self.relations = {}
        for name in self.table_names:
            self.relations[name] = self.database_config['foreign_keys'][name]

    def connect(self):
        # print("Start Connection")
        try:
            self.connection = mariadb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            # print("Connected to MariaDB successfully.")
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")
            raise

    def get_single_min(self, table_name, key_name):
        query = f"""
        SELECT MIN({key_name}) AS min_amount
        FROM {table_name};
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return pd.Series([result[0]])

    def get_single_max(self, table_name, key_name):
        query = f"""
        SELECT MAX({key_name}) AS max_amount
        FROM {table_name};
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return pd.Series([result[0]])

    def get_single_min_max(self, table_name, key_name):
        query = f"""
        SELECT MIN({key_name}) AS min_amount,
               MAX({key_name}) AS max_amount
        FROM {table_name};
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return pd.Series([result[0]]), pd.Series([result[1]])

    def get_single_unique(self, table_name, key_name):
        query = f"""
        SELECT DISTINCT {key_name} AS unique_key
        FROM {table_name};
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return pd.Series([row[0] for row in result])

    # def get_tables(self):
        # return self.table_names

    # def get_keys(self, table_name):
        # return self.table_keys.get(table_name, None)

    def query(self, sql_query, value):
        self.cursor.execute(sql_query, value)
        result = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        df = pd.DataFrame(result, columns=columns)
        return df

    def execute(self, sql_query, value):
        self.cursor.execute(sql_query, value)

    def start_transaction(self):
        if self.cursor:
            self.connection.autocommit = False
            self.cursor.execute("START TRANSACTION;")

    def rollback_full(self):
        if self.cursor:
            self.cursor.execute("ROLLBACK;")

    def checkpoint_rollback(self, checkpoint):
        self.cursor.execute(f"ROLLBACK TO SAVEPOINT {checkpoint};")

    def checkpoint_add(self, checkpoint):
        self.cursor.execute(f"SAVEPOINT {checkpoint};")
        # print(f"savepoint {checkpoint} executed")
        return checkpoint

    def commit(self):
        if self.cursor:
            self.cursor.execute("COMMIT;")

    def close(self):
        # print("Closing Connector...")
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connector closed.")

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
        # return f"'{input}'"
        return (input,)
    elif isinstance(input, int):
        # return str(input)
        return (input,)
    elif isinstance(input, float):
        # return str(input)
        return (input,)
    elif isinstance(input, tuple):
        if input[1] is None:
            # short version
            # return f"'{input[0].strftime(config.timefstr['short'])}'"
            return (input[0],)
        else:
            #  long version
            tmp = datetime.datetime.combine(input[0], input[1])
            # return f"'{tmp.strftime(config.timefstr['long'])}'"
            return (tmp,)

def search_preprocessing(name, input):
    # no process over string
    query_side = ""
    value_side = tuple()
    if isinstance(input, tuple):
        query_side = f"{name} BETWEEN ? AND ?"
        value_side = (input[0], input[1])
    else:
        query_side = f"{name} = ?"
        value_side = (input,)

    return query_side, value_side
    # if isinstance(input, str):
    #     return f"{name} = '{input}'"
    # elif isinstance(input, int):
    #     return f"{name} = {input}"
    # elif isinstance(input, float):
    #     return f"{name} = {input}"
    # elif isinstance(input, tuple):
    #     if isinstance(input[0], str):
    #         if input[0] == input[1]:
    #             return f"{name} = '{input[0]}'"
    #         else:
    #             return f"{name} BETWEEN '{input[0]}' AND '{input[1]}'"
    #     elif isinstance(input[0], int):
    #         if input[0] == input[1]:
    #             return f"{name} = {input[0]}"
    #         else:
    #             return f"{name} BETWEEN {input[0]} AND {input[1]}"
    #     elif isinstance(input[0], float):
    #         if input[0] == input[1]:
    #             return f"{name} = {input[0]}"
    #         else:
    #             return f"{name} BETWEEN {input[0]} AND {input[1]}"
    #     elif isinstance(input[0], datetime.date):
    #         if input[0] == input[1]:
    #             return f"{name} = {input[0].strftime(config.timefstr['short'])}"
    #         else:
    #             return f"{name} BETWEEN {input[0].strftime(config.timefstr['short'])} AND {input[1].strftime(config.timefstr['short'])}"
    #     elif isinstance(input[0], datetime.datetime):
    #         if input[0] == input[1]:
    #             return f"{name} = {input[0].strftime(config.timefstr['long'])}"
    #         else:
    #             return f"{name} BETWEEN {input[0].strftime(config.timefstr['long'])} AND {input[1].strftime(config.timefstr['long'])}"
    #     else:
    #         return None
    # else:
    #     return None



# connection = st.connection('source', type='sql')
# connector = Connector(connection=connection)

