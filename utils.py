import streamlit as st
import yaml
import random
import string
import config
import datetime
import sqlalchemy
import pandas as pd
import mariadb


class Connector():
    def __init__(self, connection):
        self.connection = connection
        self.connection.reset()
        self.correct_connection = connection.engine.connect()
        self.transaction = None
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
        result = self.correct_connection.execute(sqlalchemy.text(query))
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        return result['min_amount']

    def get_single_max(self, table_name, key_name, filters=None):
        query=f"""
        SELECT MAX({key_name}) AS max_amount
        FROM {table_name};
        """
        result = self.correct_connection.execute(sqlalchemy.text(query))
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        return result['max_amount']

    def get_single_min_max(self, table_name, key_name, filters=None):
        query=f"""
        SELECT MIN({key_name}) AS min_amount,
               MAX({key_name}) AS max_amount
        FROM {table_name};
        """
        result = self.correct_connection.execute(sqlalchemy.text(query))
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        return result['min_amount'], result['max_amount']

    def get_single_unique(self, table_name, key_name, filters=None):
        query = f"""
        SELECT UNIQUE({key_name}) AS unique_key
        FROM {table_name};
        """

        result = self.correct_connection.execute(sqlalchemy.text(query))
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        return result['unique_key']


    def get_tables(self):
        return self.table_names

    def get_keys(self, table_name):
        if table_name in self.table_names:
            return self.table_keys[table_name]
        else:
            return None

    def query(self, sql_query):
        result = self.correct_connection.execute(sqlalchemy.text(sql_query))
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(sql_query)
        print(result)
        print()
        return result

    def execute(self, sql_query):
        result = self.correct_connection.execute(sqlalchemy.text(sql_query))
        print(sql_query)
        print(result)
        print()
        return result

    def start_transaction(self):
        self.transaction = self.correct_connection.begin()

    def rollback_full(self):
        self.transaction.rollback()

    def checkpoint_rollback(self, checkpoint):
        checkpoint.rollback()

    def checkpoint_add(self):
        return self.correct_connection.begin_nested()

    def commit(self):
        self.transaction.commit()


class NewConnector:
    def __init__(self, user='user_431', password='user_431', host='localhost', port=3306, database='source'):
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
        print("Start Connection")
        try:
            self.connection = mariadb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            print("Connected to MariaDB successfully.")
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
        print(sql_query)
        print(df)
        return df

    def execute(self, sql_query, value):
        self.cursor.execute(sql_query, value)

    def start_transaction(self):
        if self.cursor:
            self.cursor.execute("START TRANSACTION;")

    def rollback_full(self):
        if self.cursor:
            self.cursor.execute("ROLLBACK;")

    def checkpoint_rollback(self, checkpoint):
        self.cursor.execute(f"ROLLBACK TO SAVEPOINT {checkpoint};")

    def checkpoint_add(self, checkpoint):
        self.cursor.execute(f"SAVEPOINT {checkpoint};")
        print(f"savepoint {checkpoint} executed")
        return checkpoint

    def commit(self):
        if self.cursor:
            self.cursor.execute("COMMIT;")

    def close(self):
        print("Closing Connector...")
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connector closed.")

class TestConnector:
    def __init__(self, database, user, password, host='localhost', port=3306):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Connects to the MariaDB database and initializes the cursor.
        """
        try:
            self.connection = mariadb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            print("Connection to the database established successfully.")
        except mariadb.Error as e:
            print(f"Error connecting to database: {e}")

    def disconnect(self):
        """
        Closes the cursor and the connection to the database.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def execute_query(self, query):
        """
        Executes a given query.

        :param query: SQL query to execute
        :return: Fetched results if it's a SELECT query, None otherwise
        """
        try:
            self.cursor.execute(query)
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
        except mariadb.Error as e:
            print(f"Error executing query: {e}")

    def execute(self, statement):
        """
        Executes an add, delete, or update statement.

        :param statement: SQL statement to execute
        """
        try:
            self.cursor.execute(statement)
        except mariadb.Error as e:
            print(f"Error executing statement: {e}")

    def commit(self):
        """
        Commits the current transaction to the database.
        """
        try:
            if self.connection:
                self.connection.commit()
                print("Transaction committed successfully.")
        except mariadb.Error as e:
            print(f"Error committing transaction: {e}")

    def rollback(self):
        """
        Rolls back the current transaction.
        """
        try:
            if self.connection:
                self.connection.rollback()
                print("Transaction rolled back successfully.")
        except mariadb.Error as e:
            print(f"Error rolling back transaction: {e}")

    def savepoint(self, savepoint_name):
        """
        Creates a savepoint with the given name.

        :param savepoint_name: Name of the savepoint
        """
        try:
            self.cursor.execute(f"SAVEPOINT {savepoint_name}")
            print(f"Savepoint '{savepoint_name}' created successfully.")
        except mariadb.Error as e:
            print(f"Error creating savepoint: {e}")

    def rollback_to_savepoint(self, savepoint_name):
        """
        Rolls back to the specified savepoint.

        :param savepoint_name: Name of the savepoint
        """
        try:
            self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            print(f"Rolled back to savepoint '{savepoint_name}' successfully.")
        except mariadb.Error as e:
            print(f"Error rolling back to savepoint: {e}")

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
            return f"'{input[0].strftime(config.timefstr['short'])}'"
        else:
            #  long version
            tmp = datetime.datetime.combine(input[0], input[1])
            return f"'{tmp.strftime(config.timefstr['long'])}'"

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

