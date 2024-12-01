import mariadb

class MariaDBConnection:
    def __init__(self, user='user_431', password='user_431', host='localhost', port=3306, database='source'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None
        print("Initialized")

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

    def insert_data(self, table_name, dict_value):
        pass

    def fetch_data(self, query):

        pass

    def close(self):
        print("Start close")
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("MariaDB connection closed.")

if __name__ == "__main__":
    database = MariaDBConnection()
    database.connect()
    database.close()
