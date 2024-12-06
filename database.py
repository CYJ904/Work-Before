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

    def create_user_and_grant_privileges(self):
        try:
            print("Checking and creating user if necessary...")
            # SQL statements to ensure user and privileges exist
            create_user_query = "CREATE USER IF NOT EXISTS 'user_431'@'%' IDENTIFIED BY 'user_431';"
            grant_privileges_query = "GRANT ALL PRIVILEGES ON source.* TO 'user_431'@'%';"
            flush_privileges_query = "FLUSH PRIVILEGES;"

            # Execute the statements
            self.cursor.execute(create_user_query)
            self.cursor.execute(grant_privileges_query)
            self.cursor.execute(flush_privileges_query)
            self.connection.commit()
            print("User created and privileges granted successfully (if not already existed).")
        except mariadb.Error as e:
            print(f"Error ensuring user existence or granting privileges: {e}")
            raise

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
