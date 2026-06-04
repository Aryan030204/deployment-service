import mysql.connector
from mysql.connector import Error
from app.utils.logger import get_logger

logger = get_logger(__name__)

class DBManager:
    def __init__(self, brand_name: str, host: str, user: str, password: str, database: str, port: int = 3306):
        self.brand = brand_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                logger.debug(f"Connected to database '{self.database}' for brand '{self.brand}'")
            return self.connection
        except Error as e:
            logger.error(f"Error while connecting to MySQL database '{self.database}' ({self.host}): {e}")
            raise e

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()

    def execute_query(self, query: str, params: tuple = None, commit=False):
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            if commit:
                self.connection.commit()
                return cursor.rowcount
            else:
                return cursor.fetchall()
        except Error as e:
            logger.error(f"Error executing query: {e}")
            if commit:
                self.connection.rollback()
            raise e
        finally:
            cursor.close()
