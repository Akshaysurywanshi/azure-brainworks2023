import snowflake.connector

class SnowflakeConnector:
    def __init__ (self,account,user,password,warehouse,database,schema):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        self.cursor = None

    def connect(self):
        """ establish connection to snowflake"""
        self.connection = snowflake.connector.connect(
            user = self.user
            password = self.password
            account = self.account
            warehouse = self.warehouse
            database = self.database
            schema = self.schema
        )
        self.cursor = self.connection.cursor()

    def excute_query(self,query):
        """ Execute a sql query"""
        if not self.connection or not self.cursor:
            raise Exception("connection not establish.call connect() method first.")
        
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        """ close the connection """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()