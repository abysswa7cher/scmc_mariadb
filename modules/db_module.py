import mariadb, sys, datetime, warnings
from configparser import ConfigParser

import pandas as pd
warnings.filterwarnings('ignore')
class MarketDB():

    def __init__(self) -> None: #, host
        self.connected = False
        self.connection = None
        self.cursor = None
        # self.connect(host)

    def connect(self, host):
        print(f"Connecting to {host}")
        try:
            connection = mariadb.connect(**self._read_config(section=host))
        except mariadb.Error as e:
            print(e)
            sys.exit(1)
        
        if connection is not None:
            self.connection = connection
            self.cursor = self.connection.cursor()
            self.connected = True
            print(f"Connected to {self.connection.server_name}, version: {self.connection.server_info}")
    
    def disconnect(self):
        # close the communication with the PostgreSQL
        print(f"Disconnecting from {self.connection.server_name} database.")
        self.connection.close()
        self.connected = False

    def create_table(self, table_name, cols):
        query = f"create table if not exists {table_name} ({', '.join(cols) if type(cols) != str else cols});"
        try:            
            if not self._table_exists(table_name):
                self.cursor.execute(query)
                print(f"Table {table_name} created.")
            else:
                print(f"Table {table_name} already exists.")
        except mariadb.Error as e:
            print(e)

    def insert(self, table_name, columns, values):
        val_placeholder = ("".join(["%s, " if i < len(columns)-1 else "%s" for i, _ in enumerate(columns)])) if type(columns) is list else "%s"
        cols = ", ".join(columns) if type(columns) is list else columns
        query = """insert into {} ({}) values ({})""".format(table_name, cols, val_placeholder)
        # print(query)
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except mariadb.Error as e:
            print(e)
            raise
    
    def _table_exists(self, table_name):
        self.cursor.execute("show tables;")
        return table_name in list(sum(self.cursor.fetchall(), ()))

    def dict_to_db(self, dict, table_name):
        cols = list(dict.keys())
        values = tuple(dict.values())

        try:
            self.insert(table_name, cols, values)
            # print(f"Posted\n{list(dict.values())} to {str(table_name)}")
            print(f"Inserted new data to {table_name}")
        except (Exception, mariadb.Error) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()
            raise

    def _read_config(self, filename='database.ini', section='mariadb'):
        parser = ConfigParser()
        parser.read(filename)

        config = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                try:
                    config[param[0]] = int(param[1])
                except:
                    config[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return config
    

    def get_table(self, table_name: str, columns: list) -> pd.DataFrame:

        query = f"select {', '.join(columns)} from {table_name};"
        res = pd.read_sql(query, 
                          self.connection, 
                          index_col=None, 
                          dtype={"kind": int,
                                 "price": float,
                                 "quantity": int,
                                 "id": int,
                                 "year": int,
                                 "month": int,
                                 "day": int,
                                 "hour": int,
                                 "minutes": int})
        return res
    
    def get_last_row(self, table_name):
        try:
            self.cursor.execute(f"select timestamp from {table_name} order by id desc limit 1")
        except Exception:
            raise
            sys.exit()
        return self.cursor.fetchone()