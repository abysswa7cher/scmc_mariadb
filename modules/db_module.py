import mariadb, sys
from configparser import ConfigParser

mariadb_config = {"host":"scmcmariadbsql.ddns.net", 
                    "port": 3306, 
                    "user":"remote_admin", 
                    "password":"password", 
                    "database":"scmc_dev"}


class MarketDB():

    def __init__(self) -> None:
        self.connection = None
        self.cursor = None

    def connect(self, config):
        print("Connecting to MariaDB...")
        try:
            connection = mariadb.connect(**self._read_config(config))
        except mariadb.Error as e:
            print(e)
            sys.exit(1)
        
        if connection is not None:
            self.connection = connection
            self.cursor = self.connection.cursor()
            print(f"Connected to {self.connection.server_name}, version: {self.connection.server_info}")
    
    def disconnect(self):
        # close the communication with the PostgreSQL
        print("Disconnecting from PostgreSQL database.")
        self.connection.close()

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
        val_placeholder = "".join(["%s, " if i < len(columns)-1 else "%s" for i, _ in enumerate(columns)])
        cols = ", ".join(columns) if type(columns) is list else columns
        query = """insert into {} ({}) values ({})""".format(table_name, cols, val_placeholder)
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except mariadb.Error as e:
            print(e)
    
    def _table_exists(self, table_name):
        self.cursor.execute("show tables;")
        return table_name in list(sum(self.cursor.fetchall(), ()))

    def dict_to_db(self, dict, table_name):
        cols = list(dict.keys())
        values = tuple(dict.values())

        # print(cols, values)
        try:
            self.insert(table_name, cols, values)
            print(f"Posted\n{*dict,} to {table_name,}")
        except (Exception, mariadb.Error) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()

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