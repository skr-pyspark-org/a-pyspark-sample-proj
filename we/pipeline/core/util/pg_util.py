import psycopg2

class PGQueries:

    def __init__(self , pg_host:str , pg_username:str , pg_password:str , pg_database:str , pg_port:str):

        """
        the constructor to initialize object

        :param pg_host:
        :param pg_username:
        :param pg_password:
        :param pg_database:
        :param pg_post:
        """
        self.pg_host = pg_host
        self.pg_username = pg_username
        self.pg_password = pg_password
        self.pg_database = pg_database
        self.pg_port = pg_port



    def pg_connection(self):
        db_params = {
            "host" : self.pg_host,
            "databse" : self.pg_database,
            "user" : self.pg_username,
            "password" : self.pg_password,
            "port" : self.pg_port
        }
        try:
            con = psyconpg2.connect(**db_params)
            return con
        except Exception as e:
            print(e)


    def update_query(self, query):
        pg_conn = None
        cur = None
        try:
            pg_conn = self.pg_connection()
            cur = pg_conn.cursor()
            res = cur.execute(query)
            print(f"update query result ::: {res}")
            pg_conn.commit()

        except Exception as e:
            raise ValueError(f"Postgress Error : {e}")



    def select_query(self , query):
        pg_conn = None
        cur = None
        try:
            pg_conn = self.pg_connection()
            cur = pg_conn.cursor()
            res = cur.execute(query)
            print(f"update query result ::: {res}")
            pg_conn.commit()

        except Exception as e:
            raise ValueError(f"Postgress Error : {e}")
        
