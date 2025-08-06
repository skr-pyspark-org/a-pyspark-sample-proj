from pyspark.sql import SparkSession
from we.pipeline.core.util import(
get_local_dbutils,
get_dbutils,
WE_PIPELINE_SCOPE
)


class PgSparkUtil:
    """
    A spark util to run queries against pg
    """

    def __init__(self,spark: SparkSession):
        """
        The constructor of the PgSparkUtil class
        :param spark:
        """
        dbutils = get_dbutils(spark) or get_local_dbutils()
        secrets = dbutils.secrets

        self.spark =spark
        self.pg_jdbc_driver = str(secrets.get(WE_PIPELINE_SCOPE , "PG_JDBC_DRIVEr"))
        self.pg_username = str(secrets.get(WE_PIPELINE_SCOPE , "PG_USERNAME_"))
        self.pg_password = str(secrets.get(WE_PIPELINE_SCOPE , "PG_PASSWORD"))


    def read_pg_data_from_table(self, table_name:str):
        """
        An instance method to query postgres table without any filter
        args:
            table_name:
        Returns:
            a dataframe of table
        """
        url =self.pg_jdbc_driver
        properties= {
            "user":self.pg_username,
            "password": self.pg_password,
            "driver": "org.postgressql.Driver"
        }
        query = f"( select * from {table_name} ) as temp_table "

        df = self.spark.read.jdbc(url=url , table=query , properties=properties)
        return df



    def read_pg_data_from_query(self, query:str):
        """
        An instance method to query postgres . here the query can be customized

        :param query:
        :return:
        """
        url = self.pg_jdbc_driver
        properties= {
            "user":self.pg_username,
            "password": self.pg_password,
            "driver": "org.postgressql.Driver"
        }
        query = f"( {query} ) as temp_table "

        df = self.spark.read.jdbc(url=url , table=query , properties=properties)
        return df
