import os.path
from typing import Dict

from pyspark.sql import SparkSession

WE_PIPELINE_SCOPE = "we-pipeline-scope"

SMARTY_AUTH_ID = "SMARTY_AUTH_ID"
SMARTY_AUTH_TOKEN = "SMARTY_AUTH_TOKEN"
GH_TOKEN = "GH_TOKEN"
TOTAL_EXECUTORS = "TOTAL_EXECUTORS"
SMARTY_RATE_LIMIT = "SMARTY_RATE_LIMIT"
SMARTY_RETRY_LIMIT = "SMARTY_RATRY_LIMIT"
ES_ENDPOINT = "ES_ENDPOINT"
PG_USERNAME = "PG_USERNAME"
PG_HOST = "PG_HOST"
PG_PASSWORD = "PG_PASSWORD"
PROD_PG_USERNAME = "PROD_PG_USERNAME"
PROD_PG_HOST = "PROD_PG_HOST"
PROD_PG_PASSWORD = "PROD_PG_PASSWORD"
PG_JDBC_DRIVER = "PG_JDBC_DRIVER"
JIRA_CLIENT_ID = "JIRA_CLIENT_ID"
JIRA_CLIENT_SECRET = "JIRA_CLIENT_SECRET"
SODA_CLIENT_API_KEY = "SODA_CLIENT_API_KEY"
SODA_CLIENT_API_SECRET = "SODA_CLIENT_API_SECRET"


RAW_ZONE = "raw"
BRONZE_ZONE = "bronze"
SILVER_ZONE = "silver"
GOLD_ZONE ="gold"

def get_dbutils(spark : SparkSession) :
    """
    Safely retrieves dbutils whether running locally or on a databricks cluster

    :param spark: SparkSession
    :return: A DButils object if code is executed on a Databricks cluster
             None otherwise
    """
    try:
        from pyspark.dbutils import DBUtils

        if "dbutils" not in locals():
            uils = DBUtils(spark)

        return locals().get("dbutils")
    except ImportError:
            return None


def get_local_dbutils():
    class MockDButilsSecret:
        def __init__(self,config: Dict):
            self.config = config


        def get(self,scope,key):
            return self.config.get(scope).get(key)

        def getBytes(self,scope,key):
            return bytes(self.config.get(scope).get(key))

        def list(self,scope):
            return self.config.get(scope).keys()

        def listScopes(self):
            return self.config.keys()

    class MockDbutils:
        def __init__(self, secrets:MockDButilsSecret):
            self.secrets = secrets

    from dotenv import load_dotenv
    load_dotenv()

    secrets = MockDButilsSecret({
        WE_PIPELINE_SCOPE: {
            SMARTY_AUTH_ID: os.environ[SMARTY_AUTH_ID],
            SMARTY_AUTH_TOKEN: os.environ[SMARTY_AUTH_TOKEN],
            GH_TOKEN: os.environ[GH_TOKEN],
            ES_ENDPOINT: os.environ[ES_ENDPOINT],
            TOTAL_EXECUTORS: os.environ[TOTAL_EXECUTORS],
            SMARTY_RATE_LIMIT: os.environ[SMARTY_RATE_LIMIT],
            SMARTY_RETRY_LIMIT: os.environ[SMARTY_RETRY_LIMIT],
            PG_USERNAME: os.environ[PG_USERNAME],
            PG_HOST: os.environ[PG_HOST],
            PG_PASSWORD: os.environ[PG_PASSWORD],
            PG_JDBC_DRIVER: os.environ[PG_JDBC_DRIVER]

        }
    })

    obj = MockDbutils(secrets=secrets)
    return obj





