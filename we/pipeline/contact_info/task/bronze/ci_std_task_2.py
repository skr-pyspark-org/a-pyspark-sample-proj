import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, expr

from we.pipeline.core.constant import (
    LOG_FORMAT,
    ENV_OPTION,
    SPACE_OPTION,
    CONFIG_OPTION,
    CATALOG_OPTION,
    BRONZE_ZONE,
    DATA_GROUP_NEUSTAR
)
from we.pipeline.core.util.configuration_util import (
    is_valid_environment_space,
    to_database_name,
    Configuration
)

logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

_SOURCE_TABLE_NAME = "ci_bronze_std_work"
_DESTINATION_TABLE_NAME = "ci_bronze_std_final"


def etl_process(**options):
    """"""
    """
        Task to filter out records with either first_name or last_name of city column values as empty or null 
    """
    env = options.get(ENV_OPTION)
    space = options.get(SPACE_OPTION)

    if (env is None or space is None) or not is_valid_environment_space(env, space):
        raise ValueError(f"Invalid environment or space :::: {env}/{space}")

    spark = SparkSession.builder.appName("contact_info_std")
    config = Configuration(spark, options.get(CONFIG_OPTION), env=env, space=space)
    catalog_name = config.get(CATALOG_OPTION)
    source_database = to_database_name(space, BRONZE_ZONE, DATA_GROUP_NEUSTAR)
    target_database = to_database_name(space, BRONZE_ZONE, DATA_GROUP_NEUSTAR)

    """
    Read source
    """
    ip_df = spark.sql(f"select * from {catalog_name}.{source_database}.{_SOURCE_TABLE_NAME}")

    """
    Apply filter and write to databricks
    """
    res_df = ip_df.withColumn("first_name" , expr("upper(first_name)"))
    res_df = res_df.withColumn("last_name" , expr("upper(last_name)"))
    res_df = res_df.withColumn("full_name" , expr("concat(first_name ,' ',middle_name , ' ' ,last_name )"))
    res_df.write.mode("append").saveAstable(f"{catalog_name}.{target_database}.{_DESTINATION_TABLE_NAME}")







