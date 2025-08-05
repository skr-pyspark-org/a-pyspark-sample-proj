from pyspark.sql import SparkSession

from we.pipeline.core.constant import (
    ENV_OPTION ,
    SPACE_OPTION
)

from we.pipeline.core.util.configuration_util import  is_valid_environment_space


def etl_process(**options):
    env = options.get(ENV_OPTION)
    space = options.get(SPACE_OPTION)
    query = options.get("query")

    if (env is None or space is None) or not is_valid_environment_space(env,space):
        raise ValueError(f"Invalid Environment or space :: {env}/{space}")

    spark = SparkSession.builder(__name__).getOrCreate();
    print("Queries - "+str(query))

    queries = query.split("|")
    for q in queries:
        print("Executing query - "+str(q))
        spark.sql(q)

