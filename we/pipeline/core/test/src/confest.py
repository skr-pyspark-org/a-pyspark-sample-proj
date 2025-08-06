import os
import tempfile
from pathlib import Path
from typing import Text


import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    builder = SparkSession.builder\
                .appname("we_pipeline_test")\
                .config("spark.ui.showconsoleProgress" , "false") \
                .config("spark.sql.bebug.maxToStringFields" , "200") \
                .config("spark.driver.bindAddress" , "127.0.0.1")

    if 'SPARK_MASTER' in os.environ:
        builder = builder.master(os.environ['SPARk_MASTER'])
    else:
        builder = builder.master('local[1]')


    s = builder.getOrCreate()

    with tempfile.TemporaryDirectory("we_pipeline") as dir:
        s.sparkContext.checkpointDir(dir)
        yield s
        s.stop()


@pytest.fixture(scope="module")
def config_file() -> Text:
    file = "we/pipeline/core/config/we_pipeline_test.json"
    return ( Path(os.getcwd()) / file ).as_posix()


