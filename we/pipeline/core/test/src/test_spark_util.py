import json
import os
import tempfile
import unittest
from importlib import resources as pkg_resources
from typing import Text, List, Tuple

import pytest
from pyspark.sql.dataframe import  DataFrame
from pyspark.sql.session import  SparkSession


from pyspark.sql.types import (
    DataType,
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType
)

from we.pipeline.core.test.src.util import(
    create_spark_session,
    ignore_warnings
)

from we.pipeline.core.util.configuration_util import  WE_PIPELINE_LOCAL
from we.pipeline.core.util.spark_util import (
execute_query ,
transform ,
load_schema_from_package ,
load_dataframe,
last_id,
column_names ,
load_json_from_package,
calculate_hash_value
)




@pytest.fixture(autouse=True)
def around_tests():
    env_local = os.getenv(WE_PIPELINE_LOCAL)
    yield
    #Restore original value between tests
    if env_local:
        os.environ[WE_PIPELINE_LOCAL] = env_local
    else:
        if WE_PIPELINE_LOCAL in os.environ.keys():
            del os.environ[WE_PIPELINE_LOCAL]
        else:
            pass



def create_data_frame(spark:SparkSession ,
                      columns:List[Text],
                      data: List[Tuple],
                      types: List[DataType]) -> DataFrame:
    cols = zip(columns , types)
    fields = [StructField(c , t, True) for c,t in cols]
    schema = StructType(fields)

    return spark.createDataFrame(data=data , schema=schema)



class SparkUtilTest(unittest.TestCase):
    spark:SparkSession

    @classmethod
    @ignore_warnings(ResourceWarning)
    def setUpClass(cls) -> None:
        options= {
            'spark.sql.shuffle.partitions' : 1,
            'spark.ui.showConsoleProgress' : 'false',
            'spark.sql.debug.maxToStringFields' : '200'
        }
        cls.spark = create_spark_session(__name__ , 'local[1]' , options)
    
    @classmethod
    @ignore_warnings(ResourceWarning)
    def tearDownClass(cls) -> None:
        cls.spark.stop()




    def test_last_id(self):
        spark = self.spark

        df = create_data_frame([
            (1,'JOHN','DOE') ,
            (2,'MARK','CUBAN')
        ])

        empty_df = spark.createDataFrame([],'we_pid log , first_name string , last_name string')

        assert 2 == last_id(df , 'we_pid')
        assert 2 == last_id(df , 'we_pid',16)

        assert 0 == last_id(empty_df, 'we_pid')
        assert 100 == last_id(empty_df, 'we_pid',100)


    def test_column_names(self):
        spark = self.spark

        df = create_data_frame([
            (1,'JOHN','DOE') ,
            (2,'MARK','CUBAN')
        ])

        col_names = column_names(spark , df)
        assert set(df.columns) == set(col_names)

        view_name = "test_column_names_view"
        df.createOrReplaceTempView(view_name)

        col_names = column_names(spark ,view_name)
        assert set(df.columns) == set(col_names)

        col_names = column_names(spark , '')
        assert col_names is not None
        assert col_names == []









