import json
import logging
import os
from importlib import import_module
from importlib import resources as pkg_resources
from os import PathLike
from types import ModuleType
from typing import Text , Any , List , Union

from pyspark.sql import   Column , SparkSession , Window
import pyspark as pk
from pyspark.sql.functions import sha2 , concat_ws , when , col , trim  , regexp_replace , row_number
from pyspark.sql.types import StructType

from we.pipeline.core.constant import LOG_FORMAT , OVERWRITE , APPEND , DELTA
from we.pipeline.core.util.configuration_util import WE_PIPELINE_LOCAL
from pyspark.pandas import DataFrame

logging.basicConfig(format=LOG_FORMAT , level=logging.INFO)
logger = logging.getLogger(__name__)

__SPARK_SQL_FUNCTIONS_MODULE = "spark.sql.fucntions"

def dataframe_function(name:Text , *args: Any) -> DataFrame:
    """
    Invoke a spark sql fucntion with arguments . the function is expected to return dataframe

    :param name: Function_name eg: join , select etc
    :param args: arguments to be passed
    :return: a dataframe of the transformation
    """
    mod  = import_module(__SPARK_SQL_FUNCTIONS_MODULE)
    fcn = getattr(mod , name)
    return fcn(args)


def join(df1: DataFrame ,
         df2: DataFrame ,
         on: Union[Text , List[Text] , Column , List[Column] , None],
         how: Union[Text , None]):
    return df1.join(df2 , on , how)

def execute_query(spark:SparkSession,
                  query: str,
                  num_partitions: int = None,
                  cache:bool = False,
                  temp_view_name:str = None) -> DataFrame:
    """
    Execute a spark query
    :param spark:
    :param query:
    :param num_partitions:
    :param cache:
    :param temp_view_name:
    :return:
    """
    df =spark.sql(query)

    if num_partitions is None or num_partitions <= 0:
        pass
    else:
        df = df.repartition(num_partitions)

    if cache:
        df.cache()

    if temp_view_name:
        df.createOrReplaceTempView(temp_view_name)

    return df




def execute_ddl_query(spark:SparkSession , ddl_query:str) -> None:
    """
    A common function to execute ddl queries like create or drop
    :param spark:
    :param ddl_query:
    :return:
    """
    logger.info(f"DDL query : {ddl_query}")
    spark.sql(ddl_query)


def transform(name: Text , *args: object) -> DataFrame:
    """
    Execute a transformation function in current module with name 'name'

    :param name: the transformation function name
    :param args: the arguments to be passed to function
    :return: result og transformation
    """
    from importlib import import_module
    mod = import_module('we.pipeline.core.util.spark_util')
    fcn = getattr(mod , name)
    return fcn(*args)


def load_transform_from_package(spark: SparkSession ,
                                package : Text ,
                                filename : Text ,
                                fmt : Text ,
                                schema : Union[StructType , Text],
                                **options):
    """
    Load a dtaframe from a resource in a package
    :param spark:
    :param package: package_name eg :- 'we.pipeline.core.mapping'
    :param filename: a file to load
    :param fmt: file format
    :param schema: schema of the file
    :param options: kwargs
    :return: a dataframe
    """

    if filename is None or package is None:
        raise ValueError(f'package or filename is blank')

    contents = pkg_resources.read_text(package , filename)
    lines = contents.splitlines()

    rdd = spark.sparkContext.parallelize(lines)

    # DataFrameReader.csv() and .json() accept rdd
    # but .load() doesnot , therefore this function only
    # supports csv and json

    reader = spark.read.schema(schema).options(**options)

    if fmt.lower() == 'csv':
        df = reader.csv(rdd)
    elif fmt.lower() == 'json':
        df = reader.json(rdd)
    else:
        raise ValueError(f"fmt mjust be either csv or json")

    return df


def load_schema_from_package(package: Union[Text , ModuleType] ,
                             resource: Union[Text , PathLike],
                             encoding: Text = 'utf-8') -> StructType:
    """
    Load schema from a file in package

    :param package: package name
    :param resource: resource containing schema
    :param encoding: encoding of the resource
    :return: A StructType instance
    """
    t = pkg_resources.read_text(package , resource , encoding)
    j = json.loads(t)
    schema = StructType.fromJson(j)
    return schema



def load_script_from_package(package:Union[Text , ModuleType],
                             resource: Union[Text , PathLike],
                             output_dir:Text,
                             encoding:Text = 'utf-8'
                             ):
    """
    Load schema from a file in a package

    :param package: package name
    :param resource: resource conytaining schema
    :param output_dir:
    :param encoding: encoding of resource
    :return:
    """
    print("entered load_script from package")
    t = pkg_resources.read_text(package, resource, encoding)
    print(t)
    with open(f"{output_dir}{resource}","w") as f:
        f.write(t)
    print("end load script from package")


def load_json_from_package(package: Union[Text , ModuleType] ,
                             resource: Union[Text , PathLike],
                             encoding: Text = 'utf-8') -> StructType:
    """
    Load json from a file in package

    :param package: package name
    :param resource: resource containing schema
    :param encoding: encoding of the resource
    :return: A dictionary
    """
    t = pkg_resources.read_text(package , resource , encoding)
    j = json.loads(t)
    return j



def load_dataframe(spark: SparkSession ,
                   path: Text ,
                   local: bool =False,
                   **options) -> DataFrame:
    reader = spark.read
    if options:
        reader = reader.options(**options)

    p = path
    truths = ['true' , '1' , 'y' , 'yes']
    env_local = os.getenv(WE_PIPELINE_LOCAL , 'false').lower()
    if local or env_local in truths:
        if p.startswith('s3://'):
            p = p[5:]

    return reader.load(p)




def last_id(df:DataFrame , id_column: Text , default_value:int=0 ):
    """
    Return the last value of an ID coulmn of a dataframe
    The ID column is assumed to be castable to BIGINT .
        if dtaframe is empty then default value will be returned
    :param df: a dataframe
    :param id_column: a ID column
    :param default_value: default value
    :return: the last ID or default value
    """
    if id_column not in df.columns:
        raise ValueError(f"Invalid column : {id_column}" )

    expr = f"COALESCE(MAX(CAST({id_column} as BIGINT)), {default_value}"
    return int(df.selectExpr(expr).first[0])





def column_names(spark:SparkSession , source: Union[DataFrame , str]) -> list[str]:
    """
    Return the column names of a dataframe

    :param spark:
    :param source:
    :return: a lsit of column names.if source is None , return an empty list
    """
    if not source:
        return []

    if isinstance(source , pk.sql.dataframe.DataFrame):
        return source.columns
    else:
        col_name_rows = spark.sql(f"show column in {source} ") \
                             .select("col_name") \
                             .collect()
        return [r['col_name'] for r in col_name_rows]



def calculate_hash_value(input_df:DataFrame , hash_cols: list[str] , out_col:str="hash_key") -> DataFrame :
    """
    :param input_df:  an input dataframe to which the hash_key column iis appended with SHA256-bit hash
    :param hash_col:  list[str]
                      a list of column with the hash_key value is calculated
    :param out_col: str
                     an optional output column in which the sha 256 hash value is started

    :return:
    """
    if hash_cols:
        input_df = input_df.withColumn(out_col , sha2(concat_ws("||" , *hash_cols) , 256))

    return input_df




def dataframe_writer(data:DataFrame , catalog :str , target_database:str ,
                     target_table_name:str , write_mode:str ,
                     overwrite_schema:str = "false" , partition_col:Text = None) -> None:
    """
    a utility function which will write the content of dataframe to target
    table if exists without overwriting the schema/it will create it if not exist

    :param data:
    :param catalog:
    :param target_database:
    :param target_table_name:
    :param write_mode: writing modes => append , overwrite
    :param overwrite_schema: optional value to overwrite schema
    :param partition_col:
    :return: None
    """
    logger.info(f"Writing target dataframe to {catalog}.{target_database}.{target_table_name}")
    if write_mode == OVERWRITE and partition_col is not None:
        data\
            .write\
            .format(DELTA)\
            .mode(write_mode)\
            .option("overWriteSchema",overwrite_schema)\
            .partitionBy(partition_col)\
            .saveAsTable(f"{catalog}.{target_database}.{target_table_name}")
    elif write_mode == OVERWRITE and partition_col is  None:
        data\
            .write\
            .format(DELTA)\
            .mode(write_mode)\
            .option("overwriteSchema",overwrite_schema)\
            .saveAsTable(f"{catalog}.{target_database}.{target_table_name}")
    elif write_mode == APPEND and partition_col is not None:
        data\
            .write\
            .format(DELTA)\
            .mode(write_mode)\
            .partitionBy(partition_col)\
            .saveAsTable(f"{catalog}.{target_database}.{target_table_name}")
    elif  write_mode == APPEND and partition_col is None:
          data\
            .write\
            .format(DELTA)\
            .mode(write_mode)\
            .saveAsTable(f"{catalog}.{target_database}.{target_table_name}")


def dataframe_append_or_overwrite(spark: SparkSession , data:DataFrame , catalog :str , target_database:str ,
                     target_table_name:str  , partition_col:Text = None , overwrite_schema:str = "false") -> None:
        """
        Append the dataframe into table but if table not exists then overwrite
        :param spark:
        :param data:
        :param catalog:
        :param target_database:
        :param target_table_name:
        :param partition_col:
        :param overwrite_schema:
        :return:
        None
        """
        if not spark.catalog.databaseExists(f"{catalog}.{target_database}"):
            spark.sql(f"create databse if not exists {catalog}.{target_database}")

        if not spark.catalog.tableExists(f"{catalog}.{target_database}.{target_table_name}"):
            dataframe_writer(
                data = data ,
                catalog = catalog ,
                target_database = target_database,
                target_table_name = target_table_name,
                write_mode = OVERWRITE,
                overwrite_schema = overwrite_schema ,
                partition_col = partition_col
            )
        else:
            dataframe_writer(
                data = data ,
                catalog = catalog ,
                target_database = target_database,
                target_table_name = target_table_name,
                write_mode = APPEND
            )




def blank_as_null(column:str):
    """ Replace column with empty values in to Null"""
    return when( trim(col(column)) != "" , trim(col(column)) ).otherwise(None)


def set_blank_as_null(column : Union[str , list[str]] , input_df : DataFrame):
    """

    :param column:
    :param input_df:
    :return:
    """
    result_df = input_df

    if isinstance(column , str):
        result_df = result_df.withColumn(column , blank_as_null(column))

    else:
        columns: list[str] = column
        for c in columns:
            result_df = result_df.withColumn(c , blank_as_null(c))

    return result_df


def tableExists(spark  , table_name:str):
    """ check whether the table exists in spark catalog"""
    return spark.catalog.tableExists(table_name)



def trim_and_remove_whitespaces(data_df : DataFrame ) -> DataFrame:
    for col_name in [c for c,t in data_df.dtypes if t == "string"]:
        data_df = data_df.withColumn(col_name , trim(regexp_replace(col(col_name) , "\\s+" , " ")))

    return data_df


def convert_empty_string_to_null(data_df:DataFrame):
    """
    Function to convert empty string to null values for all string column in a dataframe
    :param data_df:
    :return:
    """
    string_cols = [c for c,t in data_df.dtypes if t == "string"]
    return data_df.na.replace('',None, subset=string_cols)



def filter_by_keyy_column(input_df : DataFrame) -> DataFrame:
    """
    Method to remove all duplicate records by peid column and retain latest record

    :param input_df:
    :return:
    """
    windowSpec = Window.partitionBy("peid").orderBy(col("batch_id").desc() , col("history_flag").asc(()))
    ranked_df = input_df.withColumn("rn",row_number().over(windowSpec))
    filtered_df = ranked_df.filter(ranked_df.rn == 1).drop("rn")
    return filtered_df



def dedup_org(input_df : DataFrame) -> DataFrame:
    """
    Method to remove all duplicate records by we_org_id column and retain latest record

    :param input_df:
    :return:
    """
    windowSpec = Window.partitionBy("we_org_id").orderBy(col("source_type").ascsc())
    ranked_df = input_df.withColumn("rn",row_number().over(windowSpec))
    filtered_df = ranked_df.filter(ranked_df.rn == 1).drop("rn")
    return filtered_df






