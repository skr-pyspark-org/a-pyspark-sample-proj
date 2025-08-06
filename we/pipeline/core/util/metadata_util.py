import logging
from typing import Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower


from we.pipeline.core.constant import (
SILVER_ZONE,
WE_METADATA,
DATA_GROUP_MD,
LOG_FORMAT,
DEFAULT_BATCH_ID,
PIPELINE_PERSON_IDENTITY,
PIPELINE_URPP,
PIPELINE_NEW_PROFILES
)

from we.pipeline.core.util.configuration_util import to_database_name
from we.pipeline.core.util.error_handlers import NoMetadataRecordFoundError

logging.basicConfig(format=LOG_FORMAT , level=logging.INFO)
logger  = logging.getLogger(__name__)

def internal_pipeline_status(df):
    succeeded = False
    batch_id = None
    if not df.isEmpty() and df.count() == 1:
        succeeded =True
        batch_id = df.first().asDict().get("batch_id")
    return succeeded,batch_id

class MetadataUtil:
    # class variable __instance will keep track of whether the instance is created yet or not
    __instance = None

    def __new__(cls,spark:SparkSession, v_catalog: str , space:str ,
                    pipeline:str , vendor:str , local: bool = False):

        """
        A statis method to control hoe the class is instantiated .
        This method make sure the class is instanctiated only once per run

        Python call the __new__() method every time you instanticate a class. it does the instantiation in two steps

        1. First , it invokes the __new__() method of the class to create and return an instance
                    (this instance is then passed to __init__() as its first argument self)
        2. Next , the __init__() method is invoked to initialize the object state . Please
                    note the __init__() method can't return anything
        """
        if cls.__instance is None:
            cls.__instance = super(MetadataUtil , cls).__new__(cls)

        return cls.__instance



    def __init__(self, spark:SparkSession , v_catalog:str , space:str, pipeline:str , vendor:str , local:bool = False):
        """
        The constructor to initialize the object

        v_catalog : catalog name
        space : space name
        pipeline: pipeline name
        vendor : vendorname
        local: an optional boolean flag to indicate whether it is local env or not
        Returns:
                None
        """
        self.spark = spark
        self.catalog = v_catalog
        self.space = space
        self.pipeline = pipeline.lower()
        self.vendor = vendor.lower()
        self.db_name = to_database_name(space , SILVER_ZONE , DATA_GROUP_MD)
        self.md_table = f'{v_catalog}.{self.db_name}.{WE_METADATA}'
        self.local = local


    def create_md_table(self):
        """
        create metadata table if not exists
        :return:
        """
        self.spark.sql(f"create schema if not exists {self.catalog}.{self.db_name}")
        self.spark.sql(
            f"""
              create table if not exists {self.md_table}
              (
                pipeline string , vendor string , object_type string , batch_id int ,
                layer string , task string , description string 
              )
        
        """)


    def read_batch_id(self, object_type:str , layer:str , task:str , description: str='Na'):
        """
        Returns batch_id for given pipeline , vendor , object_type , layer and task
        """
        df = self.spark.sql(f"""
                        select batch_id from {self.md_table} 
                        where lower(vendor) = '{self.vendor}' and
                        where lower(object_type) = '{object_type.lower()}' and
                        where lower(layer) = '{layer.lower()}' and
                        where lower(task) = '{task.lower()}' and                        
                """)

    def read_batch_id_by_pipeline(self,pipeline:str , layer : str , task : str):
        """
        Returns a list of batch_id for a given pipeline , layer task
        :param pipeline:
        :param layer:
        :param task:
        :return:
        """
        df = self.spark.sql(f"""
                    select batch_id from {self.md_table} where lower(pipeline)= '{pipeline.lower()}'and 
                                                               lower(layer) = '{layer.lower()}' and 
                                                               lower(task) = '{task.lower()}' and 
                                                               
                """)

    def insert_md_record(self , object_type:str , batch_id:int , layer:str , task:str,
                                    description:str):
        """
        inserts a new metadata record
        :param object_type:
        :param batch_id:
        :param layer:
        :param task:
        :param description:
        :return:
        """
        self.spark.sql(f"""
                    insert into {self.md_table}  ( pipeline , vendor , object_type , batch_id , layer , task , description )
                                                 values ('{self.pipeline}','{self.vendor}',
                                                        '{object_type.lower()}',{batch_id},
                                                        '{layer.lower()}','{task.lower()}','{description.capitalize()}')                
        """)


    def update_batch_id(self , object_type:str ,  layer:str , task:str, batch_id: int):
        """
        upfdate batch_id for given pipeline , vendor , object_type , layer and task
        :param object_type:
        :param batch_id:
        :param layer:
        :param task:
        :param description:
        :return:
        """
        # lower the values
        
        object_type = object_type.lower()
        layer = layer.lower()
        task = task.lower()

        md_df = self.spark.read.table(f"{self.md_table}")
        md_df = md_df.filter(
                            (lower(md_df.pipeline) == f'{self.pipeline}') &
                            (lower(md_df.vendor) == f'{self.vendor}') &
                            (lower(md_df.object_type) == f'{self.object_type}') &
                            (lower(md_df.layer) == f'{self.layer}') &
                            (lower(md_df.task) == f'{self.task}')
        )
        #if metadata not found raise
        if not md_df.first():
            raise ValueError(f"there is no metadata record found for pipeline")

        #EElse update row
        self.spark.sql(f"""
                    update {self.md_table} set batch_id={batch_id} where 
                                                        lower(pipeline) == '{self.pipeline}' and
                                                        lower(vendor) == '{self.vendor}' and
                                                        lower(object_type) == '{object_type}' and
                                                        lower(layer) == '{layer}' and
                                                        lower(task) == '{task}'
        """)

    def update_batch_id_by_layer(self, object_type: str, layer: str, batch_id: int):
        """
        upfdate batch_id for given pipeline , vendor , object_type , layer and task
        :param object_type:
        :param batch_id:
        :param layer:
        :param task:
        :param description:
        :return:
        """
        # lower the values

        object_type = object_type.lower()
        layer = layer.lower()

        md_df = self.spark.read.table(f"{self.md_table}")
        md_df = md_df.filter(
            (lower(md_df.pipeline) == f'{self.pipeline}') &
            (lower(md_df.vendor) == f'{self.vendor}') &
            (lower(md_df.object_type) == f'{self.object_type}') &
            (lower(md_df.layer) == f'{self.layer}')
        )
        # if metadata not found raise
        if not md_df.first():
            raise ValueError(f"there is no metadata record found for pipeline")

        # EElse update row
        self.spark.sql(f"""
                    update {self.md_table} set batch_id={batch_id} where 
                                                        lower(pipeline) == '{self.pipeline}' and
                                                        lower(vendor) == '{self.vendor}' and
                                                        lower(object_type) == '{object_type}' and
                                                        lower(layer) == '{layer}'
        """)


    def update_batch_id_by_pipeline(self, pipeline: str, layer: str,  task:str , batch_id: int):
        """
        upfdate batch_id for given pipeline , vendor , object_type , layer and task
        :param object_type:
        :param batch_id:
        :param layer:
        :param task:
        :param description:
        :return:
        """
        # lower the values

        pipeline = pipeline.lower()
        layer = layer.lower()
        task = task.lower()

        md_df = self.spark.read.table(f"{self.md_table}")
        md_df = md_df.filter(
            (lower(md_df.pipeline) == f'{self.pipeline}') &
            (lower(md_df.layer) == f'{self.layer}') &
            (lower(md_df.task) == f'{self.task}')
        )
        # if metadata not found raise
        if not md_df.first():
            raise ValueError(f"there is no metadata record found for pipeline")

        # EElse update row
        self.spark.sql(f"""
                    update {self.md_table} set batch_id={batch_id} where 
                                                        lower(pipeline) == '{pipeline}' and
                                                        lower(layer) == '{layer}' and
                                                        lower(task) == '{task}'
        """)



    def get_task_info(self, current_object_type:str , current_layer: str , current_task:str,
                            previous_object_type: str , previous_layer:str,previous_task:str) -> Tuple(bool , int , int):
        """
        Checks whether the current layer's task is executed yet or not and returns
        task_executed_status , new_batch_id and previous_batch_id
        :param current_object_type:
        :param current_layer:
        :param current_task:
        :param previous_object_type:
        :param previous_layer:
        :param previous_task:
        :return:
         A tuple with task executed task , new_batch_id and previous_batch_id
        """
        new_batch_id = self.read_batch_id(previous_object_type , previous_layer , previous_task)
        previous_batch_id = self.read_batch_id(current_object_type , current_layer , current_task)
        logger.info(f"new_batch_id : {new_batch_id} , previous_batch_id : {previous_batch_id}")

        task_executed_status = self.get_task_executed_status(new_batch_id , previous_batch_id)

        return task_executed_status , new_batch_id , previous_batch_id


    @staticmethod
    def get_task_executed_status(new_batch_id:int , previous_batch_id:int) -> bool:
        """
        check whether the current task is executed or not
        :param new_batch_id:
        :param previous_batch_id:
        :return: the status of whether the current task is executed for the new_batch_id or not
        """
        if previous_batch_id < new_batch_id:
            logger.info("the current task will proceed to execute .....")
            return False
        elif new_batch_id == previous_batch_id:
            logger.info(" the curent task is already executed for given batch_id : {new_batch_id}")
            return True
        else:
            raise ValueError("The new_batch_id is invalid , new_batch_id cannot be grater than previous_batch_id")


    def get_internal_pipeline_status(self, spark:SparkSession):
        """
         Method to validate internal execution status before executing DDBP .
         Below pipeline batch_id are same , then it got succeeded else failed
         1.pip
         2.urpp
         3.npcp
        :param spark:
        :return:
        """
        df = spark.sql(f"""
                      select distinct batch_id from {self.md_table} where pipeline in ('{PIPELINE_URPP}')
                    """)
        succeeded , batch_id  = internal_pipeline_status(df)
        return succeeded , batch_id
