import logging
import types
from typing import Union

from pyspark.sql import SparkSession

from we.pipeline.core.constant import LOG_FORMAT
from we.pipeline.core.util.common_util import get_current_timestamp

logging.basicConfig(format=LOG_FORMAT , level=logging.info)
logger = logging.getLogger(__name__)


STARTED = "Started"
SUCCEED = "Succeed"
FAILED = "Failed"

def create_log_table(spark:SparkSession , w_catalog_db: str , log_table_name:str , path: Union[str,None]):
    """
        This functin creates a log tabel for trackking status of address regularization task
    :param spark:
    :param w_catallog_db:
    :param log_table_name:
    :param path:
    :return:
    """
    try:
        location = ''
        if path:
            location = f"LOCATION '{path}'"
        spark.sql(f"""
                create table if not exists {w_catalog_db}.{log_table_name}(
                    run_id string,
                    partition_id int,
                    status string,
                    long_msg string,
                    start_time timestamp,
                    end_time timestamp,
                    duration_in_seconds long
                )
                PARTITIONED BY (run_id)
                {location}
            """)
    except Exception as error:
        logger.error(f"Error creating '{log_table_name} - {str(error)}")



def log_start(spark:SparkSession , log_attrs: types.SimpleNamespace):
    """
    This function inserts a job start record in log table
    :param spark:
    :param log_attr:
    :return:
    """
    run_id = log_attrs.run_id
    partition_id = log_attrs.partition_id
    status = log_attrs.status
    log_msg = log_attrs.log_msg
    start_time = get_current_timestamp()
    end_time = 'null'
    duration_in_seconds = 'null'
    w_catalog_db = log_attrs.w_catalog_db
    log_table = log_attrs.log_table

    try:
        spark.sql(f""""
            insert into {w_catalog_db}.{log_table} values (
             '{run_id}',
             '{partition_id}',
             '{status}',
             '{log_msg}',
             '{start_time}',
             '{end_time}',
             '{duration_in_seconds}' )
        """)
    except Exception as error:
            error_detail_description = (f"""
                Error - log start failed - {str(error)}
                run_id : {run_id}
                partition_id : {partition_id}
            """)
            logger.error(error_detail_description)


def log_update(spark:SparkSession , log_attrs: types.SimpleNamespace):
    """
        This function updates the status of job in log table
    :param spark:
    :param log_attrs:
    :return:
    """
    run_id = log_attrs.run_id
    partition_id = log_attrs.partition_id
    status = log_attrs.status
    log_msg = log_attrs.log_msg
    end_time_str = get_current_timestamp()
    end_time = f"to_timestamp('{end_time_str}')"
    duration_in_seconds = f"cast(to_timestamp('{end_time_str}') as long) -  cast(start_time as long))"
    w_catalog_db = log_attrs.w_catalog_db
    log_table = log_attrs.log_table

    try:
        spark.sql(f""""
            update {w_catalog_db}.{log_table} set 
                                status= '{status}' , 
                                log_msg = "{log_msg}" , 
                                end_time = {end_time}
                                duration_in_seconds = {duration_in_seconds}
                    where run_id = '{run_id}' and partition_id = {partition_id}
        """)
    except Exception as error:
            error_detail_description = (f"""
                Error - log update failed - {str(error)}
                run_id : {run_id}
                partition_id : {partition_id}
            """)
            logger.error(error_detail_description)