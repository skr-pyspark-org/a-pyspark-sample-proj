import logging
import re
import boto3
import pgpy
import pysftp as pysp
from pyspark.sql import SparkSession
import subprocess


from we.pipeline.core.constant import(
    ENV_OPTION,
    SPACE_OPTION,
    CONFIG_OPTION,
    CATALOG_OPTION,
    DOWNLOAD_TASk,
    UNPROCESSED,
    CONFIG_CORE_PATH,
    NAME,
    LOG_FORMAT
)

from we.pipeline.core.util import(
    get_dbutils,
    get_local_dbutils
)

from we.pipeline.core.util.configuration_util import(
    Configuration,
    is_valid_environment_wrapper
)

from we.pipeline.core.util.metadata_util import MetadatUtil
from we.pipeline.core.util.spark_util import load_json_from_package


metadata_data_group = "md"



metadata_table = "we_metadata"

input_options = {
    "contact_info" : {
        "file_types" : ["contact_info"],
        "vendors": ["neustar"]
    }


    
}


def etl_process(**options) :
    env = options.get(ENV_OPTION)
    space = options.get(SPACE_OPTION)
    pipeline = options.get("pipeline")
    object_type = options.get("object_type")
    vendor = options.get("vendor")
    encryption_flag = options.get("encryption_flag") or 'N'


    """
    Raise Exception , if environment or space is valid
    """
    is_valid_environment_wrapper(env , space)








