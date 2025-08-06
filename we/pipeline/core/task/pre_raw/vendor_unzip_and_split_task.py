import logging
import os
import subprocess

import boto3
from pyspark.sql import SparkSession

from we.pipeline.core.constant import *
from we.pipeline.core.util.configuration_util import   Configuration ,  is_valid_environment_space_wrapper
from we.pipeline.core.util.metadata_util import  MetadataUtil
from we.pipeline.core.util.spark_util import  load_script_from_package

logger = logging.getLogger(__name__)


def etl_process(**options):
    """
    This task read compressed file from vendor-spaecific bucket , unzip it , read the batch_id
    from metadata table , partitions the input files in to a number of pieces and pushes it into target
    bucket when the autoloader library points . when the copy file is finished , move the vendor
    source file to the archive folder
    :param options:
    :return:
    """
    env = options.get(ENV_OPTION)
    space = options.get(SPACE_OPTION)
    PARTITION_FILE_NUMBERS = options.get(PARTITION_FILE_NUMBER)
    DATA_GROUP = options.get(DATA_GROUP_OPTION)
    OBJECT_TYPE = options.get(OBJECT_TYPE_OPTION)
    PIPELINE = options.get(PIPELINE_OPTION)
    VENDOR = options.get(VENDOR_OPTION)
    COMPRESSION_FORMAT = options.get(COMPRESSION_FORMAT_OPTION)


    print(env , space , PARTITION_FILE_NUMBERS ,DATA_GROUP ,OBJECT_TYPE, PIPELINE, VENDOR, COMPRESSION_FORMAT)
    """
    Raise Exception , if environment or spcae is in valid
    """
    is_valid_environment_space_wrapper(env , space)
    spark  = SparkSession.builder.appName("VENDOR_ETL_JOB").getOrCreate()
    config = Configuration(spark , options.get(CONFIG_OPTION) , env=env ,  space=space)
    catalog = config.get(CATALOG_OPTION)
    bucket = options.get(BUCKET_OPTION) or config.get(BUCKET_OPTION)
    md_util = MetadataUtil(spark , catalog , space , PIPELINE , VENDOR)


    current_task = UNZIP_AND_SPLIT_TASK.get(NAME)
    previous_task = DOWNLOAD_TASK.get(NAME)



    #skip, if the current_task is laready executed
    task_executed , current_batch_id , previous_batch_id = md_util.get_task_info(
        current_object_type= OBJECT_TYPE,
        current_layer=PROCESSED,
        current_task=current_task,
        previous_object_type=OBJECT_TYPE,
        previous_layer=PROCESSED,
        previous_task=current_task
    )

    print("current_batch_id  ::: ",{current_batch_id})
    print("previous_batch_id ::: ",{previous_batch_id})


    if task_executed:
        return


    __VENDOR_S3_TARGET_BUCKET = f'{bucket}'
    __VENDOR_S3_UNPROCESSED_FODLER = f'we/env/space/{DATA_GROUP}/unprocessed/{OBJECT_TYPE}/{DELTA}/{current_batch_id}'
    __VENDOR_S3_PROCESSED_FODLER = f'we/env/space/{DATA_GROUP}/processed/{OBJECT_TYPE}/{DELTA}/{current_batch_id}'
    __VENDOR_S3_TARGET_FOLDER = f'we/{env}/{space}/{DATA_GROUP}/{OBJECT_TYPE}/{DELTA}'
    _TEMP_DIR = options.get(DIR_OPTION) or config.get(DIR_OPTION)

    sh_file = __VENDOR_MULTI_FILEP_SH_FILE if OBJECT_TYPE in __MULTI_FILE_PIPELINE_LIST else __VENDOR_SH_FILE
    logger.info(f"shell selected for object_type {OBJECT_TYPE} is {sh_file}")

    load_script_from_package(__VENDOR_SH_PACKAGE , sh_file , _TEMP_DIR)

    # initiate s3 resource
    s3 = boto3.resource("s3")


    # select bucket
    source_bucket = s3.Bucket(__VENDOR_S3_TARGET_BUCKET)
    target_bucket = s3.Bucket(__VENDOR_S3_TARGET_BUCKET)


    # download file into current directory
    for s3_object in source_bucket.objects.filter(Prefix = __VENDOR_S3_UNPROCESSED_FODLER):
        filename_zip = s3_object.key
        logger.info(f"file processing starts for ",_TEMP_DIR , filename_zip)
        filename = filename_zip.replace(__VENDOR_S3_PROCESSED_FODLER,'')

        if len(filename) > 0:
            filename_prefix = filename_zip.replace(__VENDOR_S3_TARGET_FOLDER,"")

            source_bucket.download_file(filename_zip , _TEMP_DIR + filename)
            logger.info(" filename is ::: ",filename)
            logger.info(" filename_prefix is ::: ",filename_prefix)
            logger.info(" current_batch_id is ::: ",{current_batch_id})
            logger.info(" PARTITION_FILE_NUMBERS is ::: ",PARTITION_FILE_NUMBERS)
            logger.info(" _TEMP_DIR ::: ",_TEMP_DIR)
            logger.info(" COMPRESSION_FOTMAT is::: ",COMPRESSION_FORMAT)
            return_code = subprocess.call(
                            [f'bash {_TEMP_DIR}{sh_file} %s %s %s %s %s %s' % (
                                filename , filename_prefix , current_batch_id , PARTITION_FILE_NUMBER , _TEMP_DIR , COMPRESSION_FORMAT
                            )] ,
                shell=True
            )

            if return_code != 0 :
                raise ValueError("Bash script failed with return code - {return_code}")



            logger.info("File Processing completed for ", filename)

            batch_id_folder = current_batch_id

            try:
                logger.info()


