import logging
from typing import Dict , Any
from pyspark.sql import SparkSession

from we.pipeline.core.constant import (
    CONFIG_OPTION,
    ENV_OPTION,
    SPACE_OPTION,
    BUCKET_OPTION,
    CATALOG_OPTION,
    ROWS_TO_SKIP_OPTION,
    DATA_GROUP_OPTION,
    REFRESH_TYPE_OPTION,
    OBJECT_TYPE_OPTION,
    SCHEMA_PATH_OPTION,
    SCHEMA_FILE_OPTION,
    SOURCE_FILE_FORMAT_OPTION,
    SOURCE_FILE_HEADER_OPTION,
    SOURCE_FILE_DELIMITER_OPTION,
    OUTPUT_MODE_OPTION,
    PIPELINE_NAME_OPTION,
    VENDOR_NAME_OPTION,
    LOG_FORMAT,
    PREVIOUS_TASK_NAME_OPTION,
    PREVIOUS_OBJECT_TYPE_OPTION,
    LAYER_OPTION,
    PREVIOUS_LAYER_OPTION,
    AUTOLOADER_TASK,
    NAME,
    ROW_TAG,
    GENERATE_HASH_KEY_FLAG
)

from we.pipeline.core.task.auto_loader_lib import AutoLoader
from we.pipeline.core.util.common_util import get_app_name
from we.pipeline.core.util.configuration_util import (
    Configuration,
    to_database_name,
    to_s3_location,
    is_valid_environment_space
)
from we.pipeline.core.util.metadata_util import MetadataUtil
from we.pipeline.core.util.spark_util import load_schema_from_package


logging.basicConfig(format=LOG_FORMAT , level=logging.INFO)
logger = logging.getLogger(__name__)

def invoke_autoloader(source_file_options: Dict[str,Any] , target_table: str , **options):
    """
    Args:
        source_file_options: A dict of source_data options like schema , file_format
        target_table: The target table to save the source data
        **options: Alist of options flor running autoloaderr task
    Returns:
        None. It loads files into raw zone
    """
    env = options.get(ENV_OPTION)
    space = options.get(SPACE_OPTION)

    # skip if environment or space is valid
    is_valid_environment_space(env , space)

    #MD options
    current_object_type = options.get(OBJECT_TYPE_OPTION)
    current_layer = options.get(LAYER_OPTION)
    current_task = AUTOLOADER_TASK.get(NAME)
    previous_object_type = options.get(PREVIOUS_OBJECT_TYPE_OPTION) or options.get(OBJECT_TYPE_OPTION)
    previous_layer =  options.get(PREVIOUS_LAYER_OPTION) or options.get(LAYER_OPTION)
    previous_task = options.get(PREVIOUS_TASK_NAME_OPTION)
    pipeline_name = options.get(PIPELINE_NAME_OPTION)
    vendor_name = options.get(VENDOR_NAME_OPTION)


    app_name = get_app_name(pipeline_name , current_object_type , current_task)
    spark = SparkSession.builder.appName(app_name).getOrtCreate()

    config = Configuration(spark , options.get(CONFIG_OPTION) , env=env , space=space)
    catalog = options.get(CATALOG_OPTION) or config.get(CATALOG_OPTION)

    md_util = MetadataUtil(spark , catalog , space , pipeline_name , vendor_name)

    #skip if it is altready executed
    task_executed , current_batch_id , _ =md_util.get_task_info(
        current_object_type  , current_layer , current_task,
        previous_object_type , previous_layer , previous_task
    )

    if task_executed:
        return


    logger.info(f"The current_batch id is ::: {current_batch_id}")

    #Initialize other autoloader arguments
    bucket = options.get(BUCKET_OPTION) or config.get(BUCKET_OPTION)
    data_group = options.get(DATA_GROUP_OPTION)

    refresh_type = source_file_options.get(REFRESH_TYPE_OPTION)
    schema_path = source_file_options.get(SCHEMA_PATH_OPTION)
    schema_file = source_file_options.get(SCHEMA_FILE_OPTION)
    source_schema = load_schema_from_package(schema_path , schema_file)
    source_file_format = source_file_options.get(SOURCE_FILE_FORMAT_OPTION)
    source_file_header = source_file_options.get(SOURCE_FILE_HEADER_OPTION)
    source_file_delimiter = source_file_options.get(SOURCE_FILE_DELIMITER_OPTION)
    rows_to_skip = source_file_options.get(ROWS_TO_SKIP_OPTION)
    output_mode = source_file_options.get(OUTPUT_MODE_OPTION)
    row_tag = source_file_options.get(ROW_TAG, None)
    generate_hash_key_flag = source_file_options.get(GENERATE_HASH_KEY_FLAG, True)


    source_folder = to_s3_location(bucket , env, space , refresh_type , str(current_batch_id)
                                   , data_group , f"{current_object_type}" , ytemp=False , autoloader_flag=True)
    target_database = to_database_name(space , current_layer , data_group)


    # Instiantiate Autoloader
    autoloader = AutoLoader(spark=spark,
                            source_schema=source_schema,
                            source_file_format=source_file_format,
                            source_file_header=source_file_header,
                            source_file_delimiter = source_file_delimiter ,
                            source_files_path = source_folder,
                            catalog = catalog,
                            target_database = target_database,
                            target_tablename = target_table,
                            rows_to_skip = rows_to_skip,
                            batch_id = current_batch_id,
                            generate_hash_key = generate_hash_key_flag,
                            row_tag = row_tag
                            )
    #Run autoloader
    autoloader.run_autoloader()

    #Update nmetadat table with current_batch_id
    md_util.update_batch_id(object_type=current_object_type , layer=current_layer , task=current_task , batch_id=current_batch_id)













