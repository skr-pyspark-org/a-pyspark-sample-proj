from typing import Dict , Any

from we.pipeline.core.constant import *

source_file_options : Dict[str , Any] = {
        SCHEMA_PATH_OPTION : SCHEMA_CONTACT_INFO,
        SCHEMA_FILE_OPTION : 'ci_neustar_schema.json',
        REFRESH_TYPE_OPTION: DELTA,
        SOURCE_FILE_FORMAT_OPTION: CSV,
        SOURCE_FILE_HEADER_OPTION: "false",
        SOURCE_FILE_DELIMITER_OPTION: PIPE,
        ROWS_TO_SKIP_OPTION: 0,
        OUTPUT_MODE_OPTION: APPEND
}


md_options_ci_common: Dict[str,Any] = {
    PIPELINE_NAME_OPTION: PIPELINE_CONTACT_INFO,
    VENDOR_NAME_OPTION: VENDOR_NEUSTAR,
    DATA_GROUP_OPTION: DATA_GROUP_NEUSTAR
}


md_options_ci_autoloader: Dict[str,Any] = {
    LAYER_OPTION: BRONZE_ZONE,
    PREVIOUS_LAYER_OPTION: RAW_ZONE,
    PREVIOUS_TASK_NAME_OPTION: AUTOLOADER_TASK.get(NAME)

}

