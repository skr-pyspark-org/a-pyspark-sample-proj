from typing import Dict, Any

from we.pipeline.contact_info.constant import ( md_options_ci_common , md_options_ci_autoloader , source_file_options )
from we.pipeline.contact_info.task.raw import ci_raw_table

from we.pipeline.core.constant import (
    OBJECT_TYPE_OPTION,
    SCHEMA_FILE_OPTION,
    OBJECT_TYPE_CONTACT_INFO
)

from we.pipeline.core.util.autoloader_util import invoke_autoloader
from we.pipeline.core.util.common_util import  merge_dicts


def etl_process(**options):
    print(f"autoloader options 1st ::: {options}")

    source_options: Dict[str,Any] = source_file_options
    source_options.update({
        SCHEMA_FILE_OPTION : "ci_neustar_schema.json"
    })

    options = merge_dicts(options , md_option_ci_common , md_option_ci_autoloader)
    options.update(
        {
            OBJECT_TYPE_OPTION : OBJECT_TYPE_CONTACT_INFO
        }
    )
    print(f"autoloader options 2nd ::: {options}")

    invoke_autoloader(source_file_options=source_options , target_table=ci_raw_table , **options)






