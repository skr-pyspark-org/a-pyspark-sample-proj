import argparse
import logging
from importlib import import_module

from we.pipeline.core.constant import LOG_FORMAT
from we.pipeline.core.util.configuration_util import SubparserBuilder

logging.basicConfig(format=LOG_FORMAT , level=logging.INFO , handlers = [logging.StreamHandler()])
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    print("hello world")
    parser = argparse.ArgumentParser()

    #Build subparsers from module

    subparsers = parser.add_subparsers()

    #Load modules to execute the Subparser decorator
    modules = [
        'we.piple.core',
        'we.piple.contact_info',
    ]

    for m in modules:
        import_module(m)

    for build_fcn in SubparserBuilder.decoratees():
        build_fcn(subparsers)


    #Parser CLI with argparser
    namespace , extra = parser.parse_known_args()
    logger.info(f"namepsace : {namespace}")
    command_line_args = vars(namespace)
    logger.info(f"command_line_args : {command_line_args}")

    etl_task_name = command_line_args["command"]
    logger.info(f"ETL task name  : {etl_task_name}")

    #import module
    mod = import_module(etl_task_name)
    #Get the function in that imported module
    #etl_process is the function in imported module
    function_name_in_imported_module = getattr(mod , "etl_process")


    if not callable(function_name_in_imported_module):
        raise ValueError(
            f"Module {etl_task_name} doesnot have function name etl_process")

    #Execute function in imported module
    keys = [k for k in command_line_args.keys() if k!="command"]
    if keys:
        function_name_in_imported_module(**command_line_args)
    else:
        function_name_in_imported_module()

