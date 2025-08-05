from argparse import ArgumentParser

from we.pipeline.core.constant import environments , spaces
from we.pipeline.core.util.configuration_util import  SubparserBuilder


@SubparserBuilder
def build_subparsers(subparsers) -> list[ArgumentParser]:
    """
    Build subparsers for the tasks in this module
    :param subparsers: A subparser object from argparse.ArgumentParser.add_subparsers()
    :return: List of ArgumentParser
    """
    parsers: list[ArgumentParser] = []
    parser: ArgumentParser

    _CONFIG="--config"
    _ENV="--env"
    _SPACE="--space"
    _BUCKET="--buckeyt"
    _CATALOG="--catalog"
    _BATCH_ID="--batch-id-column"
    _BATCHES="--batches"
    config_help_desc = "Configuration file"

    task = f"we.pipeline.contact_info.task.raw.ci_autoloader_task"
    parser = subparsers.add_parser(task)
    parser.set_defaults(command = task)
    parser.add_argument(_CONFIG , help=config_help_desc)
    parser.add_argument('-e' ,_ENV, choices = environments , required=True , help='Environment')
    parser.add_argument('-s' , _SPACE , choices = spaces , required=True , help='Space')
    parser.add_argument('-b' , _BUCKET , help="S3 bucket")
    parser.add_argument('-c',_CATALOG , help='catalog' )
    parser.add_argument('-I',_BATCH_ID , help='batch_id' )
    parser.add_argument('-q',_BATCHES , help='no of batches to load oldest first' )
    parsers.append(parser)


    task = f"we.pipeline.contact_info.task.bronze.ci_std_task_1"
    parser = subparsers.add_parser(task)
    parser.set_defaults(command = task)
    parser.add_argument(_CONFIG , help=config_help_desc)
    parser.add_argument('-e' ,_ENV, choices = environments , required=True , help='Environment')
    parser.add_argument('-s' , _SPACE , choices = spaces , required=True , help='Space')
    parsers.append(parser)

    task = f"we.pipeline.contact_info.task.bronze.ci_std_task_2"
    parser = subparsers.add_parser(task)
    parser.set_defaults(command=task)
    parser.add_argument(_CONFIG, help=config_help_desc)
    parser.add_argument('-e', _ENV, choices=environments, required=True, help='Environment')
    parser.add_argument('-s', _SPACE, choices=spaces, required=True, help='Space')
    parsers.append(parser)

    task = f"we.pipeline.contact_info.task.silver.ci_resolve_id"
    parser = subparsers.add_parser(task)
    parser.set_defaults(command=task)
    parser.add_argument(_CONFIG, help=config_help_desc)
    parser.add_argument('-e', _ENV, choices=environments, required=True, help='Environment')
    parser.add_argument('-s', _SPACE, choices=spaces, required=True, help='Space')
    parsers.append(parser)

    return parsers





