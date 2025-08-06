from argparse import ArgumentParser

from we.pipeline.core import environments , spaces
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

    task = f"we.pipeline.ad_hoc.task.ad_hoc_query_task"
    parser = subparsers.add_parser(task)
    parser.set_defaults(command = task)
    parser.add_argument('--config' , help='Configuration file')
    parser.add_argument('-e' '--env' , choices = environments , required=True , help='Environment')
    parser.add_argument('-s' '--space' , choices = spaces , required=True , help='Space')
    parser.add_argument('-t','--object-type' , default='' , help='Object type' )
    parser.add_argument('-z','--zone' , default='' , help='Zones' )
    parser.add_argument('-q','--query' , default='' , help='Query to be executed' )
    parsers.append(parser)

    return parsers





