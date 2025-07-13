import os
import types
from datetime import datetime
from typing import Dict,Union


from we.pipeline.core.constant import COMMA , PIPE ,  ACK , TAB , CARET , COLON , SPACE_SYNTHETIC , ENV_DEV

def get_current_timestamp():
    """
    Returns:
        str: This function returns current timestamp in YYYY-MM-DD HH:MI:SS.%f format
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")



def get_simple_namespace(input_dict) -> types.SimpleNamespace :
    """
    This function converts input dictionary into a simple namespace
    Args:
        input_dict: dict
            An input dictionary to convert into namespace
    Returns:
        types.SimpleNamespace: a converted simple namespace
    """
    if not isinstance(input_dict , dict):
        return types.SimpleNamespace()

    return types.SimpleNamespace(**input_dict)




def get_delimiter(name:str):
    """
    Returns the delimiter value for given delimiter name
    """

    delimiter = None
    if name == COMMA or name == 'COMA':
        delimiter = ","
    elif name == PIPE:
        delimiter = "|"
    elif name == ACK :
        delimiter = "\u0006"
    elif name == TAB:
        delimiter = "\t"
    elif name == CARET :
        delimiter = "^"
    elif name == COLON:
        delimiter = ":"
        
    return delimiter



def get_module_name(module_path:str):
    """
    Returns the name of module
    """
    return str(os.path.splitext(os.path.basename(module_path))[0]).lower()



def get_app_name(pipeline:str , object_type:str, task:str) -> str:
    """
    Returns the app name by concatenating pipeline , object_type and task
    Args:
        pipeline: real_estate
        object_tyep: property
        task: re_prop_autoloader_task
    """
    return '/'.join(name for name in [pipeline , object_type , task] if name)


def merge_dicts(*dict_args) -> Dict:
    """"
        This function merges an arbitary number of dictionaries

        Parameters:
            *dict_args: Dict
            An arbitary number of dictionaries to merge

        Returns:
            Dict: rreturns a merged python dictionary
    """
    result: Dict = {}
    for dictionary in dict_args:
        result.update(dictionary)

    return result


def is_synthetic_testing(env , space):
    return env == ENV_DEV and space == SPACE_SYNTHETIC

def convert_string_to_bool(input_val:Union[str,bool]) -> bool :
    """
    This function converts input string bool on to bool
    Args:
        input_val (str or bool) : an input value to convert into bool
    Returns:
        bool: Returns a boolean if the input value is 'true' or 'True' or 'Y' or 'y'
                else False
    """
    try:
        if type(input_val) == bool:
            input_val = str(input_val)

        input_val = input_val.lower().strip()
        return input_val == "true" or input_val == "y" or input_val == "yes"

    except AttributeError:
        return False











