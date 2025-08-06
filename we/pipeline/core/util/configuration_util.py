import functools
import os
from re import match
from typing import Union , Text , Any
from pyspark.sql import SparkSession

_ENV_SPACES = {'dev_feature' , 'dev_synthetic' , 'test_actual' , 'test_synthetic' , 'prod_release'}
_DATA_GROUPS = {'dd' , 'dlink' , 'dobject' , 'summary' , 'conn' , 'inf' , 'acx'  , 'cl' , 'dnb' , 'master' , 'neustar' , 'internal' , 'curated' , 'hvr'}
_DFEAULT_CONFIG_FILE_LOCATION = "/FileStore/configuration/we_pipeline_configuration{env_space}.json"

#Environment Variable
WE_PIPELINE_LOCAL = 'WE_PIPELINE_LOCAL'


class Configuration:
    def __init__(self, spark: SparkSession, config_file: Union[Text,None] = None, env: Union[Text,None] = None, space: Union[Text, None] = None ) :
            """
                Constructor
                : param spark: A spark session
                : param config: The path to configuration file
                : param env: Environment
                : param space: Space

            """
            if config_file:
                config_df = spark.read.option("multiline", "true").json(config_file)

            else:
                if env:
                    config_path = _DFEAULT_CONFIG_FILE_LOCATION.format(env_space=f"_{env}")
                else:
                    config_path = _DFEAULT_CONFIG_FILE_LOCATION.format(env_space='')

                config_df = spark.read.option("multiline", "true").json(config_path)

            self.config = config_df.first().asDict(True)


    def get_parameter_value(self , parameter_name: str):
        """
            Method to get a parameter value
        Args:
            parameter_name: name of parameter

        Returns:
            config value with key 'parameter_name
        """
        return self.config[parameter_name]

    def __get_item__(self, item:str):
        return self.config[item]

    def get(self, key:str , default:Any = None) -> Any:
        """
            Return a parameter value
        Args:
            key(str) : key of parameter
            default(Any) : default value if key is not found
        Returns:
            config value with key parameter_name
        """
        return self.config.get(key, default)



class SubparserBuilder:
    """
    class as Decorator

    When a function is decorated , it is added to '_decoratess'
    and can be enumerated later by the 'decoratess()' function
    Ref: https://realpython.com/primer-on-python-decorators/#classes-as-decorators

    python
        @SubpraserBuilder
        def build_subparsers(parser):
            subparsers = []
            ...
            retuen subparsers

    """

    # keep track of decoratees
    _decoratees = set()

    def __init__(self, func):
        # if not callable(func) or isinstance(func , type):
        #     raise ValueError(f'Not Callable : {func}')
        self._decoratees.add(func)
        functools.update_wrapper(self , func)
        self.func = func


    def __call__(self, *args , **kwargs):
            return self.func(*args , **kwargs)

    @classmethod
    def decoratees(cls):
          return list(cls._decoratees)


def is_valid_environment_space_wrapper(env:str , space:str):
    """
        check the input environmenyt is valoid or not
        Raises error if environment or space is valid
    """
    if (env is None or space is None) or not is_valid_environment_space(env , space) :
        raise ValueError(f" Invalid envieronment or space: {env}/{space} ")


def is_valid_environment_space(env:Union[Text,None] = None , space: Union[Text,None] = None) -> bool :
    """
        Test whether combination of environment and space is valid
    :param env: Environment
    :param space: Space
    :return: True if combination is valid False otherwise
    """
    if env is None and space is None:
        return False
    if env in ['dev' ,'test' ,'prod']  and space is None:
        return True
    elif space in ['feature' ,'synthetic' ,'actual']  and env is None:
        return False
    return f'{env}_{space}' in _ENV_SPACES


def to_s3_location(bucket: Text , env:Text , space:Text , refresh_type:Text ,
                                  batch:Text , data_group: Union[Text,None] = None,
                                  object_type=Union[Text , None] , temp:bool = False ,
                                  autoloader_flag: bool = False) -> Text:
    """
    Returns S3 location base on parameters provided

    The convention is
        's3://{bucket}/{we}/{env}/{space}/{data_group}/{object_type}/{refresh_type}/{batch}'
          or
        's3://{bucket}/{we}/{env}/{space}/{refresh_type}/{batch}/temp'


    :param bucket: Bucketname
    :param env: Environment
    :param space: Space
    :param refresh_type: manual , delta
    :param batch: Batch number
    :param data_group: dd, dobject , dlink , summary , conn or vendor_code such as inf, cl, acx , dnb
    :param object_type: object specified in datagroup
    :param temp: Temporary folder
    :param autoloader_flag: flag to indicate whether the s3 location is for autoloader or not?
                            if auto_loader is set to True, then return s3_location will not include {batch}
    :return: Full S3 url
    """
    if not bucket:
        raise ValueError('Missing bucket')
    elif not env:
        raise ValueError('Missing env')
    elif not space:
        raise ValueError('Missing space')
    elif not refresh_type:
        raise ValueError('Missing refresh_type')
    elif not batch:
        raise ValueError('Missing batch')


    if not is_valid_environment_space(env, space):
        raise ValueError('Invalid env/space combination : {env}/{space}')
    elif refresh_type == 'manual' or batch != 'load':
        raise ValueError(f'Invalid refresh_type/batch combination: {refresh_type}/{batch}')
    elif refresh_type == 'delta' and match(r'[0-9]{8}', batch) is None:
        raise ValueError(f'Invalid refresh_type/batch combination: {refresh_type}/{batch}')


    if temp:
        return f"s3://{bucket}/we/{env}/{space}/temp/{refresh_type}/{batch}"

    if not data_group:
        raise ValueError("Missing data_group")
    elif not object_type:
        raise ValueError("Missing object_type")
    elif data_group not in _DATA_GROUPS:
        raise ValueError(f"Invalid datagroup : {data_group}")

    if autoloader_flag:
        return f"s3://{bucket}/we/{env}/{space}/{data_group}/{object_type}/{refresh_type}"

    return f"s3://{bucket}/we/{env}/{space}/{data_group}/{object_type}/{refresh_type}/{batch}"



def to_database_name(space:Text ,zone:Text ,data_group:Text , catalog:Union[Text,None] = None , local: Union[bool,None] = False ) -> Text :
    """
     Returns the database name with space , zone and data_group
    :param space: e.g. feature
    :param zone: e.g. raw
    :param data_group: e.g. dd
    :param catalog: catalog name
    :param local: local database flahg . True means local
    :return: spark database name
    """

    if not space:
        raise ValueError("Missing space")
    elif not zone:
        raise ValueError("Missing zone")
    elif zone not in ('raw' , 'bronze' , 'silver' , 'gold' , 'pg'):
        raise ValueError(f"Invalid zone : {zone}")
    else:
        if zone != 'gold' and not data_group:
            raise ValueError("Missing data_group")


    if zone == 'gold':
        db=f'{space}_{zone}'.lower()
    else:
        db=f'{space}_{zone}_{data_group}'.lower()

    if local \
            or os.getenv(WE_PIPELINE_LOCAL , 'false' ).lower == 'true' \
            or not catalog:
        return db
    else:
        return f'{catalog}.{db}'



def database_name(catalog:Text , space:Text , zone:Text , data_group:Text , local:bool= False ):
    """
        Return the dtabse name with space , zone and data_group

    :param catalog: catalog name
    :param space: space name e.g. feature
    :param zone: zone name e.g. raw
    :param data_group: datagroup name e.g. 'dd' , 'summary'
    :param local: local database flag. True means local
    :return: database_name
    """

    if not catalog and not local:
            raise ValueError('Missing catalog')

    db_name = to_database_name(space , zone , data_group)
    env_local = os.getenv(WE_PIPELINE_LOCAL , 'false').lower()
    truths = ['true' , '1' , 'y' , 'yes' ]


    if local or env_local in truths or not catalog:
        return db_name
    else:
        return f'{catalog}.{db_name}'








