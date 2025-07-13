from typing import Type

def create_spark_session(app_name:str , master:str , options:dict = None):
    """
    create a spark session
    :param app_name:
    :param master:
    :param options:
    :return:
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    builder = builder.config("spark.ui.showConcoleProgress","false")

    if options is not None:
        for k , v in options.items():
            builder = builder.config(k,v)

    return builder.getOrCreate()



def ignore_warnings(category: Type[Warning]):
    """
    Decorator to ignore warnings e.g 'ResourceWarning'
    Ref :- https://stackoverflow.com/a/5929165/415701

    Example
        @ignore_warnings(ResourceWarning)
        def myfunc(x , y , **kwargs):
            pass

    :param category:
    :return:
    """
    def decorator(fcn):
        def wrapper(*args , **kwargs):
            import warnings
            with warnings.catch_warnings():
                warnings.simpleFilter("ignore" , category)
                fcn(*args , **kwargs)

        return wrapper

    return decorator