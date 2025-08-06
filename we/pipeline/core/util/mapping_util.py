from pathlib import Path
from dataclasses import dataclass
from typing import Union

from pyspark.sql import SparkSession
import logging
from pyspark.sql.types import StructType
from we.pipeline.core.constant import (
LOG_FORMAT,
)
REGULARISE_ADDRESS_SCHEMA = 'regularise_address_schema.json'

logging.basicConfig(format=LOG_FORMAT , level=logging.INFO)
logger  = logging.getLogger(__name__)

@dataclass
class Mapping:
    """
    Model of the mapping file

    Each entry contains following file
    * dataset_1 (str) -> the name of the source dataset
    * section (int) -> the section in dataset_1
    * field_1 (str) -> the name of the source filed
    * type_1 (str) -> the type of the source field . default to str
    * dataset_2 (str) -> the name of the destination dataset
    * field_2 (str) -> the name of the destination field
    * type_2 (str) -> the type of the destination fields. default to str
    """

    def __init__(self , rows: list[dict]):
        """
        Constructor. The entries are sorted by dataset_1 , section
        :param rows: a list of dictionary objects
        """
        self.entries  = rows.copy()

        for e in self.entries:
            if e['section'] is None or e['section'] == '':
                e['section'] = 0

        self.entries = sorted(self.entries , key=lambda entry: (entry['dataset_1'] , entry['section']))


    def __iter__(self):
            return iter(self.entries)

    def __str__(self):
            total_entries = 0 if self.entries is None else len(self.entries)
            return f'Mapping({total_entries}) entries'


    def get_mapping(self , dataset1:str , dataset2:str , section:Union[int,None] = None):
        """
        Return a Mapping object containing a subset of entries that satisfy the filter criteria
        :param dataset1: dataset_1
        :param dataset2: dataset_2
        :param section: section (optional)
        :return: Amapping object
        """
        items = list(self.filter_entries(dataset1 , dataset2 , section))
        return Mapping(items)

    def filter_entries(self,  dataset1:str , dataset2:str , section:Union[int,None] = None):
        """
        Return a Mapping object containing a subset of entries that satisfy the filter criteria
        :param dataset1: dataset_1
        :param dataset2: dataset_2
        :param section: section (optional)
        :return: Amapping object
        """
        def matcher(r: dict) -> bool:
            return r['dataset_1'] == dataset1 \
                   and r['dataset_2'] == dataset1 \
                   and (section is None or r['section'] == section)
        return filter(matcher , self.entries)



    def __len__(self):
        return len(self.entries)




def load_mapping(spark:SparkSession ,
                 mapping:Union[str , Path , DataFrame],
                 schema: Union[StructType , str] = None) -> Mapping:
    """
    Load fields mapping

    :param spark: spark session
    :param mapping: the field ampping a table_name , path or dataframe
    :param schema: the schema
    :return: a mapping object
    """
    ds: DataFrame

    if isinstance(mapping , str):
        logger.info('Loading mapping from table : %s',mapping)
        ds = spark.sql(f"select * from {mapping}")

    elif isinstance(mapping, Path):
        logger.info('Loading mapping from table : %s', mapping)
        if schema is None:
            reader = spark.read.option('header',True)\
                               .option('inferSchema', True)\
                               .format('csv')
        else:
            if schema is None:
                reader = spark.read.option('header', True) \
                                    .format('csv')\
                                    .schema(schema)

        ds= reader.load(mapping.as_posix())
    else:
        logger.info("loading mapping from datasetr")
        ds = mapping

    entries = [row.asDict() for row in ds.toLocalIterator()]
    logger.info("Loaded %d mapping entries ",len(entries))

    return Mapping(entries)





