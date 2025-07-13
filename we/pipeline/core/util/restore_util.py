from pyspark.sql import SparkSession
from pyspark.sql.functions import lower

from we.pipeline.core.util.configuration_util import (
    Configuration , to_database_name ,is_valid_environment_space
)
from we.pipeline.core.util.metadata_util import *
from we.pipeline.core.constant import SILVER_ZONE , WE_METADATA , DATA_GROUP_MD
from we.pipeline.core.util.configuration_util import  to_database_name


_DEFAULT_TABLE_FILE_LOCATION = "/FileStore/{pipeline}_table_name_config.json/"

class Restore_func:
    # class variable to keep track of whether the instance is created yet or not

    def __int__(self,spark:SparkSession , v_catalog:str , space:str,
                pipeline:str , object_type:str , layer:str , vendor:str):
        """
        This constructor to initialize object
        :param spark:
        :param v_catalog:
        :param space:
        :param pipeline:
        :param object_type:
        :param vendor:
        Returns:
            None
        """
        self.spark =spark
        self.catalog =v_catalog
        self.space =space
        self.pipeline =pipeline
        self.object =object_type
        self.layer = layer
        self.vendor =vendor
        self.config_name = _DEFAULT_TABLE_FILE_LOCATION.format(pipeline='{pipeline}')


    def read_tbl_nm(self) -> list[str]:
        """
        Retuns table name as list which need to be restored
        For ex: if layer='bronze' then the tables in bronze layer and silver layer will be provided
        Args:

        Returns:
            the tablename in an array
        """
        tbl_lst = []
        config = Configuration(self.spark , self.config_name)
        layer_tbl_name  = config.get(self.object)
        for index,layer_name in enumerate(lst_layer_name):
            if layer_name != self.layer:
                continue
            while(index < len(lst_layer_name)):
                layer_name = lst_layer_name[index]
                tbl_name_dict = layer_tbl_name[layer_name]
                if "raw" in tbl_name_dict.keys():
                    tbl_lst.append(f"{self.catalog}.{self.space}.{tbl_name_dict['raw']}")
                if "final" in tbl_name_dict.keys() :
                    tbl_lst.append(f"{self.catalog}.{self.space}.{tbl_name_dict['final']}")
                if "reject" in tbl_name_dict.keys() :
                    tbl_lst.append(f"{self.catalog}.{self.space}.{tbl_name_dict['reject']}")
                index += 1
        return tbl_lst


    def restore_delta_tables(self, table_identifiers: list[str]):
        """
        Restores delta table to version n-1
        that is if version is 2 then it roll back to version 1
        also , it audits the rollback version and timestamp when each table is roll backed
        Args:
         table_identifiers:
            A list of table identifiers : format {zone}.{data+group}.{table_name}
            zone  => raw , bronze , silver
            data_group => cl or neustar or master
        Returns:
            None
        """
        spark = self.spark
        md_util = MetadataUtil(self.spark ,self.catalog , self.pipeline , self.vendor)
        tbl_restore_flag = 0
        for table in table_identifiers:
            if not spark.catalog.tableExists(table):
                logger.warn(f"unable to restore table as table {table} not found")
                continue

            if tbl_restore_flag == 0:
                current_version = spark.sql(f"select max(version) from (describe history {table})").first()[0]
                rollback_version = current_version - 1
                if rollback_version >= 0 :
                    logger.info(f"RESTORE TABLE {table} TO VERSION AS OF {rollback_version}")
                    spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {rollback_version}")
                    max_batch_id = spark.sql(f"select max(batch_id) from {table}").first()[0]
                    tbl_version_flg = 1
                else:
                    max_batch_tbl = spark.sql(f"select max(batch_id) from {table}").first()[0]
                    if max_batch_tbl != max_batch_id :
                        current_version = spark.sql(f"select max(version) from (describe history {table})").first()[0]
                        rollback_version = current_version - 1

                        if rollback_version >= 0:
                            logger.info(f"RESTORE TABLE {table} TO VERSION AS OF {rollback_version}")
                            spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {rollback_version}")


            for index , layer_name in enumerate(lst_layer_name):
                if layer_name == self.layer:
                    break

                while(index < len(lst_layer_name)):
                    self.layer = lst_layer_name[index]
                    if self.layer.split("_")[0] == "silver":
                        md_util.update_batch_id_by_layer(self.layer.split("_")[0], max_batch_id,"%")
                    else:
                        md_util.update_batch_id_by_layer(self.layer.split("_")[0], max_batch_id,self.object)
















