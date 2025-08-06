import logging

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from we.pipeline.core.constant import (
    LOG_FORMAT , CATALOG_OPTION , WE_AUDIT_TABLE_ROLLBACK
)
from we.pipeline.core.util.configuration_util import Configuration

logging.basicConfig(format=LOG_FORMAT , level=logging.info)
logger = logging.getLogger(__name__)



def get_size_of_path(path , dbutils):
    """
        This function return the size of all the files for agiven input path
    """
    if dbutils is None:
        return 0

    return sum(file.size for file in get_all_files_in_path(path , dbutils))


def get_all_files_in_path(path , dbutils , verbose=False):
    """
        This function returns a list of FileInfo for a given input path
    [FileInfo(path="dbfs:/FileStore/configuration/er/inputs/alt_entity_resolution/sample_pm.csv",
     name="sample_pm.csv" , size=17908 , modificationTime=1679479486000)]
    """
    nodes_new = dbutils.fs.ls(path)

    files = []
    while len(nodes_new) > 0:
        current_nodes = nodes_new
        nodes_new = []

        for node in current_nodes:
            if verbose:
                logger.info(f"Processing {node.path}")

            children = dbutils.dfs.ls(node.path)
            for child in children:
                if child.size == 0 and child.path != node.path:
                    nodes_new.append(child)
                elif child.path != node.path:
                    files.append(child)

    return files



def restore_delta_tables(spark:SparkSession , env:str , space:str , table_identifiers: list[str]):
    """
    Restore databricks delta table to version n-1
    That is if the current_version is 2 it rools back to 1
    Also it audits the rollback and timestamp when each table is roll backed

    Args:
        :param spark: sparksession
        :param env: dev or test
        :param space: feature , actual
        :param table_identifiers: list of table identifiers [raw_cl.re_property]
    Returns:
        None
    """
    config = Configuration(spark , env=env , space=space)
    catalog = config.get(CATALOG_OPTION)

    audit_table_rollback = f"{catalog}.{space}_silver_md.{WE_AUDIT_TABLE_ROLLBACK}"
    spark.sql(f"create schema if not exists {catalog}.{space}_silver_md")
    spark.sql( f""" create table if not exists {audit_table_rollback}
               (table STRING , rollback_versions INT , previous_version INT , audit_timestamp TIMESTAMP """ )

    for table_identifier in table_identifiers:
        table = f"{catalog}.{space}_{table_identifier}"

        if not spark.catalog.tableExists(table):
            logger.warning(f" Unable to restore table {table} not found in catalog {catalog}" )
            continue

        current_version = DeltaTable.forName( spark , table ).history().sort("version" , ascending=False).asDict().get("version")
        rollback_version = current_version -1

        # restore table
        if rollback_version >= 0:
            spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {rollback_version}")

        #audit rollback info
        data = [(table_identifier , rollback_version , current_version )]
        audit_data_df = spark.createDataFrame(data, 'table STRING , rollback_version INTEGER, previous_version INTEGER ').\
                              withColumn("audit_timestamp" , current_timestamp())

        audit_data_df_view = "audit_data_df_view"
        audit_data_df.createOrReplaceTempView(audit_data_df_view)

        spark.sql(f"""
            Merge into {audit_table_rollback} a using {audit_data_df_view} b
            on (a.table = b.table)
            when matched then 
            update set(
                a.rollback_version = b.rollback_version,
                a.previous_version = b.previous_version,
                a.audit_timestamp = b.audit_timestamp
            when not matched then
            insert
            (
                a.table , a.roll_back_version , a.previous_version , a.audit_timestamp
            )
            values
            (
                b.table , b.roll_back_version , b.previous_version , b.audit_timestamp
            ) 
            
        """)






