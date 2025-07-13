import logging
import boto3

from we.pipeline.core.constant import S3_BASE , DBFS_BASE
from we.pipeline.core.task.pre_raw.vendor_ftp_file_transfer_task import Bucket_Details
from we.pipeline.core.util import get_dbutils

logger = logging.getLogger(__name__)


def path_gen(env, space ,pipeline , object_type=None , base='dbfs'):
    """
    This function takes env, space ,pipeline , object_type as input and returns s3 path
    :param env: dev, test , prod
    :param space: feature, synthetic , actual , release
    :param pipeline: pipeline name
    :param object_type: name of sub-object type within pipeline
    :param base: location where checkpointing has to be done
    :return:
        path:String
        Conactenated s3 path to be used for dataframe heckpointing
        Eg: S3://we-alt-tgat-dev-v1/we/dev/feature/checkpoint/real_estate/loan
            S3://we-alt-tgat-dev-v1/we/dev/feature/checkpoint/real_estate/property
            if object type is not passed pipeline llevel checkpoint path is
            S3://we-alt-tgat-dev-v1/we/dev/feature/checkpoint/real_estate
    """
    if S3_BASE.startswith(base.lower()):
            path = (S3_BASE + Bucket_Details[env]+ '/we/' + env + '/' + space + '/' + 'checkpoint/' + pipeline + '/' + object_type)

    elif DBFS_BASE.startswith((base.lower())):
        if object_type:
            path = DBFS_BASE + 'FileStore/checkpoint/we' + env + '/' + space + pipeline + '/' + object_type + '/'
        else:
            path = DBFS_BASE + 'FileStore/checkpoint/we' + env + '/' + space + pipeline + '/'
    else:
        logger.error("Wrong  base location provided for checkpointing")

    logger.info("Checkpoint path generated for {0} and {1} : {2}".format(pipeline , object_type , path))

    return path






def dataframe_checkpoint(spark , df, env, space ,pipeline , object_type , base):
    """
        This function takes df, env, space ,pipeline , object_type , base as input and checkpoints df for later usage
    :param spark:
    :param df:
    :param env:
    :param space:
    :param pipeline:
    :param object_type:
    :param base:
    :return:
      df: datframe
    """
    checkpoint_location = path_gen(env,space,pipeline,object_type,base)
    spark.sparkContext.setCheckpointDir(checkpoint_location)

    return df.checkpoint()




def delete_checkpoint(spark , df, env, space ,pipeline , object_type=None , base='dbfs'):
    """
    Args
    :param spark:
    :param df:
    :param env:
    :param space:
    :param pipeline:
    :param object_type:
    :param base:
    :return:
        None
    """
    checkpoint_location = path_gen(env,space ,pipeline , object_type , base)
    logger.info("Data to be deleted from location : " ,checkpoint_location)

    if S3_BASE.startswith(base.lower()):

        s3 = boto3.resource('s3')

        bucket = s3.Bucket(Bucket_Details[env])
        prefix = "/".join(checkpoint_location.split("/")[3:])

        objects_to_delete = bucket.objects.filter(Prefix=prefix)

        for obj in objects_to_delete:
            obj.delete()
            logger.info("object : {0} has been deleted as part of checkpoint clean up".format(obj))

    elif DBFS_BASE.startswith(base.lower()):
        dbutils = get_dbutils(spark)
        dbutils.fs.rm(checkpoint_location , recursive=True)

    else:
        logger.error("Wrong base location provided for deleting checkpoint . It should be s3 or dbfs")
        raise ValueError("Wrong base location provided for deleting checkpoint . It should be s3 or dbfs")




