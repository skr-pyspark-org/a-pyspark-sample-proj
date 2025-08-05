LOG_FORMAT = '%(asctime)s %(levelname)-5s %(module)s - %(message)s'

#Environment , space , object_type and refresh_type details
environments = ['dev','test','prod']
spaces = ['feature','synthetic','release']
layers= ['raw','bronze','silver','gold']
object_types=['aa','ci','oa','pos','re','ri','don','it','am']
address_sources= ['ns','acx','inf','cl','hvr','dnb','can']
refresh_type = ['full','test']
DEBUG_ENVS= ['dev' , 'test']
OBJECT_TYPE_ORG = 'org'
OBJECT_TYPE_ORG_PPL = 'org_ppl'

dd_tables = ['additional_asset' , 'contact_info' , 'position' , 'other_attribute' ,
             'investment_transaction', 'donation' , 'real_estate' , 'residential_info']

#ETL job script details
__VENDOR_SH_PACKAGE = 'we.pipeline.core.sh_scripts'
__VENDOR_SH_FILE = 'run_etl.sh'
__VENDOR_MULTI_FILEP_SH_FILE = 'run_etl_multi_file.sh'
__MULTI_FILE_PIPELINE_LIST = ['fed_donation' , 'eha' , 'dmi' , 'gca']


# Truncate amd load Bronze tables
BRONZE_TRUNCATE_LOAD_TABLES = ['ri_bronze_raw' ,
                               'pos_eha_bronze_raw',
                               'pos_indepth_officers_bronze_raw',
                               'pos_dmi_bronze_raw']

# refresh type
DELTA = 'delta'
MANUAL = 'manual'
FULL = 'full'

# Batch
LOAD = "load"


# Task dict attributes
NAME = 'task_name'
DESC = 'description'


# Hashkey column
HASH_KEY = 'hash_key'


# COMMON_PARTITION_COLUMN
BATCH_ID = 'batch_id'


# Common CLI options used by tasks in this module
TASK_NAME_OPTION = "task_name"
TASK_DESC_OPTION = "task_description"
MODE_OPTION = "mode"
CONFIG_OPTION = "config"
ENV_OPTION = "env"
SPACE_OPTION = "space"
BUCKET_OPTION = "bucket"
CATALOG_OPTION = "catalog"
DATA_GROUP_OPTION = "data_group"
PARTITION_FILE_NUMBER = "partition_number"
OBJECT_TYPE_OPTION = "object_type"
CURRENT_OBJECT_TYPE_OPTION = "current_object_type"
PREVIOUS_OBJECT_TYPE_OPTION = "previuos_object_type"
OBJECT_TYPE_VENDOR_CODE_OPTION = "object_type_vendor_code"
EXCLUDE_OPTION = "exclude"
LAYER_OPTION = "layer"
ADDRESS_SOURCE_OPTION = "address_source"
SOURCE_OPTION = "source"
DESTINATION_OPTION = "destination"
REFRESH_TYPE_OPTION = "refresh_type"
SCHEMA_PATH_OPTION = "schema_path"
SCHEMA_FILE_OPTION = "schema_file"
SOURCE_FILE_FORMAT_OPTION = "source_file_format"
SOURCE_FILE_HEADER_OPTION = "source_file_header"
SOURCE_FILE_DELIMITER_OPTION = "source_file_delimiter"
ROWS_TO_SKIP_OPTION = "rows_to_skip"
OUTPUT_MODE_OPTION = "output_mode"
SOURCE_TABLE_OPTION= "source_table"
TARGET_TABLE_OPTION= "target_table"
BATCH_OPTION = "batch"
PARTITION_OPTION = "partition"
BATCH_SIZE_OPTION = "batch_size"
INPUT_FILE_OPTION = "input_file"
INPUT_FILE_FORMAT_OPTION = "input_file_format"
INPUT_SCHEMA_OPTION = "input_schema"
JOB_TYPE_OPTION = "job_type"
PIPELINE_NAME_OPTION = "pipeline_name"
PREVIOUS_PIPELINE_NAME_OPTION = "previous_pipeline_name"
VENDOR_NAME_OPTION = "vendor_name"
PREVIOUS_TASK_NAME_OPTION= "previous_task"
PREVIOUS_TASK_DESC_OPTION= "previous_task_description"
PREVIOUS_LAYER_OPTION= "previous_layer"
PIPELINE_OPTION = "pipeline"
VENDOR_OPTION = "vendor"
COMPRESSION_FORMAT_OPTION = "compression_format"
SCHEMA_NAME_OPTION = "schema_name"
OBJECT_TYPE_PIPELINE_OPTION = "object_type_pipeline"
VARIABLE_OBJECT_OPTION = "variable_obj"
CURRRENT_TASK = "current_task"
PREVIOUS_TASK = "previous_task"
FLOW_TYPE_OPTION = "flow_type"
ROW_TAG = "row_tag"
GENERATE_HASH_KEY_FLAG = "generate_hash_key_flag"
ES_ENV = "es_env"



#Mode
QUERY_MODE = "query"
IMPORT_MODE = "import"


# Entity Resolution Task Name
ER_TAK_NAME = "RunEntityResolutionTask"

# Build Target Features Task Name
BTF_TASK_NAME = "BuildTargetFeaturesTask"


# Pipeline name
PIPELINE_REAL_ESTATE = "real_estate"
PIPELINE_RESIDENTIAL_INFO = "residential_info"
PIPELINE_CONTACT_INFO = "contact_info"
PIPELINE_DONATION = "donation"
PIPELINE_OTHER_ATTRIBUTE = "other_attribute"
PIPELINE_DDBP = "ddbp"
PIPELINE_URPP = "urpp"
PIPELINE_NEW_PROFILES = "new_profile_creation"
PIPELINE_MISC = "misc"
PIPELINE_DATA_CURATION = "data_curation"
PIPELINE_POSITION = "position"
PIPELINE_PERSON_IDENTITY = "pip"


# Vendor Name
VENDOR_CORELOGIC = "corelogic"
VENDOR_NEUSTAR = "neustar"
VENDOR_ACXIOM = "acxiom"
VENDOR_HVR = "hvr"
VENDOR_DNB = "dnb"
VENDOR_INTERNAL = "internal"


# DATA GROUP
DATA_GROUP_DD = "dd"
DATA_GROUP_MD = "md"
DATA_GROUP_MASTER = "master"
DATA_GROUP_CL = "cl"
DATA_GROUP_UNRESOLVED = "unresolved"
DATA_GROUP_NEUSTAR = "neusstar"
DATA_GROUP_CH_DON = "internal"
DATA_GROUP_RI = "acx"
DATA_GROUP_NPC = "npc"
DATA_GROUP_BACKUP = "backup"
DATA_GROUP_HVR = "hvr"
DATA_GROUP_DNB = "dnb"
DATA_GROUP_DATA_CURATION = "data_curation"


# OBJECT_TYPE
OBJECT_TYPE_PROPERTY = "property"
OBJECT_TYPE_LOAN = "loan"
OBJECT_TYPE_PROPERTY_LOAN = "property_loan"
OBJECT_TYPE_REAL_ESTATE = "real_estate"
OBJECT_TYPE_CONTACT_INFO = "contact_info"
OBJECT_TYPE_CHARITABLE_DONATION = "charitable_donation"
OBJECT_TYPE_FED_DONATION = "fed_donation"
OBJECT_TYPE_PAM = "person_address_master"
OBJECT_TYPE_ER_FEATURE = "target_er_feature"
OBJECT_TYPE_INTERNAL = "internal"
OBJECT_TYPE_RESIDENTIAL_INFO = "residential_info"
OBJECT_TYPE_RESIDENTIAL_INFO_TINY = "ri"
OBJECT_TYPE_DUNSID = "dunsid"
OBJECT_TYPE_EHA = "eha"
OBJECT_TYPE_DMI = "dmi"
OBJECT_TYPE_GCA = "gca"
OBJECT_TYPE_INDEPTH = "indepth"
OBJECT_TYPE_DMI_INDEPTH = "dmi_indepth"
OBJECT_TYPE_POSITION = "position"


# DEFAULT BATCHID
DEFAULT_BATCH_ID: int = 20240101


# DEFAULT BATCHID FOR SYNTHETIC SPACE
DEFAULT_BATCH_ID_FOR_SYNTHETIC_SPACE: int = 19000101

# CONFIG PATHS
CONFIG_CORE_PATH = "we.pipeline.core.config"
CONFIG_UNRESOLVED_PATH = "we.pipeline.unresolved_data_process.config"
CONFIG_DATA_CURATION_PATH = "we.pipeline.data_curation.config"


PRE_ER_CONST = "_pre_er"
ZONE_OPTION = "zone"
DIR_OPTION = "dir"


#SCHEMA PATHS
SCHEMA_REAL_ESTATE = "we.pipeline.real_estate.schema"
SCHEMA_CONTACT_INFO = "we.pipeline.contact_info.schema"
SCHEMA_PERSON_IDENTITY = "we.pipeline.person_identity.schema"
SCHEMA_CHARITABLE_DONATION = "we.pipeline.donation.charitable_donation.schema"
SCHEMA_RESIDENTIAL_INFO = "we.pipeline.residential_info.schema"
SCHEMA_NEW_PROFILES = "we.pipeline.new_profile_creation.schema"
SCHEMA_FED_DONATION = "we.pipeline.donation.fed_donation.schema"
SCHEMA_DATA_CURATION = "we.pipeline.data_curation.schema"
SCHEMA_HVR = "we.pipeline.position.schema.hvr"
SCHEMA_DNB = "we.pipeline.position.schema.dnb"
SCHEMA_POSITION = "we.pipeline.position.schema"


#FILE FORMAT
CSV = "csv"
PARQUET = "parquet"
XML = "xml"


# DELIMITER
PIPE = "PIPE"
COMMA = "COMMA"
ACK = "ACK"
TAB = "TAB"
CARET = "CARET"
COLON = "COLON"


# WRITE MODE
OVERWRITE = "overwrite"
APPEND = "append"

# METADATA TABLE
WE_METADATA = "we_metadata"

# AUDIR TABLE ROLL BACK
WE_AUDIT_TABLE_ROLLBACK = "we_audit_table_roll_back"

# DELTA FILTER COLUMN
DELTA_FILTER_COLUMN = "batch_id"

# CHECKPOINT BASE LOCATION
S3_BASE = "s3://"
DBFS_BASE = "dbfS://"
CHECKPOINT_ROOT = "dbfs:/FileStore/Checkpoint"


# LAYER NAME
UNPROCESSED = "unprocessed"
PROCESSED = "processed"
RAW_ZONE = "raw"
BRONZE_ZONE = "bronze"
SILVER_ZONE = "silver"
GOLD_ZONE = "gold"


# common pipeline tasks
DOWNLOAD_TASK:dict = {NAME : "download" ,
                      DESC: "download raw files from FTP to unprocessed layer"}

UNZIP_AND_SPLIT_TASK:dict = {NAME : "unzip_and_spit" ,
                      DESC: "unzip , split and copy files into source folder from unprocessed layer"}


AUTOLOADER_TASK :dict = {NAME: "autoloader" ,
                         DESC: "Load vendor files into raw zone"
                        }

DELTA_DATA_EXPORT_TASK :dict = {NAME: "delta_data_export" ,
                         DESC: "Export delta data for given batch_id"
                        }


DDBP_CONNECTION_CALC_INPUT_TASK :dict = {NAME: "build_connection_calc_input" ,
                         DESC: "build input for connection calc input"
                        }

DDBP_POST_CONNECTION_CALC_INPUT_TASK :dict = {NAME: "build_post_connection_calc_input" ,
                         DESC: "build input for post connection calc input"
                        }

DDBP_BUILD_DDP_INPUT_TASK :dict = {NAME: "build_ddp_input" ,
                         DESC: "build input for ddp"
                        }

PROCESS_UNRESOLVED_TASK: dict = {
    NAME: "unresolved_data_process",
    DESC: "unresolved_data_process"
}


SPACE_SYNTHETIC = "synthetic"

ENV_DEV = "dev"
























