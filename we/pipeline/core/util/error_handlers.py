"""This module contains cutomer exception handlers"""

class Error(Exception):
    """Base class for other exception"""

class ParseConfigError(Error):
    """Raised when parsing config file is failed"""

class NoMetadataRecordFoundError(Error):
    """Raised whe there is no metadata record found in we_metadat table"""