import uuid
import sys
import boto3
import logging


logger = logging.getLogger(__name__)


def validate_uuid(uuid_value):
    """
    Checks to see if a string is a valid UUID
    """
    if not isinstance(uuid_value, str):
        return False

    try:
        uuid.UUID(uuid_value)
    except ValueError:
        return False
    return True


def get_logging_server(faasr):
    """
    Returns the default logging datastore for the payload as a string

    If LoggingDataStore is None, then returns DefaultDataStore
    """
    if faasr["LoggingDataStore"] is None:
        logging_server = faasr["DefaultDataStore"]
    else:
        logging_server = faasr["LoggingDataStore"]
    return logging_server


def get_default_log_boto3_client(faasr):
    """
    Returns a boto3 client associated with default logging datastore

    Arguments:
        faasr: FaaSr payload dict
    Returns:
        boto3.client: boto3 client for S3 datastore
    """
    # Get the target S3 server
    target_s3 = get_logging_server(faasr)
    s3_log_info = faasr["DataStores"][target_s3]

    if target_s3 not in faasr["DataStores"]:
        err = f'{"get_default_log_client":"Invalid data server name: {target_s3}"}\n'
        print(err)
        sys.exit(1)

    return boto3.client(
        "s3",
        aws_access_key_id=s3_log_info["AccessKey"],
        aws_secret_access_key=s3_log_info["SecretKey"],
        region_name=s3_log_info["Region"],
        endpoint_url=s3_log_info["Endpoint"],
    )
