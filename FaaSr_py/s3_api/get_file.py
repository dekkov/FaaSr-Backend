import boto3
import logging
import re
import os
import sys


logger = logging.getLogger(__name__)


def faasr_get_file(
    faasr_payload, local_file, remote_file, server_name="", local_folder=".", remote_folder="."
):
    """
    Download file from S3
    """
    if not server_name:
        server_name = faasr_payload["DefaultDataStore"]

    if server_name not in faasr_payload["DataStores"]:
        logger.error(f"Invalid data server name: {server_name}")
        sys.exit(1)

    target_s3 = faasr_payload["DataStores"][server_name]

    # Removes duplicate/trailing slashes from folder and local file names
    remote_folder = re.sub(r"/+", "/", remote_folder.rstrip("/"))
    remote_file = re.sub(r"/+", "/", remote_file.rstrip("/"))
    local_folder = re.sub(r"/+", "/", local_folder.rstrip("/"))
    local_file = re.sub(r"/+", "/", local_file.rstrip("/"))

    if os.path.isabs(local_file):
        get_file = local_file
    else:
        get_file = f"{local_folder}/{local_file}"

    if remote_folder == "":
        get_file_s3 = remote_file
    else:
        get_file_s3 = f"{remote_folder}/{remote_file}"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=target_s3["AccessKey"],
        aws_secret_access_key=target_s3["SecretKey"],
        region_name=target_s3["Region"],
        endpoint_url=target_s3["Endpoint"],
    )

    # If the file already exists, delete it before downloading
    if os.path.exists(get_file):
        os.remove(get_file)

    # Download file from S3
    try:
        result = s3_client.download_file(
            Bucket=target_s3["Bucket"], Key=get_file_s3, Filename=get_file
        )
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.error(f"S3 object not found: s3://{target_s3['Bucket']}/{get_file_s3}")
            sys.exit(1)
        else:
            logger.error(f"Error downloading file from S3: {e}")
            sys.exit(1)
            

