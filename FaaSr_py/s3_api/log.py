import boto3
import os
import sys
import logging

from pathlib import Path
from FaaSr_py.helpers.s3_helper_functions import get_logging_server, get_default_log_boto3_client


def faasr_log(config, log_message, log_name=None, file_name=None):
    """
    Logs a message

    Arguments:
        config: FaaSr payload dict
        log_message: str -- message to log
    """
    if not log_message:
        err_msg = "{{faasr_log: ERROR -- log_message is empty}}"
        print(err_msg)
        sys.exit(1)

    if not file_name:
        file_name = config["FunctionInvoke"] + ".txt"

    # Lazily import global_config to avoid circular imports
    from FaaSr_py.config.debug_config import global_config

    if not log_name:
        log_name = config['FaaSrLog']
    
    log_folder = Path(log_name) / config['InvocationID']
    log_path = Path(Path(log_folder)) / file_name

    if global_config.USE_LOCAL_FILE_SYSTEM:
        # make log dir
        local_log_path = Path(global_config.LOCAL_FILE_SYSTEM_DIR / log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # write log
        logs = f"{log_message}\n"
        print(f"writing {log_message} to {local_log_path}")
        with open(local_log_path, "a") as f:
            f.write(logs)
    else:
        # Get the logging data store from payload
        log_server_name = get_logging_server(config)

        if log_server_name not in config["DataStores"]:
            err_msg = (
            f'{{"faasr_log":"Invalid logging server name: {log_server_name}"}}\n'
            )
            print(err_msg)
            sys.exit(1)


        s3_client = get_default_log_boto3_client(config)

        log_download_path = Path("/tmp/", log_path)
        Path(log_download_path).parent.mkdir(parents=True, exist_ok=True)

        bucket = config["DataStores"][log_server_name]["Bucket"]

        # Check if the log file already exists
        check_log_file = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=str(log_path)
        )

        # Download the log if it exists
        if "Contents" in check_log_file and len(check_log_file["Contents"]) != 0:
            if os.path.exists(log_download_path):
                os.remove(log_download_path)
            s3_client.download_file(
                Bucket=bucket, Key=str(log_path), Filename=str(log_download_path)
            )

        # Write to log
        logs = f"{log_message}\n"
        with open(log_download_path, "a") as f:
            f.write(logs)

        # Upload log back to S3
        with open(log_download_path, "rb") as log_data:
            s3_client.put_object(Bucket=bucket, Body=log_data, Key=str(log_path))
