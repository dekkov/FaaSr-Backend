import boto3
import os
import sys

from pathlib import Path
from FaaSr_py.config.debug_config import global_config
from FaaSr_py.helpers.s3_helper_functions import get_logging_server


def faasr_log(config, log_message):
    """
    This function logs a message in the FaaSr log

    Arguments:
        config: FaaSr payload dict
        log_message: str -- message to log
    """

    log_folder = Path(config['FaaSrLog']) / config['InvocationID']
    log_path = Path(Path(log_folder)) / f"{config['FunctionInvoke']}.txt"


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

        log_server = config["DataStores"][log_server_name]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=log_server["AccessKey"],
            aws_secret_access_key=log_server["SecretKey"],
            region_name=log_server["Region"],
            endpoint_url=log_server["Endpoint"],
        )

        check_log_file = s3_client.list_objects_v2(
            Bucket=log_server["Bucket"], Prefix=str(log_path)
        )

        log_download_path = Path("/tmp/", log_path)
        Path(log_download_path).parent.mkdir(parents=True, exist_ok=True)

        # Download the log if it exists
        if "Contents" in check_log_file and len(check_log_file["Contents"]) != 0:
            if os.path.exists(log_download_path):
                os.remove(log_download_path)
            s3_client.download_file(
                Bucket=log_server["Bucket"], Key=str(log_path), Filename=str(log_download_path)
            )

        # Write to log
        logs = f"{log_message}\n"
        with open(log_download_path, "a") as f:
            f.write(logs)

        # Upload log back to S3
        with open(log_download_path, "rb") as log_data:
            s3_client.put_object(Bucket=log_server["Bucket"], Body=log_data, Key=str(log_path))
