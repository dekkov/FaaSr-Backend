import boto3
import re
import os
import sys


def faasr_get_file(
    config, local_file, remote_file, server_name="", local_folder=".", remote_folder="."
):
    """
    Download file from S3
    """
    if not server_name:
        server_name = config["DefaultDataStore"]

    if server_name not in config["DataStores"]:
        err_msg = '{"faasr_get_file":"Invalid data server name: ' + server_name + '"}\n'
        print(err_msg)
        sys.exit(1)

    target_s3 = config["DataStores"][server_name]
    local_folder = f"/tmp/{local_folder}"

    # Removes duplicate/trailing slashes from folder and local file names
    remote_folder = re.sub(r"/+", "/", remote_folder.rstrip("/"))
    remote_file = re.sub(r"/+", "/", remote_file.rstrip("/"))
    local_folder = re.sub(r"/+", "/", local_folder.rstrip("/"))
    local_file = re.sub(r"/+", "/", local_file.rstrip("/"))

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

    print(get_file_s3, get_file)

    # Download file from S3
    try:
        result = s3_client.download_file(
            Bucket=target_s3["Bucket"], Key=get_file_s3, Filename=get_file
        )
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            err_msg = (
                f'{{faasr_get_file: S3 object not found: s3://{target_s3['Bucket']}/{get_file_s3}}}'
                )
            print(err_msg)
            sys.exit(1)
        else:
            err_msg = f'{{faasr_get_file: ERROR -- {e}}}'
            print(err_msg)
            sys.exit(1)
            

