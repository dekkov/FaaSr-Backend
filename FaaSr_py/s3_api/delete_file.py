import re
import boto3
import sys


def faasr_delete_file(config, remote_file, server_name="", remote_folder=""):
    """
    This function deletes a file from S3
    """
    # Get server name from payload if one isn't provided
    if server_name == "":
        server_name = config["DefaultDataStore"]

    # Ensure that the server is a valid data store
    if server_name not in config["DataStores"]:
        err_msg = (
            '{"faasr_delete_file":"Invalid data server name: ' + server_name + '"}\n'
        )
        print(err_msg)
        sys.exit(1)

    # Get the S3 data store to delete file from
    target_s3 = config["DataStores"][server_name]

    # Remove "/" in the folder & file name to avoid situations:x
    # 1: duplicated "/" ("/remote/folder/", "/file_name")
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r"/+", "/", remote_folder.rstrip("/"))
    remote_file = re.sub(r"/+", "/", remote_file.rstrip("/"))

    # Name of file to delete from S3
    delete_file_s3 = f"{remote_folder}/{remote_file}"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=target_s3["AccessKey"],
        aws_secret_access_key=target_s3["SecretKey"],
        region_name=target_s3["Region"],
        endpoint_url=target_s3["Endpoint"],
    )

    # Delete file from S3
    result = s3_client.delete_object(Bucket=target_s3["Bucket"], Key=delete_file_s3)

    # to-do: error if fail
