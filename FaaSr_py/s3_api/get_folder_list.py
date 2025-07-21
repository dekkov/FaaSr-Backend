import boto3
import sys


def faasr_get_folder_list(config, server_name="", faasr_prefix=""):
    """
    This function gets a list of objects in the S3 bucket
    """

    # Get server name from payload if one is not providedS
    if server_name == "":
        server_name = config["DefaultDataStore"]

    # Ensure the server is a valid data store
    if server_name not in config["DataStores"]:
        err_msg = (
            '{"faasr_get_folder_list":"Invalid data server name: '
            + server_name
            + '"}\n'
        )
        print(err_msg)
        sys.exit(1)

    # Get the S3 data store to get folder list from
    target_s3 = config["DataStores"][server_name]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=target_s3["AccessKey"],
        aws_secret_access_key=target_s3["SecretKey"],
        region_name=target_s3["Region"],
        endpoint_url=target_s3["Endpoint"],
    )

    # List objects from S3 bucket
    result = s3_client.list_objects_v2(Bucket=target_s3["Bucket"], Prefix=faasr_prefix)
    if "Contents" in result:
        result = [content["Key"] for content in result["Contents"]]
        result = [obj for obj in result if not obj.endswith("/")]
        return result
    return []
