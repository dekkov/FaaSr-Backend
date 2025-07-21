import sys
from collections import namedtuple


def faasr_get_s3_creds(config, server_name="", faasr_prefix=""):
    """
    Returns credentials needed to create an Apache Pyarrow S3FileSystem instance
    """
    # fetch server name if one is not provided
    if server_name == "":
        server_name = config["DefaultDataStore"]

    # ensure that server name provided is valid
    if server_name not in config["DataStores"]:
        err_msg = f'{{"faasr_get_arrow":"Invalid data server name: {server_name}"}}\n'
        print(err_msg)
        sys.exit(1)

    target_s3 = config["DataStores"][server_name]

    if not target_s3["Anonymous"] or len(target_s3["Anonymous"]) == 0:
        anonymous = False
    else:
        match (target_s3["Anonymous"].tolower()):
            case "true":
                anonymous = True
            case "false":
                anonymous = False
            case _:
                anonymous = False

    # if the connection is anonymous, don't return keys
    if anonymous:
        secret_key = None
        access_key = None
    else:
        secret_key = target_s3["SecretKey"]
        access_key = target_s3["AccessKey"]

    s3_creds = namedtuple(
        "bucket", "region", "endpoint", "secret_key", "access_key", "anonymous"
    )

    # return credentials as namedtuple
    return s3_creds(
        target_s3["Bucket"],
        target_s3["Region"],
        target_s3["Endpoint"],
        secret_key,
        access_key,
        anonymous,
    )
