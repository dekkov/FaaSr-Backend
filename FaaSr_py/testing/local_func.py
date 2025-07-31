from pathlib import Path

import random2
import uuid6


def default_func(test=32):
    """
    Example function ran when USE_LOCAL_USER_FUNC == True
    """
    num = random2.randint(1, 10)
    print(f"rand 1-10: {num}")
    uuid = uuid6.uuid6()
    print(f"rand uuid: {uuid}")
    print(uuid)
    print(f"param test: {test}")

    filename = Path("testfile.txt")
    folder = Path("test_folder")

    # test rank
    print(faasr_rank())

    local_file = Path("tmp") / filename
    local_file.parent.mkdir(parents=True, exist_ok=True)
    with open(local_file, "w") as f:
        f.write("test file content")
    faasr_put_file(local_file=local_file, remote_file=filename, remote_folder=folder)

    # test folder list
    print(faasr_get_folder_list(prefix=folder))

    # test get file
    faasr_get_file(
        local_file="redownloaded.txt",
        local_folder="/tmp/",
        remote_file=filename,
        remote_folder=folder,
    )
    with open("/tmp/redownloaded.txt", "r") as f:
        print(f"redownloaded content: {f.readline()}")

    # test delete file
    faasr_delete_file(remote_file=filename, remote_folder=folder)
    print(faasr_get_folder_list(prefix=folder))

    # test get s3 creds
    s3_creds = faasr_get_s3_creds()
    print(f"bucket: {s3_creds["bucket"]}")
    print(f"s3_creds: {s3_creds}")
    return True
