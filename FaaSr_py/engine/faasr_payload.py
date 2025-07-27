import os
import sys
import uuid
import boto3
import logging
import random
import json

from FaaSr_py.helpers.faasr_start_invoke_helper import faasr_get_github_raw
from FaaSr_py.config.debug_config import global_config
from FaaSr_py.helpers.faasr_lock import faasr_acquire, faasr_release
from FaaSr_py.helpers.graph_functions import check_dag, validate_json
from FaaSr_py.helpers.s3_helper_functions import validate_uuid, get_default_log_boto3_client, get_logging_server


logger = logging.getLogger(__name__)


class FaaSrPayload():
    """
    This class stores the workflow as a dictionary and provides methods to access and manipulate it.
    It is initialized with a URL and an optional dictionary of overwritten values.
    The URL points to a GitHub raw file containing the workflow JSON.
    The class also provides methods to validate the workflow, replace secrets, and check S3 data stores.
    It can also initialize a log folder and handle multiple invocations of functions.
    """
    def __init__(self, url: str, overwritten={}, token=None):
        # without PAT, larger workflows run the risk
        # of hitting rate limits hen fetching payload
        if token is None:
            token = os.getenv("TOKEN")

        # fetch payload from gh
        raw_payload = faasr_get_github_raw(token=token, path=url)
        self._base_workflow = json.loads(raw_payload)
        self._overwritten = overwritten

        # validate payload against schema
        if global_config.SKIP_SCHEMA_VALIDATE:
            logger.info("SKIPPING SCHEMA VALIDATION")
        elif validate_json(self._base_workflow):
            pass
        else:
            raise ValueError("Payload validation error")

        self.url = url
        self.log_file = self["FunctionInvoke"] + ".txt"

        logger.debug(f"Payload initialized with URL: {self.url}")

    def __getitem__(self, key: str):
        if key in self._overwritten:
            return self._overwritten[key]
        elif key in self._base_workflow:
            return self._base_workflow[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value):
        self._overwritten[key] = value

    def __contains__(self, item):
        return item in self._base_workflow or item in self._overwritten

    def __it__(self):
        return iter(self.get_complete_workflow().items())
    
    @property
    def overwritten(self):
        return self._overwritten
    
    @property 
    def base_workflow(self):
        return self._base_workflow

    def get_complete_workflow(self):
        temp_dict = self._base_workflow.copy()
        for key, val in self._overwritten.items():
            temp_dict[key] = val
        return temp_dict

    def faasr_replace_values(self, secrets):
        """
        Replaces filler secrets in a payload with real credentials (writes result to overwritten data)

        Arguments:
            secrets: dict -- dictionary of secrets to replace in the payload
        """
        def recursive_replace(payload):
            for name in payload:
                if name not in ignore_keys:
                    # If the value is a list or dict, recurse
                    if isinstance(payload[name], list) or isinstance(payload[name], dict):
                        recursive_replace(payload[name])
                    # Replace value in overwritten
                    elif payload[name] in secrets:
                        payload[name] = secrets[payload[name]]

        ignore_keys = [
            "FunctionGitRepo",
            "FunctionList",
            "FunctionCRANPackage",
            "FunctionGitHubPackage",
            "PyPIPackageDownloads",
            "PackageImports",
        ]

        recursive_replace(self._base_workflow)

    def s3_check(self):
        """
        Ensures that all of the S3 data stores are valid and reachable
        """
        # Iterate through all of the data stores
        for server in self["DataStores"].keys():
            # Get the endpoint and region
            server_endpoint = self["DataStores"][server]["Endpoint"]
            server_region = self["DataStores"][server]["Region"]
            # Ensure that endpoint is a valid http address
            if not server_endpoint.startswith("http"):
                error_message = (
                    f"Invalid data store server endpoint {server}"
                )
                logger.error(error_message)
                sys.exit(1)

            # If the region is empty, then use defualt 'us-east-1'
            if not server_region:
                self["DataStores"][server]["Region"] = "us-east-1"
            if (
                "Anonynmous" in self["DataStores"][server]
                and len(self["DataStores"][server]["Anonymous"]) != 0
            ):
                # to-do: continue if anonymous is true
                print("anonymous param not implemented")

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=self["DataStores"][server]["AccessKey"],
                aws_secret_access_key=self["DataStores"][server]["SecretKey"],
                region_name=self["DataStores"][server]["Region"],
                endpoint_url=self["DataStores"][server]["Endpoint"],
            )
            # Use boto3 head bucket to ensure that the bucket exists and that we have acces to it
            try:
                s3_client.head_bucket(Bucket=self["DataStores"][server]["Bucket"])
            except Exception as e:
                err_message = (
                    f"S3 server {server} failed with message: {e}"
                )
                logger.exception(err_message, stack_info=True)
                sys.exit(1)

    def init_log_folder(self):
        """
        Initializes a faasr log folder if one has not already been created
        """
        # Create invocation ID if one is not already present
        if not self["InvocationID"]:
            if not validate_uuid(self["InvocationID"]):
                ID = uuid.uuid4()
                self["InvocationID"] = str(ID)

        faasr_msg = f"InvocationID for the workflow: {self["InvocationID"]}"
        logger.info(faasr_msg)

        target_s3 = get_logging_server(self)
        s3_log_info = self["DataStores"][target_s3]
        s3_client = get_default_log_boto3_client(self)

        if self["FaaSrLog"] is None or self["FaaSrLog"] == "":
            self["FaaSrLog"] = "FaaSrLog"

        # Get path to log
        log_folder = f"{self["FaaSrLog"]}/{self["InvocationID"]}/"

        # Check contents of log folder
        check_log_folder = s3_client.list_objects_v2(
            Prefix=log_folder, Bucket=s3_log_info["Bucket"]
        )

        # If there already is a log, log error and abort; otherwise, create log
        if "Content" in check_log_folder and len(check_log_folder["Content"]) != 0:
            err_msg = f"InvocationID already exists: {self["InvocationID"]}"
            logger.error(err_msg)
            sys.exit(1)
        else:
            s3_client.put_object(Bucket=s3_log_info["Bucket"], Key=log_folder)

    def abort_on_multiple_invocations(self, pre: dict):
        """
        Invoked when the current function has multiple predecessors
        and aborts if they have not finished or the current function instance was not
        the first to write to the candidate set

        TO-DO: SPLIT -- THIS FUNCTION IS WAY TOO LONG
        """
        target_s3 = get_logging_server(self)
        s3_log_info = self["DataStores"][target_s3]

        # Get boto3 client for default data store (to-do: make general)
        s3_client = get_default_log_boto3_client(self)

        # ID folder is of the form {faasr log}/{InvocationID}
        id_folder = f"{self['FaaSrLog']}/{self['InvocationID']}"

        # If a predecessor has a rank attribute, then we need to ensure
        # That all concurrent invocations of that function have finished
        full_predecessor_list = (
            [func for func in pre if "Rank" not in self["FunctionList"][func]]
        )
        for pre_func in pre:
            if (
                "Rank" in self["FunctionList"][pre_func]
                and self["FunctionList"][pre_func]["Rank"]
            ):
                parts = self["FunctionList"][pre_func]["Rank"].split("/")
                # Rank field should have the form number/number
                if len(parts) != 2:
                    err_msg = f"Error with rank field in function: {pre_func}"
                    logger.error(err_msg)
                    sys.exit(1)
                for rank in range(1, int(parts[1]) + 1):
                    full_predecessor_list.append(f"{pre_func}.{rank}")

        # First, we check if all of the other predecessor actions are done
        # To do this, we check a file called func.done in S3, and see if all of the other actions have
        # written that they are "done"
        # If all predecessor's are not finished, then this action aborts
        s3_list_object_response = s3_client.list_objects_v2(
            Bucket=s3_log_info["Bucket"], Prefix=id_folder
        )
        s3_contents = s3_list_object_response["Contents"]

        s3_object_keys = []
        for object in s3_contents:
            if "Key" in object:
                s3_object_keys.append(object["Key"])

        for func in full_predecessor_list:
            # check if all of the predecessor func.done objects exist
            done_file = f"{id_folder}/{func}.done"

            # if .done does not exist for a function,
            # then the current function is still waiting for
            # a predecessor and must abort
            if done_file not in s3_object_keys:
                res_msg = 'function was not the last invoked - no flag'
                logger.error(res_msg)
                sys.exit(1)

        # This code is reached only if all predecessors are done. 
        # Now, we need to select only one action to proceed
        # We use a weak spinlocklock implementation over S3 to implement atomic        
        # read/modify/write operations and avoid a race condition

        # Between lock acquire and release, we do the following:
        # 1) download the "FunctionInvoke.candidate" file from S3. 
        # 2) append a random number to the local file, which is generated by this Action
        # 3) upload the file back to the S3 bucket
        # 4) download the file from S3
        # 5) if the current action was the first to write to candidate set, it "wins"
        #    and other actions abort

        faasr_acquire(self)

        random_number = random.randint(1, 2**31 - 1)

        if not os.path.isdir(f"/tmp/{id_folder}"):
            os.makedirs(f"/tmp/{id_folder}", exist_ok=True)

        candidate_path = f"{id_folder}/{self['FunctionInvoke']}.candidate"
        candidate_temp_path = f"/tmp/{candidate_path}"

        # Get all of the objects in S3 with the prefix {id_folder}/{FunctionInvoke}.candidate
        s3_response = s3_client.list_objects_v2(
            Bucket=s3_log_info["Bucket"], Prefix=candidate_path
        )
        if "Contents" in s3_response and len(s3_response["Contents"]) != 0:
            # Download candidate set
            if os.path.exists(candidate_temp_path):
                os.remove(candidate_temp_path)
            s3_client.download_file(
                Bucket=s3_log_info["Bucket"],
                Key=candidate_path,
                Filename=candidate_temp_path,
            )

        # Write unique random number to candidate file
        with open(candidate_temp_path, "a") as cf:
            cf.write(str(random_number) + "\n")

        with open(candidate_temp_path, "rb") as cf:
            # Upload candidate file back to S3
            s3_client.put_object(
                Body=cf, Key=candidate_path, Bucket=s3_log_info["Bucket"]
            )

        # Download candidate file to local directory again
        if os.path.exists(candidate_temp_path):
            os.remove(candidate_temp_path)
        s3_client.download_file(
            Bucket=s3_log_info["Bucket"],
            Key=candidate_path,
            Filename=candidate_temp_path,
        )

        # Release the lock
        faasr_release(self)

        # Abort if current function was not the first to write to the candidate set
        with open(candidate_temp_path, "r") as updated_candidate_file:
            first_line = updated_candidate_file.readline().strip()
            first_line = int(first_line)
        if random_number != first_line:
            res_msg = "not the last trigger invoked - random number in canidate does not match"
            logger.error(res_msg)
            sys.exit(1)

    def start(self):
        # Verifies that the faasr payload is a DAG, meaning that there is no cycles
        # If the payload is a DAG, then this function returns a predecessor list for the workflow
        # If the payload is not a DAG, then the action aborts
        pre = check_dag(self)

        # Verfies the validity of S3 data stores, checking the server status and ensuring that the specified bucket exists
        # If any of the S3 endpoints are invalid or any data store server are unreachable, the action aborts
        self.s3_check()

        # Initialize log if this is the first action in the workflow
        if len(pre) == 0:
            self.init_log_folder()

        # If there are more than 1 predecessor, then only the final action invoked will sucessfully run
        # This function validates that the current action is the last invocation; otherwise, it aborts
        if len(pre) > 1:
            self.abort_on_multiple_invocations(pre)

        # Start S3 logger -- logs with level of DEBUG or higher (all logs) will be sent to
        # S3. All logs above INFO will be sent to STDOUT & S3
        global_config.add_s3_log_handler(self)

