import requests
import sys


def faasr_put_file(local_file, remote_file, server_name="", local_folder=".", remote_folder="."):
    """
    Uploads a file to the FaaSr server
    """
    request_json = {
        "ProcedureID": "faasr_put_file",
        "Arguments": {"local_file": local_file, 
                    "remote_file": remote_file,
                    "server_name": server_name,
                    "local_folder": local_folder,
                    "remote_folder": remote_folder},
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)
    

def faasr_get_file(local_file, remote_file, server_name="", local_folder=".", remote_folder="."):
    """
    Downloads a file from the FaaSr server
    """
    request_json = {
        "ProcedureID": "faasr_get_file",
        "Arguments": {"local_file": local_file, 
                    "remote_file": remote_file,
                    "server_name": server_name,
                    "local_folder": local_folder,
                    "remote_folder": remote_folder}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)


def faasr_delete_file(remote_file, server_name="", remote_folder=""):
    """
    Deletes a file from the FaaSr server
    """
    request_json = {
        "ProcedureID": "faasr_delete_file",
        "Arguments": {"remote_file": remote_file, 
                    "server_name": server_name,
                    "remote_folder": remote_folder}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)


def faasr_log(log_message):
    """
    Logs a message to the FaaSr server log
    """
    if not log_message:
        err_msg = "{{py_client_stub: ERROR -- faasr_log called with empty log_message}}"
        print(err_msg)
        sys.exit(1)
    request_json = {
        "ProcedureID": "faasr_log",
        "Arguments": {"log_message": log_message}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)


def faasr_get_folder_list(server_name="", faasr_prefix = ""):
    """
    Get the list of folders from the FaaSr server
    """
    request_json = {
        "ProcedureID": "faasr_get_folder_list",
        "Arguments": {"server_name": server_name,
                     "faasr_prefix": faasr_prefix}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)
    try:
        response = r.json()
        return response["Data"]["folder_list"]
    except Exception as e:
        err_msg = f"{{py_client_stub: failed to get folder list from server -- {e}}}"
        print(err_msg)
        sys.exit(1)

def faasr_rank():
    """
    Get the rank and max rank of the current function as a namedtuple (rank, max_rank)
    """
    request_json = {
        "ProcedureID": "faasr_rank",
        "Arguments": {}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)
    try:
        response = r.json()
        return response["Data"]["rank"]
    except Exception as e:
        err_msg = f"{{py_client_stub: failed to get rank from server -- {e}}}"
        print(err_msg)
        sys.exit(1)

def faasr_get_s3_creds():
    """
    Get S3 credentials from the server
    

    Returns:        
        dict -- S3 credentials
    """
    request_json = {
        "ProcedureID": "faasr_get_s3_creds",
        "Arguments": {}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)
    try:
        response = r.json()
        return response["Data"]["s3_creds"]
    except Exception as e:
        err_msg = f"{{py_client_stub: failed to get S3 credentials from server -- {e}}}"
        print(err_msg)
        sys.exit(1)

def faasr_return(return_value=None):
    """
    Returns the result of the user function to the FaaSr server
    Arguments:
        return_value: bool -- the return value of the user function
    """
    return_json = {
        "FunctionResult": return_value
    }
    r = requests.post("http://127.0.0.1:8000/faasr-return", json=return_json)
    sys.exit()



def faasr_exit (message=None, error=True):
    exit_json = {
        "Error": error,
        "Message": message
    }
    r = requests.post("http://127.0.0.1:8000/faasr-exit", json=exit_json)
    sys.exit()
