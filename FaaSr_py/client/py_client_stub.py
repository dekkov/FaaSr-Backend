import requests
import sys


def faasr_put_file(local_file, remote_file, server_name="", local_folder=".", remote_folder="."):
    print("put file wrapper!")
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
    request_json = {
        "ProcedureID": "faasr_delete_file",
        "Arguments": {"remote_file": remote_file, 
                    "server_name": server_name,
                    "remote_folder": remote_folder}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)


def faasr_log(log_message):
    request_json = {
        "ProcedureID": "faasr_log",
        "Arguments": {"log_message": log_message}
    }
    r = requests.post("http://127.0.0.1:8000/faasr-action", json=request_json)


def faasr_get_folder_list(server_name="", faasr_prefix = ""):
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


def faasr_return(return_value=None):
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
