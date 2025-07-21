import uvicorn
import sys
import requests
from pydantic import BaseModel
from fastapi import FastAPI

from FaaSr_py.s3_api import (
    faasr_log,
    faasr_put_file,
    faasr_get_file,
    faasr_delete_file,
    faasr_get_folder_list,
    faasr_get_s3_creds,
)


valid_functions = {
    "faasr_get_file",
    "faasr_put_file",
    "faasr_delete_file",
    "faasr_get_folder_list",
    "faasr_log",
}
faasr_api = FastAPI()


class Request(BaseModel):
    ProcedureID: str
    Arguments: dict | None = None


class Response(BaseModel):
    Success: bool
    Data: dict
    Message: str | None = None


class Return(BaseModel):
    FunctionResult: bool | None = None


class Result(BaseModel):
    FunctionResult: bool | None = None
    Error: bool | None = None
    Message: str | None = None


class Exit(BaseModel):
    Error: bool | None = None
    Message: str | None = None


# registers request and return handlers with a faasr_instance
def register_request_handler(faasr_instance):
    return_val = None
    message = None
    error = False

    @faasr_api.post("/faasr-action")
    def faasr_request_handler(request: Request):
        nonlocal error
        print(f'{{"Processing request": "{request.ProcedureID}"}}')

        if request.ProcedureID not in valid_functions:
            print({{"faasr_server.py: ERROR -- invalid FaaSr function call"}})
            error = True
            sys.exit(1)
        args = request.Arguments or {}
        return_obj = Response(Success=True, Data={})
        try:
            match request.ProcedureID:
                case "faasr_log":
                    faasr_log(config=faasr_instance, **args)
                case "faasr_put_file":
                    faasr_put_file(config=faasr_instance, **args)
                case "faasr_get_file":
                    faasr_get_file(config=faasr_instance, **args)
                case "faasr_delete_file":
                    faasr_delete_file(config=faasr_instance, **args)
                case "faasr_get_folder_list":
                    return_obj.Data["folder_list"] = faasr_get_folder_list(
                        config=faasr_instance, **args
                    )
                case "faasr_rank":
                    print("to-do: faasr_rank")
                case "faasr_get_s3_creds":
                    print("to-do: faasr_get_s3_creds")
        except Exception as e:
            err_msg = f"{{faasr_server: ERROR -- failed to invoke {request.ProcedureID} -- {e}}}"
            faasr_log(config=faasr_instance, log_message=err_msg)
            print(err_msg)
            error = True
            sys.exit(1)
        return return_obj

    @faasr_api.post("/faasr-return")
    def faasr_return_handler(return_obj: Return):
        nonlocal return_val
        print("Processing return")
        return_val = return_obj.FunctionResult
        print(f"Return val: {return_val}")

    @faasr_api.post("/faasr-exit")
    def faasr_get_exit_handler(exit_obj: Exit):
        print("Exiting user function")
        nonlocal error, message
        print(exit_obj)
        if exit_obj.Error:
            error = True
            message = exit_obj.Message

    @faasr_api.get("/faasr-get-return")
    def faasr_get_return_handler():
        print(f"Return val: {return_val} error: {error} -- get")
        return Result(FunctionResult=return_val, Error=error, Message=message)


@faasr_api.get("/faasr-echo")
def faasr_echo(message: str):
    return {"message": message}


# poll server until it's ready
def wait_for_server_start(port):
    while True:
        try:
            r = requests.get(
                f"http://127.0.0.1:{port}/faasr-echo", params={"message": "echo"}
            )
            message = r.json()["message"]
            if message == "echo":
                break
        except Exception:
            continue


# starts a server listening on localhost port 8000
def run_server(faasr_instance, port):
    register_request_handler(faasr_instance)
    config = uvicorn.Config(faasr_api, host="127.0.0.1", port=port)
    server = uvicorn.Server(config)
    server.run()
