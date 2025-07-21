import os
import subprocess
import json
import requests
import sys
from multiprocessing import Process

from FaaSr_py.engine.faasr_payload import FaaSr
from FaaSr_py.config.debug_config import global_config
from FaaSr_py.s3_api import faasr_log, faasr_put_file
from FaaSr_py.server.faasr_server import run_server, wait_for_server_start
from FaaSr_py.helpers.faasr_start_invoke_helper import faasr_func_dependancy_install


class Executor:
    """
    Handles logic related to running user function
    """
    def __init__(self, faasr: FaaSr):
        self.faasr = faasr
        self.server = None
        self.packages = []

    def call(self, function):
        """
        Runs a user function
        """
        func_name = self.faasr["FunctionList"][function]["FunctionName"]
        func_type = self.faasr["FunctionList"][function]["Type"]
        if "PackageImports" in self.faasr:
            imports = self.faasr["PackageImports"].get(func_name)
        else:
            imports = []
        user_args = self.get_user_function_args()

        if not global_config.SKIP_USER_FUNCTION:
            try:
                if func_type == "Python":
                    # entry script for py function
                    from FaaSr_py.client.py_user_func_entry import run_py_function

                    # run user func as seperate process
                    user_function = Process(
                        target=run_py_function,
                        args=(self.faasr, func_name, user_args, imports),
                    )
                    user_function.start()
                    user_function.join()
                elif type == "R":
                    # r_user_func_entry.R must be baked into container
                    r_func = subprocess.run(
                        [
                            "RScript",
                            "r_user_func_entry.R",
                            func_name,
                            json.dumps(user_args),
                            self.faasr["InvocationID"],
                        ]
                    )
                    if r_func.returncode != 0:
                        raise SystemExit(
                            f"non-zero exit code from R function: {r_func.returncode}"
                        )
            except RuntimeError as e:
                nat_err_msg = f'{{"faasr_run_user_function": "Error running user function -- {e}"}}'
                err_msg = f"Errors in the user function: {str(self.faasr["FunctionInvoke"])} check the log for the detail "
                faasr_log(self.faasr, nat_err_msg)
                # to-do: remove print
                print(nat_err_msg)
                print(err_msg)
                raise RuntimeError(err_msg)
        else:
            print("DEBUG MODE -- SKIPPING USER FUNCTION")

        # At this point, the action has finished the invocation of the user Function
        # We flag this by uploading a file with the name FunctionInvoke.done to the S3 logs folder
        # Check if directory already exists. If not, create one
        log_folder = f"{self.faasr['FaaSrLog']}/{self.faasr['InvocationID']}"
        log_folder_path = f"/tmp/{log_folder}/{self.faasr['FunctionInvoke']}/flag/"
        if not os.path.isdir(log_folder_path):
            os.makedirs(log_folder_path)
        if "Rank" in self.faasr["FunctionList"][function]:
            rank_unsplit = self.faasr["FunctionList"][function]["Rank"]
            if len(rank_unsplit) != 0:
                rank = rank_unsplit.split("/")[0]
                self.faasr["FunctionInvoke"] = f"{self.faasr['FunctionInvoke']}.{rank}"
        file_name = f"{self.faasr['FunctionInvoke']}.done"
        with open(f"{log_folder_path}/{file_name}", "w") as f:
            f.write("True")

        # Put .done file in S3
        faasr_put_file(
            config=self.faasr,
            local_folder=log_folder_path,
            local_file=file_name,
            remote_folder=log_folder,
            remote_file=file_name,
        )

    def run_func(self, function):
        """
        Fetch and run the users function
        """
        # get func type and name
        func_type = self.faasr["FunctionList"][function]["Type"]
        func_name = self.faasr["FunctionList"][function]["FunctionName"]

        print('install dependencies')
        faasr_func_dependancy_install(self.faasr, func_name, func_type)

        # Run function
        try:
            print("starting server")
            self.host_server_api()
            print('run user function')
            self.call(function)
            print('get function return value')
            function_result = self.get_function_return()
        except SystemExit as e:
            exit_msg = f'{{"faasr_start_invoke_github_actions.py": "ERROR -- non-zero exit code from user function"}}'
            print(exit_msg)
            sys.exit(1)
        except RuntimeError as e:
            err_msg = f'{{"faasr_start_invoke_github_actions.py": "RUNTIME ERROR while running user function -- {e}"}}'
            print(err_msg)
            sys.exit(1)
        except Exception as e:
            err_msg = f'{{"faasr_start_invoke_github_actions.py": ERROR -- MESSAGE: {e}"}}'
            print(err_msg)
            sys.exit(1)
        finally:
            # Clean up server
            print("terminate server process")
            self.terminate_server()
        return function_result

    def host_server_api(self, port=8000):
        """
        Starts RPC server for serverside API
        """
        self.server = Process(target=run_server, args=(self.faasr, port))
        self.server.start()
        print("waiting")
        wait_for_server_start(port)
        print("done waiting")

    def terminate_server(self):
        """
        Terminate RPC server
        """
        if isinstance(self.server, Process):
            self.server.terminate()
        else:
            err_msg = "{{executor.py: ERROR -- Tried to terminate server, but no server running}}"
            print(err_msg)
            sys.exit(1)

    def get_user_function_args(self):
        """
        Returns function arguments
        """
        user_action = self.faasr["FunctionInvoke"]

        args = self.faasr["FunctionList"][user_action]["Arguments"]
        if args is None:
            return []
        else:
            return args

    def get_function_return(self, port=8000):
        """
        Get function result

        @return result (bool | None)
        """
        try:
            return_response = requests.get(f"http://127.0.0.1:{port}/faasr-get-return")
            return_val = return_response.json()
        except requests.exceptions.RequestException as e:
            err_msg = (
                f'{{"executor.py": "REQUESTS ERROR GETTING FUNCTION RESULT -- {e}"}}'
            )
            print(err_msg)
            raise RuntimeError(err_msg)
        except Exception as e:
            err_msg = (
                f'{{"executor.py": "UNKOWN ERROR GETTING FUNCTION RESULT -- {e}"}}'
            )
            print(err_msg)
            raise RuntimeError(err_msg)

        if return_val.get("Error"):
            if return_val.get("Message"):
                err_msg = f'{{"executor.py": "ERROR IN USER FUNCTION -- MESSAGE: {return_val["Message"]} -- ABORTING"}}'
            else:
                err_msg = '{{"executor.py": "ERROR IN USER FUNCTION -- ABORTING"}}'
            print(err_msg)
            faasr_log(self.faasr, err_msg)
            raise RuntimeError(err_msg)

        return return_val["FunctionResult"]
