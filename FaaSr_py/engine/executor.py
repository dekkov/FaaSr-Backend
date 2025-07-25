import os
import sys
import json
import logging
import requests
import subprocess
from multiprocessing import Process
from pathlib import Path

from FaaSr_py.engine.faasr_payload import FaaSr
from FaaSr_py.config.debug_config import global_config
from FaaSr_py.s3_api import faasr_log, faasr_put_file
from FaaSr_py.server.faasr_server import run_server, wait_for_server_start
from FaaSr_py.helpers.faasr_start_invoke_helper import faasr_func_dependancy_install


logger = logging.getLogger(__name__)


class Executor:
    """
    Handles logic related to running user function
    """
    def __init__(self, faasr: FaaSr):
        if not isinstance(faasr, FaaSr):
            err_msg = "{scheduler.py: initializer must be FaaSr instance}"
            print(err_msg)
            sys.exit(1)
        self.faasr = faasr
        self.server = None
        self.packages = []

    def call(self, action_name):
        """
        Runs a user function given action name

        Arguments:
            action_name: str -- name of the action to run
        """
        func_name = self.faasr["FunctionList"][action_name]["FunctionName"]
        func_type = self.faasr["FunctionList"][action_name]["Type"]
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
                    py_func = Process(
                        target=run_py_function,
                        args=(self.faasr, func_name, user_args, imports),
                    )
                    py_func.start()
                    py_func.join()

                    if py_func.exitcode != 0:
                        raise SystemExit(
                            f"non-zero exit code ({py_func.exitcode}) from Python function"
                        )
                elif func_type == "R":
                    r_entry_dir = Path(__file__).parent.parent / "client"
                    r_entry_path = r_entry_dir / "r_user_func_entry.R"
                    r_func = subprocess.run(
                        [
                            "Rscript",
                            str(r_entry_path),
                            func_name,
                            json.dumps(user_args),
                            self.faasr["InvocationID"],
                        ],
                        cwd = r_entry_dir
                    )
                    if r_func.returncode != 0:
                        raise SystemExit(
                            f"non-zero exit code ({r_func.returncode}) from R function"
                        )
            except Exception as e:
                err_msg = f'{{"faasr_run_user_function": "Error running user function -- {e}"}}'
                faasr_log(self.faasr, err_msg)
                print(err_msg)
                sys.exit(1)
        else:
            print("DEBUG MODE -- SKIPPING USER FUNCTION")

        # At this point, the action has finished the invocation of the user Function
        # We flag this by uploading a file with the name FunctionInvoke.done to the S3 logs folder
        # Check if directory already exists. If not, create one
        log_folder = f"{self.faasr['FaaSrLog']}/{self.faasr['InvocationID']}"
        log_folder_path = f"/tmp/{log_folder}/{self.faasr['FunctionInvoke']}/flag/"
        if not os.path.isdir(log_folder_path):
            os.makedirs(log_folder_path)
        if "Rank" in self.faasr["FunctionList"][action_name]:
            rank_unsplit = self.faasr["FunctionList"][action_name]["Rank"]
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

    def run_func(self, action_name):
        """
        Fetch and run the users function

        Arguments:
            action_name: str -- name of the action to run
        """
        # install dependencies for function
        action = self.faasr["FunctionList"][action_name]
        faasr_func_dependancy_install(self.faasr, action)

        # Run function
        try:
            print("starting server")
            self.host_server_api()
            print('run user function')
            self.call(action_name)
            print('get function return value')
            function_result = self.get_function_return()
        except SystemExit as e:
            exit_msg = f'{{"faasr_start_invoke_github_actions.py": "ERROR -- {e}"}}'
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
            self.terminate_server()
        return function_result

    def host_server_api(self, port=8000):
        """
        Starts RPC server for serverside API

        Arguments:
            port: int -- port to run the server on
        """
        self.server = Process(target=run_server, args=(self.faasr, port))
        self.server.start()
        wait_for_server_start(port)

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
        Returns user function arguments

        Returns:
            dict -- user function arguments
        """
        user_action = self.faasr["FunctionInvoke"]

        args = self.faasr["FunctionList"][user_action]["Arguments"]
        if args is None:
            return {}
        else:
            return args

    def get_function_return(self, port=8000):
        """
        Get user function result

        Arguments:
            port: int -- port to get the function result from
        
        Returns:
            result: bool | None
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
