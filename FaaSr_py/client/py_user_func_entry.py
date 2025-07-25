import logging

from pathlib import Path
from FaaSr_py.client.py_client_stub import *
from FaaSr_py.config.debug_config import global_config
from FaaSr_py.helpers.py_func_helper import faasr_import_function_walk, faasr_import_function, source_packages, local_wrap


logger = logging.getLogger(__name__)


def run_py_function(faasr, func_name, args, imports):
    """
    Sources python function from file and runs it
    """
    if global_config.USE_LOCAL_USER_FUNC:
        print("CONFIG ----- USING LOCAL FUNCTION")
        try:
            func_path = Path(global_config.LOCAL_FUNCTION_PATH).resolve()
            func_name_local = global_config.LOCAL_FUNCTION_NAME

            user_function = faasr_import_function(func_path, func_name_local)
        except:
            raise RuntimeError("failed to get local function")
    else:
        user_function = faasr_import_function_walk(
            func_name, dir=f"/tmp/functions{faasr['InvocationID']}"
        )

    # Ensure user function is present
    if not user_function:
        err_msg = f"{{py_user_func_entry.py: cannot find function {func_name}}}"
        faasr_log(err_msg)
        print(err_msg)
        faasr_exit(err_msg)

    # Add FaaSr client stubs to user function's namespace
    user_function.__globals__["faasr_put_file"] = faasr_put_file
    user_function.__globals__["faasr_get_file"] = faasr_get_file
    user_function.__globals__["faasr_delete_file"] = faasr_delete_file
    user_function.__globals__["faasr_get_folder_list"] = faasr_get_folder_list
    user_function.__globals__["faasr_log"] = faasr_log
    user_function.__globals__["faasr_rank"] = faasr_rank
    user_function.__globals__["faasr_get_s3_creds"] = faasr_get_s3_creds
    user_function.__globals__["faasr_return"] = faasr_return
    user_function.__globals__["faasr_exit"] = faasr_exit


    if global_config.USE_LOCAL_USER_FUNC:
        print(f"using local function {global_config.LOCAL_FUNCTION_NAME}")
        result = local_wrap(user_function)(**global_config.LOCAL_FUNC_ARGS)
    else:
        func_namespace = user_function.__globals__
        source_packages(func_namespace, imports)
        result = user_function(**args)

    faasr_return(result)