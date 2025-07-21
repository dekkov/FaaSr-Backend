from faasr_server_python import *
from FaaSr_py.config.debug_config import global_config
from FaaSr_py.helpers.py_func_helper import faasr_import_function, source_packages


def run_py_function(faasr, func_name, args, imports):
    """
    Sources python function from file and runs it
    """
    if global_config.USE_LOCAL_USER_FUNC:
        print("DEBUG MODE -- USING DEBUG USER FUNC")
        global_config.source_local_func()
        user_function = global_config.local_func
    else:
        user_function = faasr_import_function(
            func_name, f"/tmp/functions{faasr['InvocationID']}"
        )

    # Ensure user function is present
    if not user_function:
        err_msg = f"cannot find function {func_name}"
        faasr_log(err_msg)
        print(err_msg)
        faasr_exit(err_msg)

    user_function.__globals__["faasr_put_file"] = faasr_put_file
    user_function.__globals__["faasr_get_file"] = faasr_get_file
    user_function.__globals__["faasr_delete_file"] = faasr_delete_file
    user_function.__globals__["faasr_get_folder_list"] = faasr_get_folder_list
    user_function.__globals__["faasr_log"] = faasr_log

    print(f"using local function {global_config.USE_LOCAL_USER_FUNC}")

    if global_config.USE_LOCAL_USER_FUNC:
        result = user_function(**global_config.LOCAL_FUNC_ARGS)
    else:
        func_namespace = user_function.__globals__
        source_packages(func_namespace, imports)
        result = user_function(**args)

    faasr_return(result)
