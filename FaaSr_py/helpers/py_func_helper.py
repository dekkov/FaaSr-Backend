import os
import sys
import importlib


def faasr_import_function(func_name, directory="."):
    """
    This function imports Python user function from file

    Returns None if function not found
    """
    ignore_files = [
        "test_gh_invoke.py",
        "test.py",
        "func_test.py",
        "faasr_start_invoke_helper.py",
        "faasr_start_invoke_openwhisk.py",
        "faasr_start_invoke_aws-lambda.py",
        "faasr_start_invoke_github_actions.py",
    ]
    # convert relative path to absolute path
    directory = os.path.abspath(directory)

    # add directory to python path
    if directory not in sys.path:
        sys.path.insert(0, directory)

    # walk through  directories and subdirectories
    for root, dirs, files in os.walk(directory):
        # filter for py files
        py_files = [file for file in files if file.endswith(".py")]
        for f in py_files:
            if f not in ignore_files:
                print(f'{{"faasr_source_py_files":"Source python file {f}"}}\n')
                try:
                    rel_path = os.path.relpath(root, directory)
                    if rel_path == ".":
                        # file is in the base directory
                        module_name = os.path.splitext(f)[0]
                    else:
                        # file is in a subdirectory
                        module_path = os.path.join(rel_path, os.path.splitext(f)[0])
                        module_name = module_path.replace(os.path.sep, ".")

                    # import module
                    module = importlib.import_module(module_name)

                    # store functions from module to return
                    for name, obj in module.__dict__.items():
                        if name == func_name and callable(obj):
                            return obj

                except Exception as e:
                    err_msg = f'{{"faasr_source_py_files":"python file {f} has following source error: {str(e)}"}}\n'
                    print(err_msg)
                    sys.exit(1)
    return None


def source_packages(__globals__, packages):
    """
    Sources packages
    """
    if not isinstance(packages, list):
        packages = [packages]

    for package in packages:
        try:
            __globals__[package] = importlib.import_module(package)
            msg = f'{{"py_func_helper.py: succesfully imported package {package}}}'
            print(msg)
        except ImportError as e:
            err_msg = (
                f'{{"py_func_helper.py: failed to import package {package} -- {e}}}'
            )
            print(err_msg)
            sys.exit(1)
