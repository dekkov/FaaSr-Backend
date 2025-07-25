
import os
import re
import sys
import requests
import tarfile
import shutil
import string
import random
import logging
import subprocess
import base64
import importlib


logger = logging.getLogger(__name__)


def faasr_get_github_clone(url, base_dir="/tmp"):
    """
    Downloads a github repo clone from the repo's url

    Arguments:
        url: HTTPS url to git repo
        base_dir: directory to which GitHub repo should be cloned
    """
    pattern = r"([^/]+/[^/]+)\.git$"
    match = re.search(pattern, url)
    if not match:
        raise ValueError(f"Invalid GitHub URL: {url} — expected to end in owner/repo.git")

    repo_name = match.group(1)
    repo_path = os.path.join(base_dir, repo_name)

    if os.path.isdir(repo_path):
        shutil.rmtree(repo_path)

    result = subprocess.run(["git", "clone", "--depth=1", url, repo_path], text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Git clone failed for {url}")

    return repo_path


def faasr_get_github(faasr_source, path, token=None):
    """
    Downloads a repo specified by a github path [username/repo] to a tarball file

    Arguments:
        faasr_source: payload dict (FaaSr)
        path: username/repo/path to file
        token: GitHub PAT
    """
    # ensure path has two parts [username/repo]
    parts = path.split("/")
    if len(parts) < 2:
        err_msg = '{"faasr_install_git_repo":"github path should contain at least two parts"}\n'
        print(err_msg)
        sys.exit(1)

    # construct gh url
    username = parts[0]
    reponame = parts[1]
    repo = f"{username}/{reponame}"

    if len(parts) > 2:
        path = "/".join(parts[2:])
    else:
        path = None

    url = f"https://api.github.com/repos/{repo}/tarball"
    tar_name = f"/tmp/{reponame}.tar.gz"

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": f"Bearer {token}" if token else None
    }

    # send get request
    response1 = requests.get(
        url,
        headers=headers,
        stream=True,
    )

    # if the response code is 200 (successful), then write the content of the repo to the tarball file
    if response1.status_code == 200:
        with open(tar_name, "wb") as f:
            for chunk in response1.iter_content(chunk_size=8192):
                f.write(chunk)

        with tarfile.open(tar_name) as tar:
            root_dir = tar.getnames()[0]

            if path:
                extract_path = os.path.join(root_dir, path)
                members = [
                    mem for mem in tar.getmembers() if mem.name.startswith(extract_path)
                ]
                tar.extractall(path=f"/tmp/functions{faasr_source['InvocationID']}", members=members)
            else:
                tar.extractall(path=f"/tmp/functions{faasr_source['InvocationID']}")

        os.remove(tar_name)

        msg = '{"faasr_install_git_repo":"Successful"}\n'
        print(msg)
    else:
        try:
            err_response = response1.json()
            message = err_response.get("message")
        except Exception:
            message = "invalid or no response from GH"
        err_msg = f'{{"faasr_install_git_repo":"ERROR -- {message}"}}\n'
        print(err_msg)
        sys.exit(1)


def faasr_get_github_raw(token, path):
    """
    Gets the contents of a single file on GitHub

    Arguments:
        token: GitHub PAT
        path: username/repo/path to file

    Returns:
        Raw GitHub file (UTF-8 string)
    """
    parts = path.split("/")
    if len(parts) < 3:
        err_msg = '{"faasr_get_github_raw":"github path should contain at least three parts"}\n'
        print(err_msg)
        sys.exit(1)

    # construct gh url
    username = parts[0]
    reponame = parts[1]
    repo = f"{username}/{reponame}"
    branch = parts[2]
    filepath = "/".join(parts[3:])
    url = f"https://api.github.com/repos/{username}/{reponame}/contents/{filepath}?ref={branch}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": f"Bearer {token}" if token else None
    }

    response1 = requests.get(url, headers=headers)

    if response1.status_code == 200:
        msg = '{"faasr_install_git_repo":"Successful"}\n'
        print(msg)
        data = response1.json()
        content = data.get("content", "")
        decoded_bytes = base64.b64decode(content)
        decoded_string = decoded_bytes.decode("utf-8")
        return decoded_string
    else:
        try:
            err_response = response1.json()
            message = err_response.get("message")
        except Exception:
            message = "invalid or no response from GH"
        err_msg = f'{{"faasr_install_git_repo":"ERROR -- {message}"}}\n'
        print(err_msg)
        sys.exit(1)


def faasr_install_git_repos(faasr_source, func_type, gits, token):
    """
    Downloads content from git repo(s)

    Arguments:
        faasr_source: faasr payload (FaaSr)
        func_type: Python or R
        gits: paths repos or files to download
        token: GitHub PAT
    """
    if isinstance(gits, str):
        gits = [gits]
    if not gits:
        print('{"faasr_install_git_repo":"No git repo dependency"}\n')
    else:
        # download content from each path
        for path in gits:
            # if path is a repo, clone the repo
            if (
                path.endswith("git")
                or path.startswith("https://")
            ):
                msg = f'{{"faasr_install_git_repo":"clone github: {path}"}}\n'
                print(msg)
                faasr_get_github_clone(path)
            else:
                # if path is a python file, download
                file_name = os.path.basename(path)
                if ((file_name.endswith(".py") and func_type == "Python")
                    or (file_name.endswith(".R") and func_type == "R")):
                    msg = f'{{"faasr_install_git_repo":"get file: {file_name}"}}\n'
                    print(msg)
                    content = faasr_get_github_raw(token, path)
                    # write fetched file to disk
                    with open(file_name, "w") as f:
                        f.write(content)
                else:
                    # if the path is a non-python file, download the repo
                    msg = f'{{"faasr_install_git_repo":"get git repo files: {path}"}}\n'
                    print(msg)
                    faasr_get_github(faasr_source, path, token)


def faasr_pip_install(package):
    """
    Pip installs a single PyPI package
    """
    # run pip install [package] command
    if not package:
        print("{\"faasr_install_PyPI\":\"No PyPI package dependency\"}\n")
    else:
        command = ["pip", "install", "--no-input", package]
        subprocess.run(command, text=True)


def faasr_install_cran(package, lib_path = None):
    """
    Installs a single cran package
    """
    if not package:
        print("{\"faasr_install_cran\":\"No CRAN package dependency\"}\n")
    else:
        print(f"{{\"faasr_install_cran\":\"Install CRAN package {package}\"}}\n")
        if lib_path:
            lib_path = f'"{lib_path}"'
        else:
            lib_path = ".libPaths()[1]"
        command = ["Rscript", "-e", f'install.packages("{package}", lib={lib_path}, repos="https://cloud.r-project.org")']
        subprocess.run(command, text=True)


def faasr_pip_gh_install(path):
    """
    Installs a single package specified via a github path (name/path) using pip
    """
    parts = path.split("/")
    if len(parts) < 2:
        err_msg = '{"faasr_pip_install":"github path should contain at least two parts"}\n'
        print(err_msg)
        sys.exit(1)

    # construct gh url
    username = parts[0]
    reponame = parts[1]
    repo = f"{username}/{reponame}"
    gh_url = f"git+https://github.com/{repo}.git"

    command = ["pip", "install", "--no-input", gh_url]
    subprocess.run(command, text=True)


def faasr_install_git_packages(gh_packages, type, lib_path=None):
    """
    Install a list of git packages
    """
    if not gh_packages:
        print('{"faasr_install_git_package":"No git package dependency"}\n')
    else:
        # install each package
        for package in gh_packages:
            print(f'{{"faasr_install_git_package":"Install Github package {package}"}}\n')
            if type == "Python":
                faasr_pip_gh_install(package)
            elif type == "R":
                if lib_path:
                    lib_path = f'"{lib_path}"'
                else:
                    lib_path = ".libPaths()[1]"
                command = ["Rscript", "-e", f'withr::with_libpaths(new={lib_path}, devtools::install_github("{package}", force=TRUE))']
                subprocess.run(command, text=True)


def faasr_func_dependancy_install(faasr_source, action):
    """
    Installs the dependencies for an action's function

    Arguments:
        faasr_source: faasr payload (FaaSr)
        action: name of current action
    """
    func_type, func_name = action["Type"], action["FunctionName"]

    # get files from git repo
    gits = faasr_source["FunctionGitRepo"].get(func_name)

    # get token if present
    token = os.getenv("TOKEN")
        
    if not token:
        msg = '{"faasr_install_git_repo":"Warning: No GH token used. May hit rate limits when installing functions"}\n'
        print(msg)

    # get gh functions
    faasr_install_git_repos(faasr_source, func_type, gits, token)

    if "PyPIPackageDownloads" in faasr_source and func_type == "Python":
        pypi_packages = faasr_source["PyPIPackageDownloads"].get(func_name)
        if pypi_packages:
            for package in pypi_packages:
                faasr_pip_install(package)
    elif "FunctionCRANPackage" in faasr_source and func_type == "R":
        cran_packages = faasr_source["FunctionCRANPackage"].get(func_name)
        if cran_packages:
            for package in cran_packages:
                faasr_install_cran(package)

    # install gh packages
    if "FunctionGitHubPackage" in faasr_source:
        if func_name in faasr_source["FunctionGitHubPackage"]:
            gh_packages = faasr_source["FunctionGitHubPackage"].get(func_name)
            if gh_packages:
                faasr_install_git_packages(gh_packages, func_type)



