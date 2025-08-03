import json
import logging
import base64
import time
import requests
from datetime import datetime

logger = logging.getLogger(__name__)


def validate_jwt_token(token):
    """
    Validates JWT tokens for SLURM authentication
    Checks token format, decodes payload, and verifies expiration
    
    Arguments:
        token: str -- JWT token string to validate
    Returns:
        dict: {"valid": bool, "error": str}
    """
    if not token or not token.startswith("eyJ"):
        return {"valid": False, "error": "Invalid token format"}
    
    try:
        # Decode JWT payload (second part)
        parts = token.split(".")
        if len(parts) < 2:
            return {"valid": False, "error": "Malformed JWT token"}
        
        payload = parts[1]
        # Add padding if necessary
        padding = 4 - (len(payload) % 4)
        if padding != 4:
            payload += "=" * padding
        
        # Decode base64
        decoded_bytes = base64.b64decode(payload)
        decoded_json = decoded_bytes.decode('utf-8')
        payload_data = json.loads(decoded_json)
        
        # Check expiration
        if payload_data.get("exp"):
            current_time = time.time()
            if current_time >= payload_data["exp"]:
                exp_time = datetime.fromtimestamp(payload_data["exp"])
                return {"valid": False, "error": f"Token expired at {exp_time}"}
        
        return {"valid": True, "error": None}
        
    except Exception as e:
        return {"valid": False, "error": f"Token validation error: {str(e)}"}


def create_job_script(faasr, actionname):
    """
    Create SLURM job script for FaaSr execution
    
    Arguments:
        faasr: FaaSrPayload -- workflow payload
        actionname: str -- name of the action
    Returns:
        str: job script content
    """
    # Get container image with fallback to default
    container_image = "faasr/openwhisk-tidyverse:latest"
    action_containers = faasr.get("ActionContainers", {})
    if actionname in action_containers and action_containers[actionname]:
        container_image = action_containers[actionname]
    
    # Determine entry point based on function type
    action_list = faasr.get("ActionList", {})
    function_type = action_list.get(actionname, {}).get("Type", "R")
    
    if function_type == "Python":
        entry_command = "python3 /action/faasr_start_invoke_slurm.py"
    else:  # Default to R
        entry_command = "Rscript /action/faasr_start_invoke_slurm.R"
    
    # Create job script following FaaSr patterns
    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name=faasr-{actionname}",
        f"#SBATCH --output=faasr-{actionname}-%j.out",
        f"#SBATCH --error=faasr-{actionname}-%j.err",
        "",
        "# FaaSr SLURM Job Script",
        f"echo \"Starting FaaSr job: {actionname}\"",
        "echo \"Job ID: $SLURM_JOB_ID\"",
        "echo \"Node: $SLURMD_NODENAME\"",
        "echo \"Time: $(date)\"",
        "",
        "# Check container runtime availability",
        "if command -v docker &> /dev/null; then",
        "    CONTAINER_CMD=\"docker\"",
        "elif command -v podman &> /dev/null; then",
        "    CONTAINER_CMD=\"podman\"",
        "else",
        "    echo \"Error: Neither Docker nor Podman found\"",
        "    exit 1",
        "fi",
        "",
        "echo \"Using container runtime: $CONTAINER_CMD\"",
        "",
        "# Execute FaaSr container with payload",
        "$CONTAINER_CMD run --rm --network=host \\",
        "  -e FAASR_PAYLOAD=\"$FAASR_PAYLOAD\" \\",
        f"  {container_image} \\",
        f"  /bin/bash -c 'cd /action && {entry_command}'",
        "",
        f"echo \"FaaSr job completed: {actionname}\"",
        "echo \"End time: $(date)\""
    ]
    
    return "\n".join(script_lines)


def get_resource_requirements(faasr, actionname, server_info):
    """
    Extract resource requirements for a function with fallback hierarchy:
    Function-level → Server-level → Default values
    
    Arguments:
        faasr: FaaSrPayload -- workflow payload
        actionname: str -- name of the action/function
        server_info: dict -- server configuration information
    Returns:
        dict: resource requirements
    """
    action_list = faasr.get("ActionList", {})
    action_config = action_list.get(actionname, {})
    
    # Function-level resources (highest priority)
    function_resources = action_config.get("Resources", {})
    
    # Extract with fallback hierarchy: Function → Server → Default
    config = {
        "partition": (
            function_resources.get("Partition") or
            server_info.get("Partition") or
            "faasr"
        ),
        "nodes": (
            function_resources.get("Nodes") or
            server_info.get("Nodes") or
            1
        ),
        "tasks": (
            function_resources.get("Tasks") or
            server_info.get("Tasks") or
            1
        ),
        "cpus_per_task": (
            function_resources.get("CPUsPerTask") or
            server_info.get("CPUsPerTask") or
            1
        ),
        "memory_mb": (
            function_resources.get("Memory") or
            server_info.get("Memory") or
            1024
        ),
        "time_limit": (
            function_resources.get("TimeLimit") or
            server_info.get("TimeLimit") or
            60
        ),
        "working_dir": (
            function_resources.get("WorkingDirectory") or
            server_info.get("WorkingDirectory") or
            "/tmp"
        )
    }
    
    return config


def make_slurm_request(endpoint, method="GET", headers=None, body=None, token=None, username=None):
    """
    Helper function to send HTTP requests to SLURM REST API
    
    Arguments:
        endpoint: str -- full URL endpoint
        method: str -- HTTP method
        headers: dict -- HTTP headers
        body: dict -- request body (optional)
        token: str -- JWT token from server configuration
        username: str -- username from server configuration
    Returns:
        requests.Response: HTTP response object
    """

    if headers is None:
        headers = {}
    
    # Add SLURM authentication headers (same as R package)
    if token and token.strip():
        headers['X-SLURM-USER-TOKEN'] = token
        headers['X-SLURM-USER-NAME'] = username or "ubuntu"
        logger.info(f"Added SLURM auth headers for user: {username or 'ubuntu'}")
    else:
        logger.warning("No SLURM token provided - request may fail")

    try:
        if method.upper() == "GET":
            response = requests.get(
                url=endpoint,
                headers=headers,
                timeout=30
            )
        elif method.upper() == "POST":
            response = requests.post(
                url=endpoint,
                headers=headers,
                json=body,
                timeout=30
            )
        elif method.upper() == "PUT":
            response = requests.put(
                url=endpoint,
                headers=headers,
                json=body,
                timeout=30
            )
        elif method.upper() == "DELETE":
            response = requests.delete(
                url=endpoint,
                headers=headers,
                timeout=30
            )
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        return response
        
    except requests.exceptions.Timeout:
        logger.error(f"SLURM request timed out: {endpoint}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error(f"SLURM connection error: {endpoint}")
        raise
    except Exception as e:
        logger.error(f"SLURM request failed: {e}")
        raise
