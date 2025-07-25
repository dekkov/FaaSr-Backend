import json
import sys
import re
import logging

from pathlib import Path
from collections import defaultdict
from jsonschema import validate
from jsonschema.exceptions import ValidationError


logger = logging.getLogger(__name__)


def validate_json(payload):
    """
    Verifies JSON payload is compliant with the FaaSr schema

    Arguments:
        payload: FaaSr payload to validate
    """
    if isinstance(payload, str):
        payload = json.loads(payload)

    schema_path = Path(__file__).parent.parent / "FaaSr.schema.json"
    if not schema_path.exists():
        err_msg = f'{{"faasr_validate_json":"FaaSr schema file not found at {schema_path}"}}\n'
        print(err_msg)
        sys.exit(1)

    # Open FaaSr schema
    with open(schema_path, "r") as f:
        schema = json.load(f)

    # Compare payload against FaaSr schema and except if they do not match
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as e:
        err_msg = (
            f'{{"faasr_validate_json":"JSON not compliant with FaaSr schema : {e.message}"}}\n',
        )
        print(err_msg)
        sys.exit(1)
    return True


def is_cyclic(adj_graph, curr, visited, stack):
    """
    Recursive function that if there is a cycle in a directed
    graph specified by an adjacency list

    Arguments:
        adj_graph: adjacency list for graph (dict)
        curr: current node
        visited: set of visited nodes (set)
        stack: list of nodes in recursion call stack (list)

    Returns:
        bool: True if cycle exists, False otherwise
    """
    # if the current node is in the recursion call
    # stack then there must be a cycle in the graph
    if curr in stack:
        return True

    curr_rank = curr.split(".")
    if len(curr_rank) > 1:
        # if the current node has a rank, then remove the rank
        curr = curr_rank[0]

    # add current node to recursion call stack and visited set
    visited.add(curr)  # remove rank from function name
    stack.append(curr)  # remove rank from function name

    # check each successor for cycles, recursively calling is_cyclic()
    for child in adj_graph[curr]:
        if child not in visited and is_cyclic(adj_graph, child, visited, stack):
            err = (
                f'{{"faasr_check_workflow_cycle":"Function loop found from node {curr} to {child}"}}\n'
            )
            print(err)
            sys.exit(1)
        elif child in stack:
            err = (
                f'{{"faasr_check_workflow_cycle":"Function loop found from node {curr} to {child}"}}\n'
            )
            print(err)
            sys.exit(1)

    # no more successors to visit for this branch and no cycles found
    # remove current node from recursion call stack
    stack.pop()
    return False


def build_adjacency_graph(payload):
    """
    This function builds an adjacency list for the FaaSr workflow graph

    Arguments:
        payload: FaaSr payload dict
    Returns:
        adj_graph: adjacency list for graph -- dict(function: successor)
    """
    adj_graph = defaultdict(list)

    # Build adjacency list from FunctionList
    for func in payload["FunctionList"].keys():
        invoke_next = payload["FunctionList"][func]["InvokeNext"]
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]
        for child in invoke_next:
            if isinstance(child, dict):
                for conditional in child.values():
                    adj_graph[func].extend(all_funcs_from_list(conditional))
            else:
                adj_graph[func].extend(all_funcs_from_rank(child))
    return adj_graph


def check_dag(payload):
    """
    This method checks for cycles, repeated function names, or unreachable nodes in the workflow
    and aborts if it finds any

    Arguments:
        payload: FaaSr payload dict
    Returns:
        predecessors: dict -- map of function predecessors
    """
    adj_graph = build_adjacency_graph(payload)

    # Initialize empty recursion call stack
    stack = []

    # Initialize empty visited set
    visited = set()

    # Initialize predecessor list
    pre = predecessors_list(adj_graph)

    # Find initial function in the graph
    start = False
    for func in payload["FunctionList"]:
        if len(pre[func]) == 0:
            start = True
            # This function stores the first function with no predecessors
            # In the cases where there is multiple functions with no
            # predecessors, an unreachable state error will occur later
            first_func = func
            break

    # Ensure there is an initial action
    if start is False:
        err_msg = (
            '{"faasr_check_workflow_cycle":"function loop found: no initial action"}\n'
        )
        print(err_msg)
        sys.exit(1)

    # Check for cycles
    is_cyclic(adj_graph, first_func, visited, stack)

    # Check if all of the functions have been visited by the DFS
    # If not, then there is an unreachable state in the graph
    for func in payload["FunctionList"]:
        if func.split(".")[0] not in visited:
            err = '{"check_workflow_cycle":"unreachable state found: ' + func + '"}\n'
            print(err)
            sys.exit(1)

    return pre[payload["FunctionInvoke"]]


def predecessors_list(adj_graph):
    """This function returns a map of action predecessor pairs

    Arguments:
        adj_graph: adjacency list for graph -- dict(function: successor)
    """
    pre = defaultdict(list)
    for func1 in adj_graph:
        for func2 in adj_graph[func1]:
            pre[func2].append(func1)
    return pre


def validate_payload(faasr):
    # Verifies that the faasr payload is a DAG, meaning that there is no cycles
    # If the payload is a DAG, then this function returns a predecessor list for the workflow
    # If the payload is not a DAG, then the action aborts
    pre = check_dag(faasr)

    # Verfies the validity of S3 data stores, checking the server status and ensuring that the specified bucket exists
    # If any of the S3 endpoints are invalid or any data store server are unreachable, the action aborts
    faasr.s3_check()

    # Initialize log if this is the first action in the workflow
    if len(pre) == 0:
        faasr.init_log_folder()

    # If there are more than 1 predecessor, then only the final action invoked will sucessfully run
    # This function validates that the current action is the last invocation; otherwise, it aborts
    if len(pre) > 1:
        faasr.abort_on_multiple_invocations(pre)


def all_funcs_from_rank(str):
    """
    This function returns a list of all the actual actions called
    by a function with a rank (e.g. "func(3)" returns ["func.1", "func.2", "func.3"])

    Arguments:
        str: function name with rank
    Returns:
        str: function name without rank
    """
    parts = str.split("(")
    if len(parts) != 2 or not parts[1].endswith(")"):
        return [str]
    rank = int(parts[1][:-1])
    return [f"{parts[0]}.{i}" for i in range(1, rank + 1)]


def all_funcs_from_list(func_list):
    """
    This function returns a list of all the actual actions called
    by a list of functions with ranks (e.g. ["func(3)", "func2(2)"]
    returns ["func.1", "func.2", "func.3", "func2.1", "func2.2"])

    Arguments:
        func_list: list of function names with rank
    Returns:
        func_list: list of function names without rank
    """
    return [func_name for func in func_list for func_name in all_funcs_from_rank(func)]
