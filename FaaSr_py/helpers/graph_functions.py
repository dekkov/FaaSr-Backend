import json
import sys
import re
from collections import defaultdict
from jsonschema import validate
from jsonschema.exceptions import ValidationError


def validate_json(payload):
    """
    This method is used to verify that the JSON payload is compliant with the FaaSr schema
    """
    if isinstance(payload, str):
        payload = json.loads(payload)

    # Open FaaSr schema
    with open("FaaSr.schema.json", "r") as f:
        schema = json.load(f)

    # Compare payload against FaaSr schema and except if they do not match
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as e:
        err_msg = (
            f'{{"faasr_validate_json":"JSON not compliant with FaaSr schema : {e.message}"}}\n',
        )
        print(err_msg)
    return True


def is_cyclic(adj_graph, curr, visited, stack):
    """This recursive function checks if there is a cycle in a directed
    graph specified by an adjacency list

    parameters:
        adj_graph(dict): adjacency list for graph
        curr(str): current node
        visited(set): set of visited nodes
        stack(list[]): list of nodes in recursion call stack
    """
    # if the current node is in the recursion call
    # stack then there must be a cycle in the graph
    if curr in stack:
        return True

    # add current node to recursion call stack and visited set
    visited.add(curr)
    stack.append(curr)

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


def check_dag(payload):
    """
    This method checks for cycles, repeated function names, or unreachable nodes in the workflow
    and aborts if it finds any

    returns a list of predecessors for the current function
    """

    adj_graph = defaultdict(list)

    # Build the adjacency list
    for func in payload["FunctionList"].keys():
        invoke_next = payload["FunctionList"][func]["InvokeNext"]
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]
        for child in invoke_next:
            if isinstance(child, dict):
                for conditional in child.values():
                    child = re.sub(r"\(.*", "", conditional)
                    adj_graph[func].append(conditional)
            else:
                child = re.sub(r"\(.*", "", child)
                adj_graph[func].append(child)

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
        if func.split("(")[0] not in visited:
            err = '{"check_workflow_cycle":"unreachable state found: ' + func + '"}\n'
            print(err)
            sys.exit(1)

    return pre[payload["FunctionInvoke"]]


def predecessors_list(adj_graph):
    """This function returns a map of action predecessor pairs

    parameters:
        adj_graph(dict): adjacency list for graph (function: successor)
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
