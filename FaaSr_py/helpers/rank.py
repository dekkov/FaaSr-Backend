from collections import namedtuple


def faasr_rank(faasr_payload):
    """
    Returns the rank # and total rank of the current function

    Returns:
        namedtuple with elements MaxRank and Rank | None if rank is not set
    """
    # get current function name
    curr_func_name = faasr_payload["FunctionInvoke"]

    # get current function
    curr_func = faasr_payload["ActionList"][curr_func_name]

    # define namedtuple for return type
    Rank = namedtuple("Rank", ["MaxRank", "Rank"])

    if "Rank" in curr_func and len(curr_func["Rank"]) != 0:
        # split rank
        parts = curr_func["Rank"].split("/")

        if len(parts) == 2:
            return Rank(parts[1], parts[0])
        else:
            return Rank(None, None)
    else:
        return Rank(None, None)
