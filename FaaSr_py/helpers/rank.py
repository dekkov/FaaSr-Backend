from collections import namedtuple


def faasr_rank(faasr):
    """
    Returns the rank # and total rank of the current function

    RETURN: namedtuple with elements MaxRank and Rank

    In case where there is no rank, returns MaxRank, Rank = None
    """
    # get current function name
    curr_func_name = faasr["FunctionInvoke"]

    # get current function
    curr_func = faasr["FunctionList"][curr_func_name]

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
