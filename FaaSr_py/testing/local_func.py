import random2
import uuid6

def default_func(test=32):
    """
    Example function ran when USE_LOCAL_USER_FUNC == True 
    """
    num = random2.randint(1, 10)
    print(f"rand 1-10: {num}")
    uuid = uuid6.uuid6()
    print(f"rand uuid: {uuid}")
    print(uuid)
    print(f"param test: {test}")
    return True