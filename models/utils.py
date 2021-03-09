import configparser
import os
import re
from functools import partial

from models.exceptions import InvalidTagException

__TAG_REGEX__ = re.compile("#[PYLQGRJCUV0289]+")

def env_istrue(env_var, default=None):
    v = os.getenv(env_var, default)
    return v if isinstance(v, bool) else str(v).lower() in ("1", "true","t") if v else False

def fmt_tag(tag, verify = True):
    tag = tag.upper() if tag.startswith("#") else "#" + tag.upper()
    if verify and __TAG_REGEX__.fullmatch(tag) is None:
        raise InvalidTagException(f"{tag} is not a valid Tag")
    return tag

## ^#[PYLQGRJCUV0289]+$ < regex for clan/player tag
def chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# def sized(lst, maxn):
#     """
#     Yield successive n-sized chunks from lst.
#     https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
#     """
#     for i in range(0, len(lst), n):
#         yield lst[i:i + n]


def add_method(obj, func):
    """
    Bind a function and store it in an object
    https://stackoverflow.com/questions/30294458/any-elegant-way-to-add-a-method-to-an-existing-object-in-python
    """
    setattr(obj, func.__name__, partial(func, obj))

def load_config(path):
    cfg = configparser.RawConfigParser()
    cfg.read(path)
    return cfg
