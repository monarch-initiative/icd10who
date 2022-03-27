"""Utilities"""
import operator
from functools import reduce
from typing import Any, Dict, List, Union


key_type = Union[int, float, str, bool]


def get_nested(d: Dict, path: List[key_type]):
    """https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys"""
    return reduce(operator.getitem, path, d)


def set_nested(dic, keys, value):
    """https://stackoverflow.com/a/37704379/5258518"""
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value


def del_nested(d: Dict, path: List[key_type]):
    """https://stackoverflow.com/questions/47911607/remove-key-from-nested-python-dictionary-via-list-of-keys
    https://mathspp.com/blog/pydonts/unpacking-with-starred-assignments"""
    *leading_path, key = path
    # noinspection PyUnresolvedReferences
    del reduce(operator.getitem, leading_path, d)[key]


def kv_recursive_generator(d):
    """https://stackoverflow.com/a/66189132/5258518"""
    for key, value in d.items():
        yield key, value
        if isinstance(value, dict):
            yield from kv_recursive_generator(value)
