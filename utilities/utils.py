"""
utilities file for app.py
"""
import re
import ctypes
from pickle import loads
from typing import Union
from pandas.api.types import is_object_dtype
from pandas.api.types import is_numeric_dtype
import pandas as pd


def convert(col):
    """
    convert stringified List/Dictionary to List/Dictionary
    :param col: stringified List/Dictionary
    :return: List/Dictonary
    """

    return col[col.notnull()].apply(py_to_pickle).apply(loads)


def normalize_all(target_df, col, bridge_name, is_list=True):
    """
    :param target_df: DataFrame
    :param col: Target_Column
    :param bridge_name:  bridge that connects dataframe with Target_column
    :param is_list: bool to check if col contains list or dictionary
    :return: exploded Target_column as DataFrame
    """
    target_df[col] = convert(target_df[col])
    if is_list:
        target_df = target_df.explode(col).reset_index(drop=True)
    bridge = target_df[bridge_name]
    target_df = pd.json_normalize(target_df[col])
    target_df = pd.concat([bridge, target_df], axis=1)
    return target_df


def categorize_gender(target_col):
    """
    categorize gender column
    :param target_df: DataFrame
    :param col: str | Target_column
    :return: numerical gender column turned into categorical
    """
    target_col = target_col.astype('float64')
    return target_col.replace({0.0: 'unspecified', 1.0: 'male', 2.0: 'female'}, inplace=True)


def replace_tt(row):
    """
    removes tt from row
    :param row: str
    :return: return row without tt
    """
    row = re.sub('tt', '', row)
    return row


def na_ld(target_df, columns_list):
    """
    converts str na/nulls to None
    :param target_df: DataFrame
    :param columns_list: list
    :return: return columns with fixed null values
    """

    try:
        for col in columns_list:
            if is_object_dtype(target_df[col]):
                cond = (target_df[col] == '[]') | (target_df[col] == '{}') | (target_df[col] == 'nan') | (
                            target_df[col] == '')
                target_df.loc[cond, col] = None
            elif is_numeric_dtype(target_df[col]):
                cond = (target_df[col] == 0) | (target_df[col] == 0.0)
                target_df.loc[cond, col] = None
        return target_df
    except (IndexError, KeyError) as error:
        print(error)
        return None


def py_to_pickle(stringified: Union[str, bytes]) -> bytes:
    """

    :param: str
    :return: list/dict
    out_len: some buffer len + len of literal Python code
    """
    if isinstance(stringified, bytes):
        in_bytes = stringified
    else:
        in_bytes = stringified.encode("utf8")
    in_ = ctypes.create_string_buffer(in_bytes)
    in_len = len(in_bytes)

    out_len = in_len + 1000
    out_ = ctypes.create_string_buffer(out_len)

    lib = ctypes.CDLL("libpytopickle.so")
    lib.py_to_pickle.argtypes = (ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t)
    lib.py_to_pickle.restype = ctypes.c_int

    res = lib.py_to_pickle(in_, in_len, out_, out_len)
    assert res == 0, f"there was some error in {in_bytes}"
    return out_.raw
