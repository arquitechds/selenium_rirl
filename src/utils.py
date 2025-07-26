import functools

def run_twice(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        results = []
        for _ in range(2):
            results.append(func(*args, **kwargs))
        return results
    return wrapper

import re
import unicodedata
import os
import io
import shutil
import calendar
from typing import Set, List, Any, Dict, Callable, Iterable, Tuple
from itertools import takewhile
from datetime import datetime, date
from functools import partial, wraps, update_wrapper
from numbers import Number
import hashlib

from unicodedata import normalize
import requests
from loguru import logger
import pandas as pd
import numpy as np
import boto3


def add_remove_delta_from_interval_references(df: pd.DataFrame)->pd.DataFrame:
    """Removes delta from min and adds delta to interval references.
    (Should this be the other way around?)

    Args:
        df (pd.DataFrame): A dataframe with interval references

    Returns:
        pd.DataFrame: The delta modified interval references dataframe
    """
    for col in df.columns:
        if col.endswith('_max'):
            df[col] = df[col] + 0.000001
        elif col.endswith('_min'):
            df[col] = df[col] - 0.000001
    return df

def get_df_difference(main: pd.DataFrame,
                      smaller: pd.DataFrame) -> pd.DataFrame:
    """Gets the difference between a main dataframe and a smaller dataframe.
    Only works with daframes with no duplicates themselves.

    Args:
        main (pd.DataFrame): The dataframe with all the records.
        smaller (pd.DataFrame): The dataframe with some of the records but not
            all

    Returns:
        pd.DataFrame: A DataFrame that returns the records that are in the main
            but not in the smaller.
    """
    return pd.concat([main, smaller],
                     ignore_index=True).drop_duplicates(keep=False)

def df_has_nones(df: pd.DataFrame) -> bool:
    """Returns True if the dataframe has any None values.

    Args:
        df (pd.DataFrame): Any dataframe

    Returns:
        bool: Whether the dataframe has any None Values
    """
    return df.isnull().values.any()

def filter_list(string_list: List, substring_list: List) -> List:
    """Filters a list of strings by a list of substrings.

    Args:
        string_list (list): A list of strings
        substring_list (list): The list of allowed substrings

    Returns:
        List: The filtered list of strings
    """
    return [
        str for str in string_list if any(sub in str for sub in substring_list)
    ]

def get_max_date_and_df_max_date_subset(df: pd.DataFrame,
                                        date_col: str = 'date') -> Tuple:
    """Gets the records of max date in a dataframe followed by the max date in
    the dataframe.

    Args:
        df (pd.DataFrame): A dataframe with a date column.
        date_col (str, optional): The datecol to filter by. Defaults to 'date'.

    Returns:
        pd.DataFrame, datetime: The dataframe with the max column filtered and
            the max date of the dataframe.
    """
    max_date = df[date_col].max()
    return df[df[date_col] == max_date], max_date

def keep_value_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Filters a dataframe to keep only numeric values from the value column.

    Args:
        df (pd.DataFrame): A tukan data table dataframe

    Returns:
        _type_: The filtered dataframe.
    """
    before = len(df)
    new_df = df[~pd.to_numeric(df.value, errors='coerce').isnull()]
    if len(new_df) < before:
        logger.info(
            f'{before-len(new_df)} values were removed from the dataframe for being non-numeric'
        )
        return new_df
    else:
        return new_df

def value_fix(df: pd.DataFrame, drop_zeroes: bool=True) -> pd.DataFrame:
    df2 = df.copy()
    df2.loc[:, 'value'] = df2['value'].astype(float)
    # Round the values in the specified column to the specified precision
    df2.loc[:, 'value'] = df2['value'].astype(float).round(6)
    # Convert the values to strings and remove the trailing zeroes if specified
    if drop_zeroes:
        df2.loc[:, 'value'] = df2['value'].apply(
            lambda x: '{:.{prec}f}'.format(x, prec=6).rstrip('0').rstrip('.')
            )
    return df2

def strip_accents(s: str) -> str:
    """Removes the accented characters from a string.

    Args:
        s (str): Any String

    Returns:
        str: String without accented characters
    """
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn').replace('  ', ' ')

def remove_non_alphanum(s: str) -> str:
    """Removes special characters from a string.

    Args:
        s (str): Any string

    Returns:
        str: The string without special characters
    """
    return ''.join(char for char in s if char.isalnum())

def remove_subscript_upperscript(s: str) -> str:
    """Removes subscript and uppperscript characters from a string.

    Args:
        s (str): Any string

    Returns:
        _type_: The string without subscript and uppperscript characters
    """
    return "".join(c for c in s if unicodedata.category(c) not in ['No', 'Lo'])

def mata_acentos(s):
    """It takes away the accents present in any string

    Args:
        s (str): string to remove accents
    """
    s = re.sub(
        r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+",
        r"\1", normalize('NFD', s), 0, re.I)
    s = normalize('NFC', s)
    return s

def tukan_text_treater(text: str, type_of_replace: str = 'soft') -> str:
    """The main funtion to treat text in TUKAN.

    Args:
        text (_type_): Any string
        type_of_replace (str, optional): The type of replace to be done. Accepts
            "soft","hard" and "super_hard" as arguments
        Soft replace removes accents, strips the text and lowers it
        Hard replace is the same as soft replace but also removes special
            characters
        Super Hard is the same as hard but also removes subscript and
            uppperscript characters
        .Defaults to 'soft'.

    Returns:
        str: A string without accents, special characters, subscript and
            uppperscript characters
    """
    if type_of_replace == 'soft':
        return mata_acentos(strip_accents(text.strip().lower()))
    elif type_of_replace == 'hard':
        return remove_non_alphanum(
            mata_acentos(strip_accents(text.strip().lower())))
    elif type_of_replace == 'super_hard':
        return remove_subscript_upperscript(
            remove_non_alphanum(
                mata_acentos(strip_accents(text.strip().lower()))))

def replace_is_nones_with_nones_in_dataframe_column(df: pd.DataFrame,
                                                    column: str,
                                                    replace_with: str = None
                                                    ) -> pd.Series:
    """Used for replacing any "None" Values in a series with a given value.

    Args:
        series (pd.Series): A pandas series
        replace_with (_type_, optional): The value to replace None values with.
            Defaults to None.

    Returns:
        pd.Series: The series with the None values replaced.
    """
    df[column] = [replace_with if is_none(x) else x for x in df[column]]
    return df

def tukan_df_multiple_column_text_treater(df: pd.DataFrame,
                                          columns: List,
                                          type_of_replace: str = 'soft'):
    df = df.copy()
    for col in columns:
        df[col] = tukan_df_column_text_treater(df, col, type_of_replace)
    return df


def tukan_df_column_text_treater(df: pd.DataFrame, column: str,
                                 type_of_replace: str = 'soft') -> pd.DataFrame:
    """Applies the tukan text treater to a DataFrame column.

    Args:
        df_DataFrame (pd.DataFrame): Any pd.DataFrame
        column (str): The column name.
        type_of_replace (str, optional): the type of replace in the text
            treater. Defaults to 'soft'.

    Returns:
        _type_: pd.DataFrame column
    Be careful using with float and int columns
    """
    df = df.copy()
    df[column] = df[column].astype(str)

    func_remove_nones = func_remove_nones_from_str()
    df[column] = df[column].apply(func_remove_nones)

    func_text_treater = func_to_treat_text(type_of_replace)
    df[column] = df[column].apply(func_text_treater)

    df[column] = df[column].apply(func_remove_nones)
    return df[column]

def func_remove_nones_from_str(replace_with: str = None):

    def wrapped(x: str) -> str:
        none_list = [
            None, 'none', 'nan', 'None', 'no disponible', 'n/d', '<NA>'
        ]
        return replace_with if x in none_list or pd.isnull(x) else x

    return wrapped

def func_to_treat_text(type_of_replace: str = 'soft'):
    type_replace_to_func = {
        'soft': soft_text_replace,
        'hard': hard_text_replace,
        'smart': smart_text_replace,
        'bbva': bbva_text_replace,
        'bbva_free': bbva_free_text_replace,
        'super_hard': super_hard_text_replace,
        'free': free_text_replace
    }
    return type_replace_to_func.get(type_of_replace)

def execute_func_if_not_none(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(text: str) -> str:
        if isinstance(text, str):
            return func(text)
        else:
            return None

    return wrapper

@execute_func_if_not_none
def super_hard_text_replace(text: str) -> str:
    return remove_subscript_upperscript(hard_text_replace(text))

@execute_func_if_not_none
def hard_text_replace(text: str) -> str:
    return remove_non_alphanum(soft_text_replace(text))

@execute_func_if_not_none
def soft_text_replace(text: str) -> str:
    return mata_acentos(strip_accents(text.strip().lower()))

@execute_func_if_not_none
def bbva_text_replace(s):
    # Replace ñ and Ñ with "ni"
    s = s.replace("ñ", "ni")
    s = s.replace("Ñ", "NI")
    #Underscore replacement?
    s = s.replace('_',' ')
    #Non-breaking spaces problem
    s = s.replace('\u00A0', ' ')
    # Remove all special characters
    s = re.sub(r'[^\w\s]', '', s)
    # Remove accents
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    return s

@execute_func_if_not_none
def bbva_free_text_replace(s):
    # Replace ñ and Ñ with "ni"
    s = s.replace("ñ", "ni")
    s = s.replace("Ñ", "NI")
    #Non-breaking spaces problem
    s = s.replace('\u00A0', ' ')
    # Remove all special characters
    # s = re.sub(r'[^\w\s]', '', s)
    s = s.replace("\r\n", "\n").replace("\n", "")
    s = s.replace("\t", "")
    s = s.replace('|', "")
    s = s.replace('"', "")
    # s = s.replace("\n", "")
    # Remove accents
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    return s

@execute_func_if_not_none
def smart_text_replace(s):
    # Remove all special characters
    s = re.sub(r'[^\w\s]', '', s)
    # Replace multiple consecutive whitespace characters with a single space
    s = re.sub(r'\s+', ' ', s)
    s = s.lower()
    s = s.strip()
    # Remove accents
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    # Replace 'None', 'nan', and '' with None
    if s in ['None', 'nan', '', 'none']:  #Limited on purpose
        s = None
    return s

@execute_func_if_not_none
def free_text_replace(s: str) -> str:
    #Underscore replacement?
    s = s.replace('_',' ')
    s = s.replace('-',' ')
    # Remove all special characters
    s = re.sub(r'[^\w\s]', '', s)
    #Non-breaking spaces problem
    s = s.replace('\u00A0', ' ')
    # Replace multiple consecutive whitespace characters with a single space
    s = re.sub(r'\s+', ' ', s)
    s = s.strip()
    # Remove accents
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    # Replace 'None', 'nan', and '' with None
    if s in ['None', 'nan', '', 'none']:  #Limited on purpose
        s = None
    return s
