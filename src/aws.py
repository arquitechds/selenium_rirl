import os
import time
import pickle
from typing import List, Dict
from itertools import chain

import pandas as pd
import boto3
from loguru import logger


import signal
from typing import Callable

from functools import partial, wraps, update_wrapper


def timeout(seconds: int) -> Callable:
    def process_timeout(func: Callable) -> Callable:
        def handle_timeout(signum, frame):
            raise TimeoutError(f'The function {func.__name__} timed out.')

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, handle_timeout)
            signal.alarm(seconds)
            try:
                return func(*args, **kwargs)
            finally:
                signal.alarm(0) # No need to time out!

        return wrapper

    return process_timeout

def wrapped_partial(func, *args, **kwargs) -> Callable:
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func




import os
import json
import time
from typing import Any, Set, List
from datetime import datetime, timezone, timedelta
import time
import pytz

from dotenv import load_dotenv
import pandas as pd
import boto3
import awswrangler as wr
from loguru import logger
from pypdf import (
    PdfReader,
    PdfWriter,
)
from hashlib import sha1

import glob
import os
from typing import Set, List
import json

import pandas as pd
from loguru import logger


def read_all_json_in_directory(directory):
    files_to_read = get_files_from_directory(directory)
    list_of_dfs = []
    for i, x in enumerate(files_to_read):
        df = read_json_local(f'./{x}')
        list_of_dfs.append(df)
        if i % 10 == 0:
            logger.info(f'{i/len(files_to_read):.0%} files processed')
    return pd.DataFrame(list_of_dfs)

def read_all_csvs_in_directory(directory):
    files_to_read = get_files_from_directory(directory)
    files_with_issues = []
    list_of_dfs = []
    for i, x in enumerate(files_to_read):
        df = pd.read_csv(f'./{x}')
        na_check = (df.count(1) > df.shape[1]/2)
        if na_check.sum()/df.shape[0] < 1:
            if df.shape[0] == 0:
                continue
            df = df.loc[df.count(1) > df.shape[1]/2, :]
            df.columns = df.iloc[0,:]
            df = df.iloc[1:,:]
            df.reset_index(drop=True, inplace=True)
            df.to_csv(f'./{x}')
            files_with_issues.append(x)
        list_of_dfs.append(df)
        if i % 10 == 0:
            logger.info(f'{i/len(files_to_read):.0%} files processed')
    logger.warning(f'Files with issues are {files_with_issues}')
    return pd.concat(list_of_dfs)

def get_files_from_directory(filepath: str) -> Set:
    """Returns all files in a local directory

    Args:
        filepath (str): a local path

    Returns:
        [set]: set of files in a local path
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*'))
        files = filter(lambda x: '.' in x, files)
        for f in files:
            local_path = f.replace('\\', '/')
            all_files.append(local_path)
    return {*all_files}

def get_file_name_and_extension(url: str, check_assert: bool = True) -> List:
    """From a file or url get the file name and extension.

    Args:
        url (str): Any URL or file path.
        check_assert (bool, optional): Checks whether the result is two elements
            or not. Defaults to True.

    Returns:
        List: The list with the file name and extension.
    """
    file_name = os.path.basename(url)
    if check_assert:
        if len(file_name.split(".")) != 2:
            logger.warning(
                f"The file name {file_name} contains multiple periods.")
            aux = file_name.split(".")[0:len(file_name.split('.')) - 1]
            return '.'.join(aux), file_name.split(".")[-1]
    return file_name.split(".")

def get_files_from_directory_with_filter(filepath: str, filter_func) -> Set:
    """Returns all the type: str file types in a local directory

    Args:
        filepath (str): a local path
        filter_func (function): a function to filter files

    Returns:
        [set]: files in a directory that match the filter function
    """
    all_files = get_files_from_directory(filepath)
    return {*filter(filter_func, all_files)}

def read_json_local(file):
    # read file
    with open(file, 'r') as myfile:
        data=myfile.read()
        # parse file
    obj = json.loads(data)
    return obj



load_dotenv()

class S3Handler():
    def __init__(self, working_bucket:str = 'rirl-documents'):
        self.aws_access = os.getenv('AWS_ACCESS')
        self.aws_secret = os.getenv('AWS_SECRET')
        self.working_bucket = working_bucket
        self.boto_session = self.create_boto_session()
        self.boto_client = self.create_boto_client()
        self.boto_resource = self.create_boto_resource()

    def create_boto_client(self):
        return boto3.client('s3',
                            aws_access_key_id=self.aws_access,
                            aws_secret_access_key=self.aws_secret)
        
    def create_boto_session(self):
        return boto3.Session(region_name="us-east-2",
                            aws_access_key_id=self.aws_access,
                            aws_secret_access_key=self.aws_secret)

    def create_boto_resource(self):
        return boto3.resource('s3',
                              aws_access_key_id=self.aws_access,
                              aws_secret_access_key=self.aws_secret)

    def list_objects(self, s3_id: str, **kwargs):
        """get the list of keys in an s3_id

        Args:
            s3_id (str): The s3 resource to check

        Returns:
            [type]: returns the list of the s3_prefix
        """
        if 'last_modified_begin' in kwargs.keys():
            kwargs['last_modified_begin'] = create_datetime_from_str(
                kwargs['last_modified_begin']
                )
        if 'last_modified_end' in kwargs.keys():
            kwargs['last_modified_end'] = create_datetime_from_str(
                kwargs['last_modified_end']
                )

        return wr.s3.list_objects(f'{s3_id}', boto3_session=self.boto_session,
                                  **kwargs)

    def bucket_objects_without_s3_prefix(self, s3_id: str) -> Set:
        existing_objs_s3 = self.list_objects(s3_id)
        prefix_size = len(s3_id)
        return {obj[prefix_size:] for obj in existing_objs_s3}

    # def list_objects_from_last_changed_prefix()
    # Returns s3_prefix of the last file directory. Help Wanted.

    def does_object_exist(self, s3_id: str, **kwargs) -> bool:
        """Returns a bool if a file from s3_id exists

        Args:
            s3_id (str): the s3_id to check if exists

        Returns:
            bool: True if object exists and False if not
        """
        return wr.s3.does_object_exist(s3_id, boto3_session=self.boto_session,
                                       **kwargs)

    def get_s3_objects_in_working_bucket(self, prefix: str, suffix: str = None):
        s3 = self.create_boto_client()
        paginator = s3.get_paginator("list_objects_v2")
        kwargs = {'Bucket': self.working_bucket}
        # We can pass the prefix directly to the S3 API.  If the user has passed
        # a tuple or list of prefixes, we go through them one by one.
        if isinstance(prefix, str):
            prefixes = (prefix, )
        else:
            prefixes = prefix

        for key_prefix in prefixes:
            kwargs["Prefix"] = key_prefix

            for page in paginator.paginate(**kwargs):
                try:
                    contents = page["Contents"]
                except KeyError:
                    break
                for obj in contents:
                    key = obj["Key"]
                    if suffix and key.endswith(suffix):
                        yield obj
                    elif not suffix:
                        yield obj


    def get_objects_from_bucket(self, prefix: str, suffix: str = None):
        """
        Generate the objects in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        listed = []
        for obj in self.get_s3_objects_in_working_bucket(prefix, suffix):
            try:
                listed.append(obj)
            except AttributeError as e:
                pass
        return listed
    

    def wait_for_object_to_be_updated(self, s3_id: str, timeout: int = 5, threshold: int = 5

                                      ) -> bool:
        """
        Waits and returns true when the object has been updated, if it exceeds
        the timeout it returns false.
        
        Args:
        s3_id (str): the s3_id to check if exists
        timeout (int): the number of minutes to wait
        
        Returns:
        bool: True if object exists and False if not
        """
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=timeout)
        while datetime.now() < end_time:
            if self.object_has_been_updated(s3_id, threshold):

                return True
            else:
                time.sleep(5)
        return False
    
    def object_has_been_updated(self, s3_id: str, threshold: int = 10) -> bool:
        """
        Check if object has been updated recently, using a threshold of minutes.
        
        Args:
        s3_id (str): the s3_id to check if exists.
        threshold (int): minutes to check

        Returns:
        bool: True if object exists and False if not
        """
        if self.does_object_exist(s3_id=s3_id):
            #Credential rotator. Auth Error
            obj_description = wr.s3.describe_objects(
                s3_id, boto3_session=self.boto_session
                )
            last_modified = obj_description[s3_id]['LastModified']
            today = datetime.now(timezone.utc)
            delta = today - last_modified
            return delta < timedelta(minutes=threshold)
        else:
            return False
        

    def object_is_newer_than_threshold(self, s3_id: str, threshold: int
                                       ) -> bool:
        """Checks if an s3_urid is newer than a set number of days

        Args:
            s3_id (str): The s3_urid to check for last modified
            threshold (int): The days to check for how old

        Returns:
            bool: True if the object delta is longer than the amount of days we
                allow the file to be
        """
        if self.does_object_exist(s3_id=s3_id):
            obj_description = wr.s3.describe_objects(
                    s3_id, boto3_session=self.boto_session
                    )
            last_modified = obj_description[s3_id]['LastModified']
            today = datetime.now(timezone.utc)
            delta = today - last_modified
            return delta < timedelta(days=threshold)
        else:
            return False

    def get_last_key_modified_in_s3(self, s3_prefix: str, filter_function= None
                                    ) -> str:
        """Get the key of the last object modified in an s3_prefix

        Args:
            s3_prefix (str): The s3_prefix to search
            filter_function ([type], optional): The function to filter the keys
                of the s3_prefix. Defaults to None.

        Returns:
            str: the s3_urid of the last object changed
        """
        obj_descriptions = wr.s3.describe_objects(
            s3_prefix, boto3_session=self.boto_session, use_threads=False
            )
        if filter_function:
            obj_descriptions = {
                key: value for key, value in obj_descriptions.items()
                if filter_function(key)
                }
        last_object = max(obj_descriptions.items(),
                          key=lambda x: x[1]['LastModified'])
        return last_object[0]

    def read_csv(self, s3_id: str, **kwargs) -> pd.DataFrame:
        """Reads a csv file as a dataframe from S3. Accepts kwargs of
            pd.read_csv

        Args:
            s3_id (str): The s3_id where our .csv is saved on S3.

        Raises:
            Exception: If file does not exist

        Returns:
            pd.DataFrame: THe dataframe of the .csv
        """
        if self.does_object_exist(s3_id):
            return wr.s3.read_csv(s3_id,boto3_session= self.boto_session,
                                  **kwargs)
        else:
            raise Exception(f'File does not exist: {s3_id}')

    def read_last_csv_in_s3_prefix(self, s3_prefix: str, **kwargs
                                   ) -> pd.DataFrame:
        """Reads the last modified .csv in a s3_prefix

        Args:
            s3_prefix (str): the prefix where to search for the .csvs

        Returns:
            pd.DataFrame: [description]
        """
        get_last_obj_in_prefix = self.get_last_key_modified_in_s3(
            s3_prefix, lambda x: x.endswith('.csv')
            )
        if get_last_obj_in_prefix:
            logger.info(f"Last CSV IN PREFIX WAS {get_last_obj_in_prefix}")
            return self.read_csv(get_last_obj_in_prefix, **kwargs)

    def read_all_csvs_in_s3_prefix(self, s3_prefix: str, **kwargs
                                   ) -> pd.DataFrame:
        """Reads all .csvs in an s3_prefix into a single concatenated dataframe

        Args:
            s3_prefix (str): the s3_prefix where to read .csvs from

        Returns:
            pd.DataFrame: The dataframe of concatenated .csvs
        """
        objects_in_prefix = self.list_objects(s3_prefix)
        csv_objects = filter(lambda file: file.endswith('.csv'),
                             objects_in_prefix)
        dataframes = map(lambda file: self.read_csv(file, **kwargs),
                         csv_objects)
        return pd.concat(list(dataframes))

    def read_all_csvs_in_s3_prefix_that_match(self, s3_prefix: str, pattern: str,
                                              **kwargs) -> pd.DataFrame:
        """
        Reads all .csvs in an s3_prefix that match pattern
        into a single concatenated dataframe

        Args:
            s3_prefix (str): the s3_prefix where to read .csvs from
            pattern (str):  pattern used to filter .csvs

        Returns:
            pd.DataFrame: The dataframe of concatenated .csvs
        """
        objects_in_prefix = self.list_objects(s3_prefix)
        csv_objects = filter(lambda file: file.endswith('.csv'),
                             objects_in_prefix)
        matched_csvs = filter(lambda file: pattern in file,
                             csv_objects)
        dataframes = map(lambda file: self.read_csv(file, **kwargs),
                         matched_csvs)
        return pd.concat(list(dataframes))

    def write_csv(self, df: pd.DataFrame, s3_id: str, threshold: int= None,
                  overwrite: bool= False, **kwargs) -> None:
        """From a dataframe write a .csv into s3

        Args:
            df (pd.DataFrame): A dataframe
            s3_id (str): The s3_id where to write the df as .csv
            threshold (int, optional): Number of days we allow the file to be.
                Defaults to None.
            overwrite (bool, optional): If we should overwrite the file.
                Defaults to False.
        """
        if not overwrite and self.does_object_exist(s3_id):
            if (threshold
                and not self.object_is_newer_than_threshold(s3_id, threshold)):
                wr.s3.to_csv(df, s3_id, index=False,
                             boto3_session=self.boto_session,**kwargs)
            else:
                logger.info("File is newer then threshold. Not overwriting")
        else:
            wr.s3.to_csv(df, s3_id, index=False,
                         boto3_session=self.boto_session,**kwargs)
            logger.info(f"Uploaded df to {s3_id}")

    def read_excel(self, s3_id: str, **kwargs) -> pd.DataFrame:
        """Read an excel file from an S3_Id. Accepts same kwargs as
            pd.read_excel

        Args:
            s3_id (str): The id where we are reading the s3 .xls or .xlsx file.

        Raises:
            Exception: If file does not exist

        Returns:
            pd.DataFrame: the first table of an excel file
        """
        if self.does_object_exist(s3_id):
            return wr.s3.read_excel(s3_id,boto3_session= self.boto_session,
                                  **kwargs)
        else:
            raise Exception(f'File does not exist: {s3_id}')

    def read_last_excel_in_s3_prefix(self, s3_prefix: str, **kwargs
                                   ) -> pd.DataFrame:
        """Reads the last .xls or .xlsx file in a s3_prefix

        Args:
            s3_prefix (str): the prefix where we are going to read from

        Returns:
            pd.DataFrame: the dataframe read from excel
        """
        get_last_obj_in_prefix = self.get_last_key_modified_in_s3(
            s3_prefix, lambda x: x.endswith((".xls", ".xlsx"))
            )
        if get_last_obj_in_prefix:
            return self.read_excel(get_last_obj_in_prefix, **kwargs)

    def read_json(self, s3_id: str, **kwargs) -> pd.DataFrame:
        """Reads a json file from the S3_id and returns it as a pd.DataFrame

        Args:
            s3_id (str): the s3_id from we are going to read a json file

        Raises:
            Exception: If file does not exist

        Returns:
            pd.DataFrame: The dataframe from write_json
        """
        if self.does_object_exist(s3_id, **kwargs):
            return wr.s3.read_json(s3_id, boto3_session= self.boto_session,
                                   orient='table', **kwargs)
        else:
            logger.critical(f'File does not exist: {s3_id}')
            raise Exception(f'File does not exist: {s3_id}')
            

    def read_all_json_in_s3_prefix(self, s3_prefix: str, **kwargs
                                   ) -> pd.DataFrame:
        """Reads all .csvs in an s3_prefix into a single concatenated dataframe

        Args:
            s3_prefix (str): the s3_prefix where to read .csvs from

        Returns:
            pd.DataFrame: The dataframe of concatenated .csvs
        """
        objects_in_prefix = self.list_objects(s3_prefix)
        json_objects = filter(lambda file: file.endswith('.json'),
                              objects_in_prefix)
        dataframes = map(lambda file: self.read_json(file, **kwargs),
                         json_objects)
        return pd.concat(list(dataframes))

    def read_last_json_in_s3_prefix(self, s3_prefix: str, **kwargs
                                   ) -> pd.DataFrame:
        """Reads the last json file in an s3_prefix and returns a pd.Dataframe
            from the json file

        Args:
            s3_prefix (str): the s3_prefix where we are finding the last file

        Returns:
            pd.DataFrame: the dataframe that was read from a json file
        """
        get_last_obj_in_prefix = self.get_last_key_modified_in_s3(
            s3_prefix, lambda x: x.endswith(".json")
            )
        if get_last_obj_in_prefix:
            return self.read_json(get_last_obj_in_prefix,**kwargs)

    def write_json(self, df: pd.DataFrame, s3_id: str,
                   threshold: int= None, overwrite: bool= False,
                   **kwargs) -> None:
        """Takes a dataframe and writes it as a json file in the s3_id.

        Args:
            df (pd.DataFrame): Any data frame
            s3_id (str): The S3 URID where we will write to
            threshold (int, optional): How many days to overwrite.
                Defaults to None.
            overwrite (bool, optional): If we should overwrite the files
                or not. Defaults to False.
        """
        if not overwrite and self.does_object_exist(s3_id):
            if (threshold
                and not self.object_is_newer_than_threshold(s3_id, threshold)):
                wr.s3.to_json(df, s3_id, index=False, orient='table',
                              boto3_session=self.boto_session,**kwargs)
            else:
                logger.info("File is newer than threshold.")
        else:
            wr.s3.to_json(df, s3_id, index=False, orient='table',
                          boto3_session=self.boto_session,**kwargs)
            logger.info(f"Uploaded df to {s3_id}")

    def write_any_file_to_s3(self, s3_id: str, local_path: str, 
                             threshold: int= None, overwrite: bool= False, 
                             verify_integrity: bool = False, **kwargs) -> None:
        """From the local directory upload a file to an s3 with s3_id.

        Args:
            s3_id (str): The S3 location where we will write to
            local_path (str): The local path where we will be writing from
        """
        if not overwrite and self.does_object_exist(s3_id):
            if threshold and not self.object_is_newer_than_threshold(s3_id, 
                                                                 threshold):
                self.__upload_file__(s3_id, local_path, **kwargs)
            else:
                logger.info('File is newer than threshold.')
        else:
            self.__upload_file__(s3_id, local_path, **kwargs)
        if verify_integrity:
            self.__check_if_file_is_empty__(s3_id)
            
    def __upload_file__(self, s3_id: str, local_path: str, **kwargs) -> None:
        wr.s3.upload(local_path, s3_id, boto3_session=self.boto_session, 
                     **kwargs)
        if not self.does_object_exist(s3_id):
            raise Exception(f'File {local_path} has not been uploaded to {s3_id}.')
        logger.info(f'Uploaded {local_path} to {s3_id}')
        
    def __check_if_file_is_empty__(self, s3_id: str) -> None:
        logger.info('Checking File Integrity. Working bucket changed on Class.')
        self.working_bucket = s3_id.split("/")[2:3][0]
        file_details = self.get_objects_from_bucket(
            prefix=s3_id.split("/",3)[3:][0]
            )
        if not file_details[0]['Size'] > 30000:
            raise Exception('You have uploaded an apparently empty file')

    def download_any_file_from_s3(self, s3_id: str, local_path: str, **kwargs
                                  ) -> Any:
        """Download any file from S3

        Args:
            s3_id (str): the file to download from S3
            local_path (str): the path where it will be written into

        Returns:
            Any: Writes the local_path file into memory
        """
        wr.s3.download(s3_id, local_path,
                boto3_session=self.boto_session, **kwargs)
        logger.info(f"Downloaded {s3_id} to {local_path}")

    def download_latest_file_from_s3(self, s3_prefix: str, local_path: str,
                                     **kwargs) -> Any:
        """Downloads a the newest file from an S3 prefix into the local 
        directory

        Args:
            s3_prefix (str): the s3_prefix where we are looking
            local_path (str): the path where we will download the file to

        Raises:
            Exception: If file does not exist

        Returns:
            Any: Writes the local_path file into memory
        """
        #Consider adding filter_function= None
        get_last_obj_in_prefix = self.get_last_key_modified_in_s3(s3_prefix)
        if get_last_obj_in_prefix:
            wr.s3.download(get_last_obj_in_prefix, local_path,
                    boto3_session=self.boto_session, **kwargs)
            logger.info(f'Downloaded {get_last_obj_in_prefix} to {local_path}')
        else:
            logger.critical(f'Files do not exist in directory: {s3_prefix}')
            raise Exception(f'Files do not exist: {s3_prefix}')

    def read_excel_as_excel_file(self, s3_id: str, **kwargs) -> Any:
        file_name = get_file_name_and_extension(s3_id)
        self.download_any_file_from_s3(s3_id,f'{file_name[0]}.{file_name[1]}')
        df =  pd.ExcelFile(f'{file_name[0]}.{file_name[1]}')
        #This file is now being using by this function. We should consider
        # making it a handler.
        # os.remove(f"{file_name[0]}.{file_name[1]}") #Not allowed. But we can
        # del df and os.remove()
        return df

    def upload_directory(self, s3_id_prefix: str, local_directory: str,
                         threshold: int= None, overwrite: bool= False,
                         filter_func= None)-> None:
        """This function upload a local directory into an s3_id_prefix with the
            option to overwrite or set a last updated threshold.

        Args:
            s3_id_prefix (str): s3_prefix for local directory to be uploaded
            local_directory (str): local_directory that we are going to upload
            threshold (int, optional): Number of days that need to pass for
                overwrite to apply. Defaults to None.
            overwrite (bool, optional): If we should overwrite the files.
                Defaults to False.

        Raises:
            ValueError: Can't setup a days threshold if there is no overwrite
                option.
        """
        if not filter_func:
            filter_func = lambda path: path.endswith(('csv', 'xls', 'xlsx', 'xlsm'))
        files_in_directory = get_files_from_directory_with_filter(
            local_directory, filter_func
            )
        s3_id_prefix = fix_s3_id_prefix(s3_id_prefix)
        for file in files_in_directory:
            upload_name = upload_name_for_file(local_directory, file)
            self.write_any_file_to_s3(f'{s3_id_prefix}{upload_name}',
                                      file, threshold, overwrite)

    def read_json_from_tukan_s3(self, file_to_read):
        client = self.boto_client
        result = client.get_object(Bucket=self.working_bucket, Key=file_to_read)
        text = result['Body'].read().decode()
        return(json.loads(text))

    def upload_json_to_tukan_s3(self, file_name, json_data):
        resource = self.boto_resource
        s3object = resource.Object(self.working_bucket, file_name+'.json')

        s3object.put(
            Body=(bytes(json.dumps(json_data).encode('UTF-8')))
        )

    def delete_objects(self, path: str):
        wr.s3.delete_objects(path=path, boto3_session=self.boto_session)
        
    def read_dfs_from_file_list(self, files: List, format = 'csv',**kwargs
                                ) -> List[pd.DataFrame]:
        if format == 'csv':
            read_func = self.read_csv
        elif format == 'json':
            read_func = self.read_json
        return [read_func(file,**kwargs) for file in files]

    def copy_s3_files(self, files: List[str], target: str) -> None:
        """Copies files from one directory to another"""
        directory = files[0].rsplit('/', 1)[0]
        wr.s3.copy_objects(files, directory, target,
                           boto3_session=self.boto_session)
        
    def move_s3_files(self, files: List[str], target: str) -> None:
        self.copy_s3_files(files, target)
        for file in files:
            self.delete_objects(file)
        
    def read_json_as_json(self, s3_id: str) -> dict:
        """Reads a json file from the S3_id and returns it as a dict

        Args:
            s3_id (str): the s3_id from we are going to read a json file

        Raises:
            Exception: If file does not exist

        Returns:
            dict: The dataframe from write_json
        """
        if self.does_object_exist(s3_id):
            bucket_name = s3_id.split('//')[1].split('/')[0]
            key = '/'.join(s3_id.split('//')[1].split('/')[1:])
            client = self.boto_client
            result = client.get_object(Bucket=bucket_name, Key=key)
            text = result['Body'].read().decode()
            return json.loads(text)
        else:
            logger.critical(f'File does not exist: {s3_id}')
            raise Exception(f'File does not exist: {s3_id}')


    def split_pdf_from_s3_uri(self, s3_id: str, pages: List[int]):
        """
        input:
            -s3_id of the pdf
            -list of ints with the desired pages
        output:
            -s3_id of the new pdf that has the desired pages of the input id.
        """
        input_pdf = 'pypdf-input.pdf'
        self.download_any_file_from_s3(s3_id, input_pdf)

        reader = PdfReader(input_pdf)
        writer = PdfWriter()

        for page in pages:
            # reader.pages starts counting the pages from the page 0
            writer.add_page(reader.pages[page-1])

        # Hash of all requested pages
        pages_string = '-'.join([str(x) for x in pages])
        page_hash = sha1(pages_string.encode()).hexdigest()[:10]
        s3_id_new_file = s3_id.rsplit('/', 1)[0] + f'/{page_hash}_' + s3_id.rsplit('/', 1)[1]
        output_pdf = 'pypdf-output.pdf'

        with open(output_pdf, 'wb') as fp:
            writer.write(fp)

        self.write_any_file_to_s3(s3_id_new_file, output_pdf, threshold=0, overwrite=True)

        os.remove(input_pdf)
        os.remove(output_pdf)
        return s3_id_new_file
    
    def get_size_from_object(self, s3_uri):
        if self.does_object_exist(s3_uri):
            bucket = s3_uri.split('/')[2:3][0]
            prefix = '/'.join(s3_uri.split('/')[3:])
            logger.info(f'Changing S3Handler working bucket to {bucket}')
            self.working_bucket = bucket
            info = self.get_objects_from_bucket(prefix)[0]
            kilo_bytes = info['Size']/1024
        else:
            raise Exception(f'Object {s3_uri} does not exist!')
        return kilo_bytes
        
    def list_all_objects_versions_from_prefix(self, s3_uri_prefix: str):
        bucket_name = s3_uri_prefix.split('/')[2:3][0]
        prefix = '/'.join(s3_uri_prefix.split('/')[3:])
        paginator = self.boto_client.get_paginator('list_object_versions')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        all_objects = []
        for page in page_iterator:
            if 'Versions' in page:
                for version in page['Versions']:
                    all_objects.append({
                        'Key': version['Key'],
                        'VersionId': version['VersionId'],
                        'IsLatest': version['IsLatest'],
                        'LastModified': version['LastModified'],
                        'Size': version['Size'],
                        'StorageClass': version['StorageClass']
                    })
            if 'DeleteMarkers' in page:
                for delete_marker in page['DeleteMarkers']:
                    all_objects.append({
                        'Key': delete_marker['Key'],
                        'VersionId': delete_marker['VersionId'],
                        'IsLatest': delete_marker['IsLatest'],
                        'LastModified': delete_marker['LastModified'],
                        'IsDeleteMarker': True
                    })
        return all_objects
    

    def restore_objects_with_delete_markers_from_s3_uri(self, s3_uri_prefix: str, delete_date: datetime = datetime.now(timezone.utc)):
        bucket_name = s3_uri_prefix.split('/')[2:3][0]
        prefix = '/'.join(s3_uri_prefix.split('/')[3:])
        paginator = self.boto_client.get_paginator('list_object_versions')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        delete_markers_today = []
        
        # Identify delete markers created today
        for page in page_iterator:
            if 'DeleteMarkers' in page:
                for delete_marker in page['DeleteMarkers']:
                    if delete_marker['LastModified'].date() == delete_date.date():
                        delete_markers_today.append({
                            'Key': delete_marker['Key'],
                            'VersionId': delete_marker['VersionId']
                        })

        # Remove delete markers by copying the object version before the delete marker back to the original key
        for delete_marker in delete_markers_today:
            versions = self.boto_client.list_object_versions(Bucket=bucket_name, Prefix=delete_marker['Key'])
            
            # Find the latest version before the delete marker
            latest_version = None
            for version in versions.get('Versions', []):
                if version['Key'] == delete_marker['Key'] and version['VersionId'] != delete_marker['VersionId']:
                    if latest_version is None or version['LastModified'] > latest_version['LastModified']:
                        latest_version = version
            
            if latest_version:
                self.boto_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': delete_marker['Key'], 'VersionId': latest_version['VersionId']},
                    Key=delete_marker['Key']
                )
                logger.info(f"Restored {delete_marker['Key']} to version {latest_version['VersionId']}")
            else:
                logger.info(f"No previous version found for {delete_marker['Key']}")
                
    def should_read_object(self, s3_uuid: str, kilo_byte_size: int = 1024):
        if not self.does_object_exist(s3_uuid):
            read = False
        else:
            if self.get_size_from_object(s3_uuid) > 1024*kilo_byte_size:
                read = False
            else:
                read = True
        return read

def create_datetime_from_str(date_str: str) -> datetime:
    return pytz.utc.localize(
        datetime.strptime(date_str,'%Y-%m-%d %H:%M')
        )

def upload_name_for_file(directory: str, file_name: str) -> str:
    if directory.endswith('/'):
        num_chars = len(directory) 
    else:
        num_chars = len(directory) + 1
    return file_name[num_chars:]

def fix_s3_id_prefix(s3_id_prefix: str) -> str:
    if s3_id_prefix.endswith('/'):
        return s3_id_prefix
    else:
        return s3_id_prefix + '/'
class TextractHandler(S3Handler):

    def __init__(self):
        super().__init__()
        self.create_boto_client()

    def create_boto_client(self):
        self.boto_client = boto3.client(
            'textract',
            aws_access_key_id=self.aws_access,
            aws_secret_access_key=self.aws_secret,
            region_name='us-east-2'
        )

    @timeout(600)
    def complete_textract(self, s3_uri: str, overwrite: bool = False) -> str:
        """
        Extracts raw text from a document using Amazon Textract Get Document Text Detection API.
        
        :param s3_uri: The S3 URI of the document to analyze.
        :param overwrite: Whether to overwrite any cached results.
        :return: A string containing the extracted raw text.
        """
        pickle_id = s3_uri.rsplit('.')[0] + '_raw_text.pickle'

        if self.does_object_exist(pickle_id) and not overwrite:
            return self.get_pickle(pickle_id)
        else:
            raw_text = self.run_text_detection_job(s3_uri)
            self.save_raw_text_to_pickle(raw_text, pickle_id)
            return raw_text

    def get_pickle(self, s3_uri: str) -> str:
        """
        Retrieves cached raw text from a pickle file.
        
        :param s3_uri: The S3 URI of the cached pickle file.
        :return: The cached raw text as a string.
        """
        s3_folder = s3_uri.rsplit('/', 1)[0]
        s3_id_pickle = s3_uri.rsplit('.')[0] +'.pickle'
        pickle_filename = s3_id_pickle.rsplit('/', 1)[1]

        if s3_id_pickle in self.list_objects(s3_folder):
            self.download_any_file_from_s3(s3_id_pickle, pickle_filename)

            with open(pickle_filename, 'rb') as f:
                raw_text = pickle.load(f)
            #os.remove(pickle_filename)
            return raw_text
        return ''

    def run_text_detection_job(self, s3_uri: str) -> str:
        """
        Runs the Get Document Text Detection API to extract text.
        
        :param s3_uri: The S3 URI of the document to analyze.
        :return: Extracted raw text as a string.
        """
        bucket_name = s3_uri.split('//')[1].split('/')[0]
        document_file_name = s3_uri.split('//')[1].split('/', 1)[1]

        try:
            response = self.boto_client.start_document_text_detection(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': document_file_name
                    }
                }
            )
            job_id = response['JobId']
            return self.wait_for_text_detection_job(job_id)
        except Exception as e:
            logger.exception(f"Couldn't detect text in {document_file_name}: {e}")
            raise

    def wait_for_text_detection_job(self, job_id: str) -> str:
        """
        Waits for the text detection job to complete and retrieves the results.
        
        :param job_id: The ID of the text detection job.
        :return: Extracted raw text as a string.
        """
        state = 'IN_PROGRESS'
        text_lines = []
        next_token = None

        # Poll until the job is complete
        while state == 'IN_PROGRESS':
            time.sleep(7.5)
            if next_token:
                response = self.boto_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
            else:
                response = self.boto_client.get_document_text_detection(JobId=job_id)

            state = response['JobStatus']

            if state == 'SUCCEEDED':
                # Extract text lines from the current page
                text_lines.extend(
                    block['Text'] for block in response.get('Blocks', []) if block['BlockType'] == 'LINE'
                )
                # Check for more pages
                next_token = response.get('NextToken', None)
                while next_token:
                    # Fetch the next set of results
                    response = self.boto_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
                    text_lines.extend(
                        block['Text'] for block in response.get('Blocks', []) if block['BlockType'] == 'LINE'
                    )
                    next_token = response.get('NextToken', None)

            if state == 'FAILED':
                raise Exception(f"Textract text detection job {job_id} failed.")

        return ' '.join(text_lines)


    def extract_text_from_blocks(self, response: dict) -> str:
        """
        Extracts text from the response blocks returned by Textract.
        
        :param response: The response from Textract containing blocks of text.
        :return: Extracted text as a string.
        """
        blocks = response.get('Blocks', [])
        text_lines = [block['Text'] for block in blocks if block['BlockType'] == 'LINE']
        return ' '.join(text_lines)

    def save_raw_text_to_pickle(self, raw_text: str, pickle_id: str) -> None:
        """
        Saves extracted raw text to a pickle file and uploads it to S3.
        
        :param raw_text: The raw text to save.
        :param pickle_id: The S3 ID for the pickle file.
        """
        pickle_filename = pickle_id.rsplit('/', 1)[1]
        with open(pickle_filename, 'wb') as f:
            pickle.dump(raw_text, f)

        self.write_any_file_to_s3(pickle_id, pickle_filename, overwrite=True)
        os.remove(pickle_filename)


def map_blocks(blocks, block_type):
    return {
        block['Id']: block
        for block in blocks if block['BlockType'] == block_type
    }

def get_children_ids(block):
    for rels in block.get('Relationships', []):
        if rels['Type'] == 'CHILD':
            yield from rels['Ids']
