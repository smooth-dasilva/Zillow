import os
import re
import shutil
import tarfile
import logging
import pandas as pd

import config
import dateutil.parser

def archive_file(source, destination, logger=logging):
        '''
        Archives a file as .tar from a source to a destination
        Parameters:
                source (str): the path and name of file (e.g. /temp/source.csv)
                destination (str): the path and name of file (e.g. /temp/dest.tar)
                logger (optional): the logger to forward logs
        Returns:
                nothing
        '''

        archive = None

        if(not(os.path.exists(source))):
                return

        try:
                os.remove(destination)
        except Exception as e:
                logger.info(f'No file at destination {destination}: {e}')

        try:
                archive = tarfile.open(destination, 'w|gz')
        except Exception as e:
                logger.error(f'Could not create archive: {e}')

        try:
                archive.add(source)
        except Exception as e:
                logger.error(f'Could not archive source {source}: {e}')
        
        try:
                archive.close()
        except Exception as e:
                logger.error(f'Could not close archive: {e}')

def copy_file(source, destination, logger=logging):
        '''
        Copies a file from a source to a destination
        Parameters:
                source (str): the path and name of file (e.g. /temp/source.csv)
                destination (str): the path and name of file (e.g. /temp/dest.tar)
                logger (optional): the logger to forward logs
        Returns:
                nothing
        '''

        try:
                shutil.copy2(source, destination)
        except Exception as e:
                logger.error(f'Could not copy source {source} to destination {destination} : {e}')
        

def is_file_empty(source, logger=logging):
        '''
        Checks if the file at the source location is empty
        Parameters:
                source (str): the path and name of file (e.g. /temp/source.csv)
                logger (optional): the logger to forward logs
        Returns:
                Bool: True (empty file or nonexistent file), False (file size not zero)
        '''

        size = 0
        
        try:
                size = os.stat(source).st_size
        except Exception as e:
                logger.info(f'Could not find file size of source {source}: {e}')
        
        return (size == 0)
     
def abbreviateLongNames(colname):
    if colname.lower() == 'date':
        return 'Date_Column'
    header_map = config.header_map
    colname_split = colname.split('_')
    for index, delim in enumerate(colname_split):
        if delim in header_map.keys() and len(colname) > 30:
                colname_split[index] = header_map[colname_split[index]]
    return '_'.join(colname_split)
