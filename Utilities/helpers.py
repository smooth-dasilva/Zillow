import os
import re
import shutil
import tarfile
import logging
import pandas as pd
import numpy as np

import config
import dateutil.parser


from pyspark import rdd
from pyspark import SparkContext

def replace_nulls_with(data, replacewith, logger=logging):
        '''
        Replaces all nulls in pandas dataframe or spark rdd 
                and returns output in matching format.
        Parameters:
                data (dataframe | sparkRDD): the data to scan
                replacewith (str): the value to replace nulls
                logger (optional): the logger to forward logs
        Returns:
                output (dataframe | sparkRDD): the input data with all nulls replaced, or the same data
        '''
        output = data

        if isinstance(data, rdd.RDD) or isinstance(data, rdd.PipelinedRDD):
                try:
                        output = data.map(lambda line: 
                                        tuple(map(lambda field: (re.match(r'^[ ]*$', str(field)) != None)*(replacewith) or field, line.split(','))))
                except Exception as e:
                        logger.warning(f'Error replacing nulls with value {replacewith} : {e}')

        elif isinstance(data, pd.core.frame.DataFrame):
                try:
                        output = data.replace(to_replace = np.nan, value=replacewith)

                except Exception as e:
                        logger.warning(f'Error replacing nulls with value {replacewith} : {e}')

        return output

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

        if(not(os.path.exists(source))):
                return
        
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
                nothing
        '''

        size = 0
        
        try:
                size = os.stat(source).st_size
        except Exception as e:
                logger.info(f'Could not find file size of source {source}: {e}')
        
        return (size == 0)

def get_type(val):
    try:
        float(val)
    except:
        try:
            dateutil.parser.parse(val)
        except:
            return "string"
        else:
            return "datetime64[ns]"
    else:
        return "float64"
        
def convert_col_types(df):
    col_types = {}
    for col in df.columns:
        for cell in df[col]:
            try:
                col_type = get_type(cell)
            except:
                pass
            else:
                col_types[col] = col_type
                break
    col_types["RegionName"] = "string"
    return df.astype(col_types, copy = False)


def abbreviateLongNames(colname):
    if colname.lower().split("_")[0] == 'inventorytiershare':
        colname = colname.split("_")[0]+'_'+colname.split("_")[1]
    if colname.lower() == 'date':
        return 'Date_Column'
    header_map = config.header_map
    colname_split = colname.split('_')
    for index, delim in enumerate(colname_split):
        if delim in header_map.keys() and len(colname) > 30:
                colname_split[index] = header_map[colname_split[index]]
    return '_'.join(colname_split)
