import os
import math
import logging
import contextlib
import mysql.connector

from Utilities.logger import setup_logger
from Utilities.quickstart import SendMessage
from Utilities.mysql_conn import get_mysql_conn, get_mysql_dbs




# set up loggers
app_logger = setup_logger('app_logger', './app.log')

# for log session separator
logSeparator = '++++++++++++++++++++++++++++'

# On app load : create logs
app_logger.info(f"Begin app log\n{logSeparator}")



def main():   
    get_mysql_dbs(app_logger)
if __name__ =="__main__":
    main()


# logger ending
app_logger.info(f"End app log\n{logSeparator}")