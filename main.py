import pandas as pd
import re

from Utilities.logger import setup_logger
from Utilities.quickstart import SendMessage
from Utilities.mysql_conn import mysql_conn_class
from Experts.FileExpert import FileExpertClass


import time

#start timer
start = time.time()



# set up loggers
app_logger = setup_logger('app_logger', './app.log')

# for log session separator
logSeparator = '++++++++++++++++++++++++++++'

# On app load : create logs
app_logger.info(f"Begin app log\n{logSeparator}")







def main():

    DatabaseExpert = mysql_conn_class(app_logger)
    #db_names_in_mysql = DatabaseExpert.get_database_names()

    #we use a file expert to get all the right dataset names into a list that we access (fileExpert.NameDeq)
    fileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv', './datasets/')
    
    #we set FE's path to be ERROR if Check_Validity doesnt get a valid path
    if fileExpert.path=="ERROR":
        return

    if not fileExpert.NameDeq:
        return app_logger.error(f"No files with regex pattern {fileExpert.pattern} match in the given {fileExpert.path}. \nTerminating the program...")

    """
    We have: 
    Validation for directory and filename
    List of all dataset path +  file names  (and so access to pandas df or spark df)
    Logger that can record anything anywhere
    Access to mysql database. though we dont have any tables thats nothing but a sql query away
    Able to tar and archive (not implemented yet)
    """
##### TODO ###########################
    for name in fileExpert.NameDeq:
        # here we will process the each files in a different loop
        #for now process 1 specified


        if name =="State_time_series.csv":
            df = pd.read_csv(fileExpert.path+name)
            col_types =df.infer_objects().dtypes
            for name, type in col_types.items():
                print(name, type)


    #after the above loop, ideally we'll have all the files processed and added to the database. 
    #this may need to altered depending on that complexity



if __name__ =="__main__":
    main()


# logger ending
app_logger.info(f"End app log\n{logSeparator}")

end = time.time()
print(end - start)