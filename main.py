import pandas as pd
import config


from Utilities.cxorcl_conn import orcl_conn_class
from Utilities.logger import setup_logger
from quickstart import SendMessage
from Experts.FileExpert import FileExpertClass
from Utilities.helpers import *
from datetime import date

today = date.today().strftime("%b-%d-%Y")

#list of regex to search for
regex_list = [r'[A-Za-z0-9]+_(time_series|crosswalk).csv'] 


os.chdir(config.source)

app_logger = setup_logger('app_logger', './app.log')
logSeparator = '\n++++++++++++++++++++++++++++'
app_logger.info(f"\nBegin app log\n{logSeparator}")

old_new_names_map = {}

def main():
    orcl_conn = orcl_conn_class(app_logger, config.orcl_user, config.orcl_pwd , config.orcl_host,config.orcl_service, config.orcl_port)
    
    for pattern in regex_list:
        
        filesFinder = FileExpertClass(app_logger, pattern, config.dataLocation)
        if filesFinder.path=="ERROR":
            return app_logger.error(f"\nValid path not given: {config.dataLocation}. \nTerminating program...")
       
        if not filesFinder.NameDeq:
            app_logger.error(f"\nNo files with regex pattern {filesFinder.pattern} match in the given {filesFinder.path}. \nMoving on to next pattern; if none, terminating...")
        else:
            for name in filesFinder.NameDeq: 
                nameNoExtension = name[:-4]
                fullpath=filesFinder.path+name

                if is_file_empty(fullpath):
                    app_logger.error(f"\nFound empty file at {fullpath}")
                else: 
                    app_logger.info(f"\nProcessing file: {name}\nLoading file into pandas dataframe...")
                    df = pd.read_csv(fullpath)
                
                    app_logger.info(f"\nRenaming columns")
                    for colname in df.columns:
                        old_new_names_map[colname] = abbreviateLongNames(colname)
                    df = df.rename(columns=old_new_names_map)

                    app_logger.info(f"\nCreating oracle RDS table")
                    orcl_conn.create_table(df.columns, name)
                    
                    app_logger.info(f"\nArchiving file...") 
                    archive_file(config.dataLocation+name, config.archiveLocation+nameNoExtension+'_' +today + '.tar.gz')


if __name__ =="__main__":
    main()


app_logger.info(f"\nEnd app log\n{logSeparator}")
SendMessage(config.sender, config.to, config.subject, config.msgHtml, config.msgPlain, config.appLogLocation)