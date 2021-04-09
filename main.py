import pandas as pd
import time
import pandas as pd


from Utilities.logger import setup_logger
from quickstart import SendMessage
from Experts.FileExpert import FileExpertClass
from Utilities.helpers import *

#list of regex to search for
regex_list = [r'[A-Za-z0-9]+_time_series.csv'] 

#Send email of app log to self vars
to = "luan.dasilva@smoothstack.com"
sender = "luan.dasilva@smootstack.com"
subject = "Automated Log File Email"
msgHtml = "Log file from app run"
msgPlain = "Log file from app run"

os.chdir("C:/Users/luanf/OneDrive/Desktop/ZillowMAIN/ZillowEnv")
start = time.time()

app_logger = setup_logger('app_logger', './app.log')
logSeparator = '\n++++++++++++++++++++++++++++'
app_logger.info(f"\nBegin app log\n{logSeparator}")

emptyFileList = [] 
old_new_names_map = {}
def main():
    for pattern in regex_list:
        filesFinder = FileExpertClass(app_logger, pattern, './datasets/')
        if filesFinder.path=="ERROR":
            return app_logger.error(f"\nValid path not given. \nTerminating program...")
        if not filesFinder.NameDeq:
            return app_logger.error(f"\nNo files with regex pattern {filesFinder.pattern} match in the given {filesFinder.path}. \nTerminating the program...")

        for name in filesFinder.NameDeq:
            fullpath=filesFinder.path+name
            if is_file_empty(fullpath):
                app_logger.error(f"\nFound empty file at {fullpath}")
            else: 
                tname = name[:-4]
                app_logger.info(f"\nProcessing file: {name}\nLoading file into pandas dataframe...")
                df = pd.read_csv(fullpath)
                for colname in df.columns:
                    if '_' in colname:
                        old_new_names_map[colname] = abbreviateLongNames(colname)
                df.rename(columns=old_new_names_map)

                app_logger.info(f"\nConverting dtypes...")
                df = convert_col_types(df)
                app_logger.info(f"\nReplacing nulls with string 'NAN'")
                df = replace_nulls_with(data=df, replacewith="NAN", logger=app_logger)
                app_logger.info(f"\nArchiving file...")
                DataframeArchive(filesFinder.path+name,filesFinder.path+'./archive/'+tname+'tar.gz', app_logger)
    SendMessage(sender, to, subject, msgHtml, msgPlain, './app.log')

if __name__ =="__main__":
    main()


# logger ending
app_logger.info(f"\nEnd app log\n{logSeparator}")

end = time.time()
print(end - start)