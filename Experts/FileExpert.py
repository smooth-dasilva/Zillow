import datetime
import os
import logging
import os.path, time 
import re

from collections import deque

# use Expert to :
# Assign responsibility to class that has information required to fulfill responsibility.

class FileExpertClass:

    NameDeq : dict

    app_logger : logging

    pattern : str
    path :str


    def __init__(self,_app_logger, pattern, path) -> None:
        
        self.app_logger = _app_logger

        self.pattern =pattern
        self.path = path
        
        self.NameDeq = self.AddFileNamesDeq(self.CheckPathValidity())
    
    #Get List of all files in specified folder with specified pattern
    def AddFileNamesDeq(self, files):
        if files:
            NameDeqTemp = deque()
            for el in files:
                if (self.MatchFilenameToPattern(el)):
                    NameDeqTemp.append(el)
            return NameDeqTemp

    #match filename with specified patter when creating File Expert
    def MatchFilenameToPattern(self,filename):

 
        match = re.match(self.pattern, filename)
        if match:
            return True
        return False

    #check path validity
    def CheckPathValidity(self):
        try:
            return os.listdir(self.path)
        except FileNotFoundError:
            self.app_logger.error(f"Input error caught from {__name__}. Valid path not given. \nTerminating program...")
            self.path = "ERROR"
            return ""      