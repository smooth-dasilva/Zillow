import os
import logging
import os.path
import re

from collections import deque

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
    
    def AddFileNamesDeq(self, files):
        if files:
            NameDeqTemp = deque()
            for el in files:
                if (self.MatchFilenameToPattern(el)):
                    NameDeqTemp.append(el)
            return NameDeqTemp

    def MatchFilenameToPattern(self,filename):
        match = re.match(self.pattern, filename)
        if match:
            return True
        return False

    def CheckPathValidity(self):
        try:
            return os.listdir(self.path)
        except FileNotFoundError:
            self.path = "ERROR"