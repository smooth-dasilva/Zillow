import os
import logging
import os.path
import re



class FileExpertClass:
    NameList : list

    app_logger : logging

    pattern : str
    path :str


    def __init__(self,_app_logger, pattern, path) -> None:
        
        self.app_logger = _app_logger

        self.pattern =pattern
        self.path = path
        
        self.NameList = self.AddFileNamesList(self.CheckPathValidity())
    
    def AddFileNamesList(self, files):
        if files:
            NameListTemp = []
            for el in files:
                if (self.MatchFilenameToPattern(el)):
                    NameListTemp.append(el)
            return NameListTemp

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