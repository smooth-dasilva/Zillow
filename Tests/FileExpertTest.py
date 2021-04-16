from os import path
from typing import Deque
from Experts.FileExpert import FileExpertClass
import config
import unittest     

from Utilities.logger import setup_logger


app_logger = setup_logger('app_logger', './app.log')

FileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv',  config.dataLocation)
NoPatterFileExpert = FileExpertClass(app_logger, r'badpattern',  config.dataLocation)
InvalidDirectoryFileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv', "")

class FileExpertTestCase(unittest.TestCase):

    def test_FileExpertFindsFilesWithPatternWithConstructor(self):
        self.assertEqual(FileExpert.NameDeq[0]+FileExpert.NameDeq[1], "Metro_time_series.csvState_time_series.csv") 

    def test_FileExpertFindsInputtedDirectory(self):
        self.assertEqual(FileExpert.CheckPathValidity(), ["archives","Metro_time_series.csv", "State_time_series.csv"])
    
    def test_FileExpertMatchesRegexPattern(self):
        self.assertTrue(FileExpert.MatchFilenameToPattern("Metro_time_series.csv"), True)

    def test_FileExpertDoesntMatchWrongRegexPattern(self):
        self.assertFalse(FileExpert.MatchFilenameToPattern("Metro_time_series"), False)

    def test_FileExpertsAddNamesFuncAddsNamesToNameDeqIfMathchesRegex(self):
        self.assertEqual(FileExpert.AddFileNamesDeq(["New1", "New2", "Metro_time_series.csv"]), Deque(["Metro_time_series.csv"]))

    def test_FileExpertIgnoresFilesWithoutPatternWithConstructor(self):
        self.assertEqual(NoPatterFileExpert.NameDeq, Deque([])) 

    def test_FileExpertDoesntFindWrongInputtedDirectory(self):
        self.assertEqual(InvalidDirectoryFileExpert.CheckPathValidity(), None )
        
    def test_FileExpertsReplacesPathToErrorGivenBadPath(self):
        self.assertEqual(InvalidDirectoryFileExpert.path, "ERROR")

    

if __name__ == '__main__':
    unittest.main()