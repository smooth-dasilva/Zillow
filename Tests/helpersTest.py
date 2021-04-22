import pandas as pd
import config
import unittest     
import Utilities.helpers as hlper
import unittest     
from Utilities.logger import setup_logger

from os import path
from typing import Deque
from Experts.FileExpert import FileExpertClass



app_logger = setup_logger('app_logger', './app.log')


FileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv',  config.staticTestLocation)
NoPatterFileExpert = FileExpertClass(app_logger, r'badpattern',  config.dataLocation)
InvalidDirectoryFileExpert = FileExpertClass(app_logger, r'[A-Za-z0-9]+_time_series.csv', "")

class FileExpertTestCase(unittest.TestCase):

    def test_FileExpertFindsFilesWithPatternWithConstructor(self):
        self.assertEqual(FileExpert.NameDeq[0]+FileExpert.NameDeq[1], "Another_time_series.csvA_time_series.csv") 
    def test_FileExpertIgnoresFilesWithoutPatternWithConstructor(self):
        self.assertEqual(NoPatterFileExpert.NameDeq, Deque([])) 

    def test_FileExpertFindsInputtedDirectory(self):
        self.assertEqual(FileExpert.CheckPathValidity(), ["Another_time_series.csv", "A_time_series.csv"])
    def test_FileExpertDoesntFindWrongInputtedDirectory(self):
        self.assertEqual(InvalidDirectoryFileExpert.CheckPathValidity(), None )
    
    
    def test_FileExpertMatchesRegexPattern(self):
        self.assertTrue(FileExpert.MatchFilenameToPattern("Metro_time_series.csv"), True)
    def test_FileExpertDoesntMatchWrongRegexPattern(self):
        self.assertFalse(FileExpert.MatchFilenameToPattern("Metro_time_series"), False)
    
    def test_FileExpertsAddNamesFuncAddsNamesToNameDeqIfMathchesRegex(self):
        self.assertEqual(FileExpert.AddFileNamesDeq(["New1", "New2", "Metro_time_series.csv"]), Deque(["Metro_time_series.csv"]))
    
    def test_FileExpertsReplacesPathToErrorGivenBadPath(self):
        self.assertEqual(InvalidDirectoryFileExpert.path, "ERROR")



d = {'Population' : ['37000000','54000000'], 'Record Date' : ['2003-07-15','23-07-1985'], 'RegionName' : ['Texas','California']}
test_df = pd.DataFrame(data = d)
convert_df = hlper.convert_col_types(test_df)

class HelpersTestCase(unittest.TestCase):

    def test_AbbreviateLongNamesChangesDateColName(self):
        self.assertEqual(hlper.abbreviateLongNames("Date"), "Date_MDY") 
    
    def test_AbbreviateLongNamesReturnsShortenedStringOfFirstIndexWhenDelimittedByUnderScore(self):
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted"), "InvSeasAdj") 
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted_Else"), "InvSeasAdj_Else") 
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted_Else_AnythingElse"), "InvSeasAdj_Else_AnythingElse")
    
    def test_AbbreviateLongNamesDoesntReturnShortenedStringIfNotFormattedProperly(self):
        self.assertEqual(hlper.abbreviateLongNames("inventoryseasonallyadjusted_AnythingElse"), "inventoryseasonallyadjusted_AnythingElse")

    def test_GetTypeReturnsFloatGivenFloatType(self):
        self.assertEqual(hlper.get_type("999"), "float64")
    
    def test_GetTypeReturnsDateGivenDateType(self):
        self.assertEqual(hlper.get_type("1999-01-27"), "datetime64[ns]")
    
    def test_GetTypeReturnsStringGivenStringType(self):
        self.assertEqual(hlper.get_type("California"), "string")
    
    def test_ConvertColTypesCorrectlyConvertsColumns(self):
        self.assertEqual(convert_df.dtypes['Population'], "float64")
        self.assertEqual(convert_df.dtypes['Record Date'], "datetime64[ns]")
        self.assertEqual(convert_df.dtypes['RegionName'], "string")     

if __name__ =="__main__":
    unittest.main()