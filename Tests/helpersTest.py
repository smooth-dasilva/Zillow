import pandas as pd
import config     
import unittest
import Utilities.helpers as hlper     

from Utilities.logger import setup_logger
from os import path, remove, stat
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

class HelpersTestCase(unittest.TestCase):

    def test_AbbreviateLongNamesChangesDateColName(self):
        self.assertEqual(hlper.abbreviateLongNames("Date"), "Date_Column") 
    
    def test_AbbreviateLongNamesReturnsShortenedStringOfWhenDelimittedByUnderScore(self):
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted_Else"), "InvSeasAdj_Else") 
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted_MedianListingPricePerSqft_AnythingElse"), "InvSeasAdj_MedLstPrPerSqft_AnythingElse")
    
    def test_AbbreviateLongNamesDoesntReturnsShortenedStringIfNotLongEnough(self):
        self.assertNotEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted"), "InvSeasAdj") 

    def test_AbbreviateLongNamesDoesntReturnShortenedStringIfNotFormattedProperly(self):
        self.assertEqual(hlper.abbreviateLongNames("inventoryseasonallyadjusted_AnythingElse"), "inventoryseasonallyadjusted_AnythingElse")

class HelpersArchiveCopyCheckAndNullFileTestCase(unittest.TestCase):
    def setUp(self):
        self.df1 = pd.DataFrame({'a':[1,3,pd.NA], 'b':[2,4,pd.NA]})
        self.df2 = pd.DataFrame({'a':[1,3,'NAN'], 'b':[2,4,'NAN']})
        self.df3 = pd.DataFrame({'a':[1,3,5], 'b':[2,4,6]})
        self.df4 = pd.DataFrame({'a':[1,3,5], 'b':[2,4,6]})

    def tearDown(self):
        try:
            remove(config.fileTestLocation + 'archive.tar')
        except Exception as e:
            app_logger.debug(f'Could not delete archive in testing {e}')
        
        try:
            remove(config.fileTestLocation + 'copy-destination.csv')
        except Exception as e:
            app_logger.debug(f'Could not delete copy in testing {e}')

    def test_ArchiveFileCreatesArchive(self):
        hlper.archive_file(config.fileTestLocation + 'archive-source.csv', config.fileTestLocation + 'archive.tar', logger=app_logger)
        self.assertEqual(path.exists(config.fileTestLocation + 'archive.tar'), True)

    def test_ArchiveFileFailsArchiveCreation(self):
        hlper.archive_file(config.fileTestLocation + 'archive-source-fake.csv', config.fileTestLocation + 'archive.tar', logger=app_logger)
        self.assertEqual(path.exists(config.fileTestLocation + 'archive.tar'), False)

    def test_ArchiveFileFailsLoadAddClose(self):
        hlper.archive_file(config.fileTestLocation + 'archive-source.csv', config.fileTestLocation + 'fake-directory/archive.tar', logger=app_logger)
        self.assertEqual(path.exists(config.fileTestLocation + 'fake-directory/archive.tar'), False)

    def test_CopyFileCopiesFile(self):
        hlper.copy_file(config.fileTestLocation + 'copy-source.csv', config.fileTestLocation + 'copy-destination.csv', logger=app_logger)
        self.assertEqual(path.exists(config.fileTestLocation + 'copy-destination.csv'), True)
        self.assertEqual(stat(config.fileTestLocation + 'copy-source.csv').st_size, stat(config.fileTestLocation + 'copy-destination.csv').st_size)
    
    def test_CopyFileFailsFileCopy(self):
        hlper.copy_file(config.fileTestLocation + 'copy-source-fake.csv', config.fileTestLocation + 'copy-destination.csv', logger=app_logger)
        self.assertEqual(path.exists(config.fileTestLocation + 'copy-destination.csv'), False)
    
    def test_IsFileEmptySucceeds(self):
        self.assertEqual(hlper.is_file_empty(config.fileTestLocation + 'empty-file.csv', logger=app_logger), True)

    def test_IsFileEmptyFails(self):
        self.assertEqual(hlper.is_file_empty(config.fileTestLocation + 'data-file.csv', logger=app_logger), False)

    def test_IsFileEmptyReturnsZeroWithNonExistentFile(self):
        self.assertEqual(hlper.is_file_empty(config.fileTestLocation + 'data-file-fake.csv', logger=app_logger), True)

if __name__ =="__main__":
    unittest.main()