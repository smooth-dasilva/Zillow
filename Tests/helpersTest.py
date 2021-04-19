import Utilities.helpers as hlper
import unittest     
from Utilities.logger import setup_logger


app_logger = setup_logger('app_logger', './app.log')



class HelpersTestCase(unittest.TestCase):

    
    def test_AbbreviateLongNamesChangesDateColName(self):
        self.assertEqual(hlper.abbreviateLongNames("Date"), "Date_MDY") 
    def test_AbbreviateLongNamesReturnsShortenedStringOfFirstIndexWhenDelimittedByUnderScore(self):
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted"), "InvSeasAdj") 
    def  test_AbbreviateLongNamesReturnsShortenedStringOfFirstIndexWhenDelimittedByUnderScore2(self):
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted_Else"), "InvSeasAdj_Else") 
    def  test_AbbreviateLongNamesReturnsShortenedStringOfFirstIndexWhenDelimittedByUnderScore3(self):
        self.assertEqual(hlper.abbreviateLongNames("InventorySeasonallyAdjusted_Else_AnythingElse"), "InvSeasAdj_Else_AnythingElse")
    def test_AbbreviateLongNamesDoesntReturnShortenedStringIfNotFormattedProperly(self):
        self.assertEqual(hlper.abbreviateLongNames("inventoryseasonallyadjusted_AnythingElse"), "inventoryseasonallyadjusted_AnythingElse") 


if __name__ == '__main__':
    unittest.main()