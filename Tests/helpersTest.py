import pandas as pd
import Utilities.helpers as hlper
import unittest     
from Utilities.logger import setup_logger


app_logger = setup_logger('app_logger', './app.log')

d = {'Population' : ['37000000','54000000'], 'Record Date' : ['2003-07-15','23-07-1985'], 'RegionName' : ['Texas','California']}
test_df = pd.DataFrame(data = d)
convert_df = hlper.convert_col_types(test_df)

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

if __name__ == '__main__':
    unittest.main()