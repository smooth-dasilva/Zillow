import logging
from collections import deque

from Utilities.mysql_conn import get_update_mysql_conn 

class ProcessingExpertClass:

    app_logger : logging

    ListCols : deque
    ListTypes : deque

    def __init__(self, _app_logger, listCols, listTypes) -> None:
        self._app_logger = _app_logger 
        self.ListCols = listCols
        self.ListTypes = listTypes