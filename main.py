import os
import math
import pandas as pd
import smtplib, ssl
import matplotlib.pyplot as plt


from Utilities.logger import setup_logger
from Utilities.quickstart import SendMessage


# set up loggers
app_logger = setup_logger('app_logger', 'app.log')

# for log session separator
logSeparator = '++++++++++++++++++++++++++++'

# On app load : create logs
app_logger.info(f"Begin app log from {__name__}\n{logSeparator}")



def main():   
    app_logger.info("I work...")


if __name__ =="__main__":
    main()



# logger ending
app_logger.info(f"End app log from {__name__}\n{logSeparator}")