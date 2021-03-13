import os
import math



from getpass import getpass
import mysql.connector



from Utilities.logger import setup_logger
from Utilities.quickstart import SendMessage


# set up loggers
app_logger = setup_logger('app_logger', 'app.log')

# for log session separator
logSeparator = '++++++++++++++++++++++++++++'

# On app load : create logs
app_logger.info(f"Begin app log\n{logSeparator}")



def main():   
    try:
        with mysql.connector.connect(
            host="localhost",
            user=input("Enter username: "),
            password=getpass("Enter password: "),
        ) as connection:
            print(connection)
            create_db_query = "CREATE DATABASE online_movie_rating"
            with connection.cursor() as cursor:
                cursor.execute(create_db_query)
    except mysql.connector.Error as e:
        app_logger.exception(e)


if __name__ =="__main__":
    main()



# logger ending
app_logger.info(f"End app log\n{logSeparator}")