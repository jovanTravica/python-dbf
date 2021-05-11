import pypyodbc as odbc # pip install pypyodbc
import pandas as pd # pip install pandas
import glob
import pyodbc
import os
from sqlalchemy import create_engine
from dbfread import DBF
from pathlib import Path
import logging
import logging.handlers

smtp_handler = logging.handlers.SMTPHandler(mailhost=("smtp.gmail.com", 587),
                                            fromaddr="jtravica1@gmail.com", 
                                            toaddrs="jocaskull0809@gmail.com", 
                                            subject=u"Python error",
                                            credentials=("pythondbf@gmail.com", "yBp3FTaY8yH8zNj"),
                                            secure =())


logger = logging.getLogger()
logger.addHandler(smtp_handler)

directory = input('Please input directory path:')
files = glob.glob(os.path.join(directory, '*.dbf')) #makes a list of files in the given folder, with dbf extension



# for root, dirs, files in os.walk('C:\\'):
#     if directory in files:
#         print (os.path.join(root, directory))
   
       




username = input('Please input your username:')
database = input('Please input the database:')
table = input('Please input the name of the table:')


engine = create_engine('mssql+pyodbc://'+username+':@localhost/'+database+'?driver=SQL+Server+Native+Client+11.0')

try:
  for file in files:     
    dbfPath = DBF(file, ignore_missing_memofile=True)    
    frame = pd.DataFrame(iter(dbfPath))         #converts from dbf to dataframe
    frame.to_sql(table, con=engine, index=False, if_exists='append')    # creates table and inserts data into it from dataframe             
     
except Exception as e:
    logger.exception(e)






