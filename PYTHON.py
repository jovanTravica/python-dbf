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
import configparser

# smtp_handler = logging.handlers.SMTPHandler(mailhost=("smtp.gmail.com", 587),
#                                             fromaddr="pythondbf@gmail.com", 
#                                             toaddrs="jtravica1@gmail.com", 
#                                             subject=u"Python error",
#                                             credentials=(config['EMAIL']['username'],config['EMAIL']['password']),
#                                             secure =())


# logger = logging.getLogger()
# logger.addHandler(smtp_handler)
config = configparser.ConfigParser()
config.read('once.conf')

yearsSTR = config['DEFAULT']['years']
years = yearsSTR.split(',')
path= config['PATH']['path']
directory = ['fak1','fak2','k_d','sifmat']  
files = []
for dir in directory:
 for year in years:
  file = (os.path.join(path, year, dir + '.dbf')) #makes a list of files in the given folder, with dbf extension
  files.append(file)


   
username = config['DATABASE']['username'] #isto
database = config['DATABASE']['database']  #sve preko configa



engine = create_engine('mssql+pyodbc://'+username+':@localhost/'+database+'?driver=SQL+Server+Native+Client+11.0')
i = int(config['DEFAULT']['i'])   
j = int(config['DEFAULT']['j'])
# try:
for file in files: 
  if(os.path.basename(file)=='fak1.dbf'):
   if (i == 0):
    dbfPath = DBF(file, ignore_missing_memofile=True, encoding='latin-1')    
    frame = pd.DataFrame(iter(dbfPath))       #converts from dbf to dataframe
    frame['Path'] = file
    frame.to_sql('fakture', con=engine,index=False, if_exists='append')    # creates table and inserts data into it from dataframe  /  if_exists='replace',  index_label='Path'
         
   else:
    dbfPath = DBF(file, ignore_missing_memofile=True, encoding='latin-1')    
    frame = pd.DataFrame(iter(dbfPath)).drop(columns=['POTPIS1','POTPIS2','POTPIS3'])      #converts from dbf to dataframe
    frame['Path'] = file
    if(j!=0):
     engine.execute("DELETE FROM fakture WHERE Path='{0}'".format(file))
    frame.to_sql('fakture', con=engine, index=False, if_exists='append')      
   i=i+1 
  
  elif(os.path.basename(file) == 'fak2.dbf'):
    dbfPath = DBF(file, ignore_missing_memofile=True, encoding='latin-1')    
    frame = pd.DataFrame(iter(dbfPath))       #converts from dbf to dataframe
    frame['Path'] = file
    if(j!=0):
     engine.execute("DELETE FROM fakture WHERE Path='{0}'".format(file))
    frame.to_sql('fakture_stavke', con=engine,index=False, if_exists='append')
  
  elif(os.path.basename(file) == 'k_d.dbf'):
    dbfPath = DBF(file, ignore_missing_memofile=True, encoding='latin-1')    
    frame = pd.DataFrame(iter(dbfPath))       #converts from dbf to dataframe
    frame['Path'] = file
    if(j!=0):
     engine.execute("DELETE FROM fakture WHERE Path='{0}'".format(file))
    frame.to_sql('kupci_dobavljaci', con=engine,index=False, if_exists='append')
  
  else:
    dbfPath = DBF(file, ignore_missing_memofile=True, encoding='latin-1')    
    frame = pd.DataFrame(iter(dbfPath))       #converts from dbf to dataframe
    frame['Path'] = file
    if(j!=0):
     engine.execute("DELETE FROM fakture WHERE Path='{0}'".format(file))
    frame.to_sql('artikli', con=engine,index=False, if_exists='append')

# except Exception as e:
#     logger.exception(e)






