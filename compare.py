from dbfread import DBF
import pandas as pd # pip install pandas
import os



directory1 = input('file name:')
directory2 = input(' file name:')

for root, dirs, files in os.walk(r'C:\Users\jocas\Desktop\New folder'):
  if directory1 in files:        
    dbf1 = DBF(os.path.join(root, directory1), ignore_missing_memofile=True, encoding='latin-1')    
    df1 = pd.DataFrame(iter(dbf1))
for root, dirs, files in os.walk(r'C:\Users\jocas\Desktop\New folder'):
  if directory2 in files:   
    dbf2 = DBF(os.path.join(root, directory2), ignore_missing_memofile=True, encoding='latin-1')    
    df2 = pd.DataFrame(iter(dbf2))

differences = set(df1.columns).difference(set(df2.columns))
f = open("differences.txt", "a")
if (len(differences)>0):
  f.write('Differences between {0} and {1} file are:{2}\n'  .format(directory1, directory2, differences))
else:
  f.write('There are no differences between {0} and {1}\n' .format(directory1, directory2))  
f.close()