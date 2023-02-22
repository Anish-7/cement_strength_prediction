import os
import pandas as pd
import sqlite3 as sq
import json 
import csv

class Db_operation:

    def __init__(self):
        self.cwd = os.getcwd()
        self.goodFilePath = f"{self.cwd}/Training_Raw_files_validated/Good_Raw"
        self.train_schema_file = open(f"{self.cwd}/schema_training.json",'r')
        self.train_schema = json.load(self.train_schema_file)
        self.Columns = self.train_schema['ColName']

    def db_connection(self,dbname):
        
        try:
            con = sq.connect(f"{self.cwd}/{dbname}.db")
        except:
            print("Error while connecting to database:")
        return con

    def db_create_table(self,con):  
        for col in self.Columns.keys():
            try:
            
                # print(col)
                con.execute("CREATE TABLE  Good_Raw_Data ({c} {d})".format(c=col,d=self.Columns[col]))

            except:
            # print("already created")
                con.execute('ALTER TABLE  Good_Raw_Data ADD COLUMN "{c}" "{d}"'.format(c=col,d=self.Columns[col]))
    
    
    def insertIntoTableGoodData(self,Database):

        conn = self.db_connection(Database)
        self.db_create_table(conn)

        goodFilePath= self.goodFilePath

        good_files = [f for f in os.listdir(self.goodFilePath)]
 

        for file in good_files:
            try:
                with open(goodFilePath+'/'+file, "r") as f:
                    next(f)# avoid header
                    reader = csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        
                        for list_ in (line[1]):
                            try:
                                # print(list_)
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()

                conn.close()

        conn.close()

    def convert_to_csv(self,dbname):
        conn = self.db_connection(dbname)
        query = f"SELECT * FROM Good_Raw_Data"
        dat = pd.read_sql_query(query,conn)
        dat.to_csv(f'{self.cwd}/agg_train_data/train.csv')
        



# d = Db_operation()
# # d.insertIntoTableGoodData('train')
# d.convert_to_csv('train')