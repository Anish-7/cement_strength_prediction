import json
import os
import re
import shutil
import pandas as pd


class validate:

    def train_validate(self,path):

    
        self.cwd = os.getcwd()
        self.train_schema_file = open(f"{self.cwd}/schema_training.json",'r')
        self.train_schema = json.load(self.train_schema_file)
        self.LengthOfDateStampInFile = self.train_schema['LengthOfDateStampInFile']
        self.LengthOfTimeStampInFile = self.train_schema['LengthOfTimeStampInFile']
        self.column_names = self.train_schema['ColName']
        self.NumberofColumns = self.train_schema['NumberofColumns']

        
        self._validate_file_name(path)
        self.validateColumnLength(self.NumberofColumns)
     
    ### helper validate functions###


    def _validate_file_name(self,path):
        files = [f for f in os.listdir(path)]
        regex = "['cement_strength']+['\_'']+[\d_]+[\d]+\.csv"
        self._create_validated_dir()

        for file in files :

                if (re.match(regex,file)):

                    splitAtDot = re.split('.csv',file) 
                    splitAtDot = (re.split('_', splitAtDot[0]))

                    if len(splitAtDot[2]) == self.LengthOfDateStampInFile:
                        if len(splitAtDot[3]) == self.LengthOfTimeStampInFile:
                            shutil.copy(f"{self.cwd}/Training_Batch_Files/" +file, f"{self.cwd}/Training_Raw_files_validated/Good_Raw")

                        else:
                            shutil.copy(f"{self.cwd}/Training_Batch_Files/" +file, f"{self.cwd}/Training_Raw_files_validated/Bad_Raw")

                    else:
                        shutil.copy(f"{self.cwd}/Training_Batch_Files/" +file, f"{self.cwd}/Training_Raw_files_validated/Bad_Raw")

                else:
                    shutil.copy(f"{self.cwd}/Training_Batch_Files/" +file, f"{self.cwd}/Training_Raw_files_validated/Bad_Raw")
  
    
    def validateColumnLength(self,NumberofColumns):

        try:

            for file in os.listdir(f'{self.cwd}/Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv(f'{self.cwd}/Training_Raw_files_validated/Good_Raw/'+ file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move(f'{self.cwd}/Training_Raw_files_validated/Good_Raw/' + file, f'{self.cwd}/Training_Raw_files_validated/Bad_Raw/')

        except OSError:

            raise OSError

    ## file CRUD operation ##
    def _create_validated_dir(self):
        try:
            path = os.path.join(f"{self.cwd}/Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join(f"{self.cwd}/Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
        except:
            print("error")

# r = validate()
# fi=r.train_validate("/home/anish/plan/infosys/ceament_proj/orginal/code/cement_strength_reg/Training_Batch_Files")
# print(fi)
