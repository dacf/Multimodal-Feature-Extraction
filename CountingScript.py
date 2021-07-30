import pandas as pd
import glob
import numpy as np
import os


def main():

path = 'input_files/'
excel_files = glob.glob("excels/*.xlsx")

for excel in excel_files:
    out = excel.split('.')[0]+'.csv'
    fileName = out.split('/')[1]
    singleFile = pd.read_excel(excel)
    outputFilePath = os.path.join(path,fileName)
    singleFile.to_csv(outputFilePath, encoding='utf-8', index=False)


all_files = glob.glob("input_files/*.csv")
df_from_each_file = (pd.read_csv(f,index_col=0) for f in all_files)
df  = pd.concat(df_from_each_file, ignore_index=True,sort=False)


df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
df = df.dropna(subset = ['task_number'])
df['experimenter_speaking'] = df['experimenter_speaking'].fillna('Participant')
df = df.drop(df[df.experimenter_speaking == 'Experimenter'].index)
df.reset_index(drop=True, inplace=True)

missing_df = df.loc[df.time_stamp_start.isna() & df.time_stamp_end.isna()]

output_df = missing_df['spoken_word'].value_counts()

output_df.to_csv('output.csv',encoding='utf-8')

if __name__ == "__main__":
    main()