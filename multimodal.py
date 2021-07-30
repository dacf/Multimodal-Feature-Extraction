import pandas as pd
import glob
import numpy as np
import geopandas as gp
from shapely.geometry import Point, Polygon
from pandas.core.common import flatten
import os

def myPoint(n):
    return Point(n)

def main():
    print("hi")
    #Path for the gaze data files
    file_path = "FILES_PATH" 
    all_files = glob.glob(file_path + "/*.csv")

    #We iterate through every file and read it
    for file in all_files:
        path = file
        patient = os.path.basename(path).replace('.csv', '')
        df = pd.read_csv(path)
        dfAOI = df[df["InfoUnit"].notna()]
        #Windows are defined here
        backWindow = 500
        frontWindow = 500 
        # We create a marker on the dataframe with only AOI'S to mark the difference of consecutive groups of AOI'S
        dfAOI['marker'] = (dfAOI['InfoUnit'] != dfAOI['InfoUnit'].shift()).cumsum()
        # We keep the first and last indices of each group and we add them as columns to our new df_master, we also reset the index
        df_master = dfAOI.index.to_series().groupby(dfAOI['marker']).agg(['first','last']).reset_index()
        df_first = df_master[['first']]
        df_last = df_master[['last']]
        #We add our patient to a new patient coulmn
        df_master['PATIENT'] = patient
        # We query the main dataframe to find the recording time stamp of our first and last indices 
        df_firstIndexTime = df.loc[df_master['first'], ['RecordingTimestamp']]
        df_lastIndexTime = df.loc[df_master['last'], ['RecordingTimestamp']]
        df_master= df_master.assign(recordingTimeStampStart=df_firstIndexTime.values,recordingTimeStampEnd=df_lastIndexTime.values)
        df_master=df_master.assign(timeTargetBack=df_master['recordingTimeStampStart'].values-backWindow,timeTargetFront=df_master['recordingTimeStampEnd'].values+frontWindow)
        df['copy_index'] = df.index
        df_backward = pd.merge_asof(df_master, df, left_on='timeTargetBack',right_on='RecordingTimestamp',direction='backward')
        df_forward = pd.merge_asof(df_master,df,left_on='timeTargetFront',right_on='RecordingTimestamp',direction='forward')
        df_forward[['RecordingTimestamp', 'timeTargetBack','copy_index']]
        df_master=df_master.assign(backWindowIndex=df_backward['copy_index'].values)
        df_master=df_master.assign(frontWindowIndex=df_forward['copy_index'].values)
        df_master=df_master.assign(InfoUnits=df.loc[df_master['first'],'InfoUnit'].values)
        df_list = []
        df['point'] = list(zip(df['FixationPointX..MCSpx.'], df['FixationPointY..MCSpx.']))
        for index, row in df_master.iterrows():
            df_list.append(df.loc[row['backWindowIndex'] : row['frontWindowIndex']].drop_duplicates('FixationIndex'))
        

        polys = gp.GeoSeries({
            'BOY': Polygon([(590,165),(680,200),(605,655),(495,617),(510,300),(595,170)]),
            'JAR': Polygon([(400,115),(520,115),(520,235),(400,235)]),
            'COOKIE': Polygon([(420,260),(475,260),(475,320),(420,320)]),
            'STOOL': Polygon([(495,617),(605,655),(522,878),(438,869),(386,788)]),
            'GIRL': Polygon([(255,444),(452,350),(413,739),(386,788),(420,845),(420,900),(325,910)]),
            'WOMAN': Polygon([(920,145),(1020,145),(1045,350),(985,395),(990,455),(950,500),(990,565),(1040,560),(1065,670),(1010,880),(890,890),(830,550)]),
            'PLATE': Polygon([(1045,350),(1110,365),(1110,415),(1060,460),(990,455),(985,395)]),
            'DISHCLOTH': Polygon([(1060,460),(1040,560),(990,565),(950,500),(990,455)]),
            'CURTAINS': Polygon([(970,145),(970,90),(1480,90),(1480,615),(1385,575),(1425,445),(1285,230),(1250,120),(1210,230),(1080,355),(1045,350),(1020,145)]),
            'WINDOW': Polygon([(1250,120),(1285,230),(1425,445),(1385,575),(1055,510),(1060,460),(1110,415),(1110,365),(1089,355),(1210,230)]),
            'SINK': Polygon([(1055,510),(1350,565),(1222,672),(1055,605),(1040,560)]),
            'WATER': Polygon([(1222,672),(1155,990),(890,990),(830,940),(890,890),(1010,880),(1065,670),(1055,605)]),
            'DISHES': Polygon([(1300,625),(1480,625),(1480,710),(1300,710)])
            })
        hitsList = []
        for x in df_list:
            pointList = list(map(myPoint, (x['point'].values)))
            _pnts = pointList
            pnts = gp.GeoDataFrame(geometry=_pnts)
            hitsList.append(pnts.assign(**{key: pnts.within(geom) for key, geom in polys.items()}))
        resList = []
        for data in hitsList:
            res = list(flatten(pd.DataFrame(data.columns.where(data == True).tolist()).values.tolist()))
            ans = [x for x in res if not isinstance(x, float)]
            noDuplicate = pd.Series(ans).drop_duplicates()
            resList.append(list(noDuplicate))
        df_master['HITS' + '_' + str(backWindow)] = resList
        df_master
        hitToCSV = df_master[['PATIENT', 'InfoUnits', 'HITS_500']]
        mypath= "/Users/diegochui/Desktop/Canary-Work/MultimoalFeatures/outputhitsv"
        hitToCSV.to_csv(mypath + '/' + patient + 'output' +'.csv',encoding='utf-8')

        df_backLatency = df_master
        df_backLatency.drop(df_backLatency.loc[df_backLatency['InfoUnits']=='KITCHEN'].index, inplace=True)
        df_backLatency.drop(df_backLatency.loc[df_backLatency['InfoUnits']=='EXTERIOR'].index, inplace=True)
        df_backLatency.drop(df_backLatency.loc[df_backLatency['InfoUnits']=='CUPBOARD'].index, inplace=True)
        df_backLatency.reset_index()

        df_listBackLatency = []
        df_backIndices = []
        for index, row in df_backLatency.iterrows():
            sub_df = df.loc[0 : row['first'] - 1].drop_duplicates('FixationIndex')
            pointList = list(map(myPoint, (sub_df['point'].values)))
            _pnts = pointList
            pnts = gp.GeoDataFrame(geometry=_pnts)
            df_table = pnts.assign(**{key: pnts.within(geom) for key, geom in polys.items()})
            exactPoly =  df_table[['geometry', row['InfoUnits']]]
            lastHit = exactPoly.where(exactPoly == True).last_valid_index()
            if(lastHit != None):
                #x = sub_df.iloc[lastHit]['RecordingTimestamp'] 
                res = df.loc[row['first']]['RecordingTimestamp'] - sub_df.iloc[lastHit]['RecordingTimestamp'] 
                df_backIndices.append(sub_df.iloc[lastHit]['copy_index'])   
            else:
                df_backIndices.append(-1)
                res = -1
            df_listBackLatency.append(res)
        df_master['BackLatency'] = df_listBackLatency
        df_master['BackIndex'] = df_backIndices
        
        df_frontLatency = df_master
        df_frontLatency.drop(df_backLatency.loc[df_backLatency['InfoUnits']=='KITCHEN'].index, inplace=True)
        df_frontLatency.drop(df_backLatency.loc[df_backLatency['InfoUnits']=='EXTERIOR'].index, inplace=True)
        df_frontLatency.drop(df_backLatency.loc[df_backLatency['InfoUnits']=='CUPBOARD'].index, inplace=True)
        df_frontLatency.reset_index()

        df_listFrontLatency = []
        df_frontIndices = []
        for index, row in df_frontLatency.iterrows():
            sub_df = df.loc[row['last'] + 1 : len(df)].drop_duplicates('FixationIndex')
            pointList = list(map(myPoint, (sub_df['point'].values)))
            _pnts = pointList
            pnts = gp.GeoDataFrame(geometry=_pnts)
            df_table = pnts.assign(**{key: pnts.within(geom) for key, geom in polys.items()})
            exactPoly =  df_table[['geometry', row['InfoUnits']]]
            firstHit = exactPoly.where(exactPoly == True).first_valid_index()
            if(firstHit != None):
                #x = sub_df.iloc[lastHit]['RecordingTimestamp'] 
                res = sub_df.iloc[firstHit]['RecordingTimestamp'] - df.loc[row['last']]['RecordingTimestamp']
                df_frontIndices.append(sub_df.iloc[firstHit]['copy_index'])   
            else:
                df_frontIndices.append(-1)
                res = -1
            df_listFrontLatency.append(res)
        df_master['FrontLatency'] = df_listFrontLatency
        df_master['FrontIndex'] = df_frontIndices

        latencyToCSV = df_master[['PATIENT', 'InfoUnits', 'BackLatency', 'FrontLatency']]

        mypath= "PATH_OUTPUT"
        latencyToCSV.to_csv(mypath + '/' + patient + 'output' +'.csv',encoding='utf-8')

if __name__ == "__main__":
    main()