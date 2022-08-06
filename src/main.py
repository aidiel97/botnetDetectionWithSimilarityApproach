"""Botnet Detection with Similarity Approach"""
"""Writen By: M. Aidiel Rachman Putra"""
"""Organization: Net-Centic Computing Laboratory | Institut Teknologi Sepuluh Nopember"""

import warnings
# from botnetDetectionWithSimilarityApproach.src.preProcessing import preProcessing
warnings.simplefilter(action='ignore')


from dataLoader import *
from classification import *
from splitData import *
from preProcessing import *
from mongoDb import *
import time
import os
import uuid

attackStageLength = 1 #in second
listMenu = [
    # "Extract Source Dataset",
    "EXIT"
]

#menu
def menu():
    print("\n\n================================================================")
    print("======| Botnet Simultaneous Dataset Generator |=================")
    print("================================================================\n")
    print("Main Menu: ")
    for i, menu in enumerate(listMenu):
        print(str(i+1)+". "+menu)
    print("\n================================================================")
    choose = input("Enter Menu: ")
    print("============| Processing Menu: "+choose+". "+listMenu[int(choose)-1]+" |========================\n")
    if(choose == "1"):
        # extract()
        time.sleep(3) # adding 3 seconds time delay
        os.system("clear")
        print("++++++| "+listMenu[0]+" Process Success |++++++")
        print("...back to menu...")
    elif(int(choose) == len(listMenu)):
        print("++++++| "+listMenu[-1]+" |++++++")
        exit()
    else:
        print("This Menu Does Not Exist, Please Try Another Input!")
        time.sleep(3) # adding 3 seconds time delay
        os.system("clear")

def detectionWithMachineLearning(datasetName, selectedScenario):
  raw_df = loadDataset(datasetName, selectedScenario)
  df = preProcessing(raw_df) #preProcessed dataframe
  train, test = splitTestAllDataframe(df)

  result = classification(df, train, test, 'randomForest')
  raw_df = raw_df.drop(['activityLabel'],axis=1)
  data = raw_df.assign(ActivityLabel=result) #assign new column

  botnet_df = data[data['ActivityLabel'].isin([1])] #create new bot dataframes
  normal_df = data[data['ActivityLabel'].isin([0])] #create new normal dataframes
  botnet_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe
  normal_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe

  return botnet_df, normal_df

def detectionWithLabel(datasetName, selectedScenario): #not intended for anything else except of a knowledgebase
    df = loadDataset(datasetName, selectedScenario)
    df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
    botnet_df = df[df['ActivityLabel'].isin([1])] #create new bot dataframes
    normal_df = df[df['ActivityLabel'].isin([0])] #create new normal dataframes
    botnet_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe
    normal_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe

    return botnet_df, normal_df

if __name__ == "__main__":
#   menu()
    i=7
    datasetName = ctu
    stringDatasetName = 'ctu'
    selectedScenario = 'scenario'+str(i)

    # botnet_df, normal_df = detectionWithMachineLearning(datasetName,  selectedScenario)
    botnet_df, normal_df = detectionWithLabel(datasetName, selectedScenario)
    botnet_df = labelGenerator(botnet_df)
    columns = botnet_df.columns.values

    tempSrcAddr = ''
    tempSequentialActivity = {}
    for index, row in botnet_df.iterrows():
        netT = [row[x] for x in columns] #stack values in row to array
        if(tempSrcAddr != row['SrcAddr']):
            query = {
                'SrcAddr':row['SrcAddr'],
                'FromDatasets': str(stringDatasetName).upper()+str(i)
            }
            sequentialActivity = findOne(query)
            #if the sequential activity not exist in Database
            if(sequentialActivity == None):
                sequentialActivity = {
                    'SequentialActivityId':str(uuid.uuid4()),
                    'SrcAddr':row['SrcAddr'],
                    'Activities': [netT],
                    'Vectors':[],
                    'FromDatasets': stringDatasetName+str(i),
                    'ActivityHeaders': columns.tolist()
                }
                insertOne(sequentialActivity)

            #if the sequential activity already exist in Database
            else:
                sequentialActivity['Activities'].append(netT)
                upsertOne(
                    {'SequentialActivityId': sequentialActivity['SequentialActivityId']},
                    sequentialActivity
                )
            tempSequentialActivity = sequentialActivity

        else:
            tempSequentialActivity['Activities'].append(netT)
            upsertOne(
                {'SequentialActivityId': tempSequentialActivity['SequentialActivityId']},
                tempSequentialActivity
            )

        tempSrcAddr = row['SrcAddr']



    record = findOne({'SequentialActivityId':'4efbc90f-fe6c-47ec-b0c5-a9abd46c5090'})
    dataframe = pd.DataFrame(record['Activities'], columns =record['ActivityHeaders'], dtype = float)
    print(dataframe.head)