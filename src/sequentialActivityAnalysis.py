import uuid
from mongoDb import *
from datetime import datetime

attackStageLength = 60 #in second
defaultColumns = [
	"StartTime",
	"Dur",
	"Proto",
	"SrcAddr",
	"Sport",
	"Dir",
	"DstAddr",
	"Dport",
	"State",
	"sTos",
	"dTos",
	"TotPkts",
	"TotBytes",
	"SrcBytes",
	"Label",
	"ActivityLabel",
	"t1",
	"DiffWithPreviousAttack",
	"NetworkActivity"
]

def sequentialActivityMining(dataframe, stringDatasetName, datasetDetail, columns=defaultColumns):
    tempSrcAddr = ''
    tempSequentialActivity = {}
    for index, row in dataframe.iterrows():
        netT = [row[x] for x in columns] #stack values in row to array
        if(tempSrcAddr != row['SrcAddr'] or (tempSrcAddr == row['SrcAddr'] and row['DiffWithPreviousAttack'] >= attackStageLength)):
            query = {
                'SrcAddr':row['SrcAddr'],
                'FromDatasets': str(stringDatasetName).upper()+str(datasetDetail)
            }
            sequentialActivity = findOne(query)
            #if the sequential activity not exist in Database
            if(sequentialActivity == None):
                sequentialActivity = {
                    'SequentialActivityId':str(uuid.uuid4()),
                    'SrcAddr':row['SrcAddr'],
                    'Activities': [netT],
                    'Vectors':[],
                    'FromDatasets': stringDatasetName,
                    'DatasetsDetails': str(datasetDetail),
                    'ActivityHeaders': columns,
                    'createdAt': datetime.now(),
                    'modifiedAt': datetime.now(),
                }
                insertOne(sequentialActivity)

            #if the sequential activity already exist in Database
            else:
                sequentialActivity['Activities'].append(netT)
                sequentialActivity['modifiedAt'] = datetime.now()
                upsertOne(
                    {'SequentialActivityId': sequentialActivity['SequentialActivityId']},
                    sequentialActivity
                )
            tempSequentialActivity = sequentialActivity

        else:
            tempSequentialActivity['Activities'].append(netT)
            tempSequentialActivity['modifiedAt'] = datetime.now()
            upsertOne(
                {'SequentialActivityId': tempSequentialActivity['SequentialActivityId']},
                tempSequentialActivity
            )

        tempSrcAddr = row['SrcAddr']