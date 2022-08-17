import time
import os
import utilities.mongoDb as mongo
import utilities.dataLoader as dl
import src.knowledgebaseGenerator as kbgen

listMenu =[
  {
    'title': '[Knowledgebase Management] Generate - Single scenarios/sensors',
    'functionName': kbgen.singleDatasetDetail
  },
  {
    'title': '[Knowledgebase Management] Generate - All Scenarios/sensors in Dataset',
    'functionName': kbgen.multiDatasetDetail
  },
  {
    'title': '[Knowledgebase Management] Export(dump) yours',
    'functionName': mongo.dump
  },
  {
    'title': '[Knowledgebase Management] Restore',
    'functionName': mongo.restore
  },
  {
    'title': 'EXIT',
    'functionName': exit
  }
]

def getListDatasetMenu():
  os.system("clear")
  x=1
  print("\n\n----------------------------------")
  print("| List Available Datasets |-----------")
  print("---------------------------------\n")
  for dataset in dl.listAvailableDatasets:
    print(str(x)+". "+dataset['name'])
    x += 1
  print("\n---------------------------------")
  choose = input("Choose one dataset: ")
  return choose

def getListDatasetDetailMenu(datasetIndex):
  os.system("clear")
  chooseDataset = dl.listAvailableDatasets[int(datasetIndex)-1]
  listSubDataset = list(chooseDataset['list'].keys())
  x=1
  print("\n\n----------------------------------")
  print("| List Sub Dataset on "+chooseDataset['name']+" |-----------")
  print("---------------------------------\n")
  for subDataset in listSubDataset:
    print(str(x)+". "+subDataset)
    x += 1
  print("\n---------------------------------")
  choose = input("Choose one subDataset: ")
  return choose

def banner():
  x=1
  print("\n\n================================================================")
  print("======| Botnet Detection with Similarity Approach |=============")
  print("================================================================\n")
  print("Main Menu: ")
  for menu in listMenu:
    print(str(x)+". "+menu['title'])
    x += 1
  print("\n================================================================")

def execute(menuIndex):
  if(menuIndex < len(listMenu)):
    print("============| Processing Menu: "+str(menuIndex+1)+". "+listMenu[menuIndex]['title']+" |========================\n")
    # execute the function
    listMenu[menuIndex]['functionName']()

    time.sleep(3) # adding 3 seconds time delay
    os.system("clear")
    print("++++++| "+listMenu[menuIndex]['title']+" Process Success |++++++")
    print("...back to menu...")
  else:
    print("This Menu Does Not Exist, Please Try Another Input!")
    time.sleep(3) # adding 3 seconds time delay
    os.system("clear")
    mainMenu() #call menu again

def mainMenu():
  banner()
  choose = input("Enter Menu: ")
  execute(int(choose)-1)