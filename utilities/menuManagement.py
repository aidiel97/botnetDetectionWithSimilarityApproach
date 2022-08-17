import time
import os
import utilities.mongoDb as mongo

listMenu =[
  {
    'title': '[Knowledgebase Management] Restore',
    'functionName': mongo.restore
  },
  {
    'title': '[Knowledgebase Management] Export(dump) yours',
    'functionName': mongo.dump
  },
  {
    'title': 'EXIT',
    'functionName': exit
  }
]

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