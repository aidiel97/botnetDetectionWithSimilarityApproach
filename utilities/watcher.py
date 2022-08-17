import time

def watcherStart(processName):
  start = time.time()
  print('\n==========|\t\t '+processName+' [ START ] \t\t\t|==========')

  return start

def watcherEnd(processName, start=time.time()):
  end = time.time()
  processingTime = end - start
  print('\n==========|\t '+processName+' [ End ] '+str(processingTime)+' s \t|==========')
