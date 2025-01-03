#!/awips2/python/bin/python
helpstr='''
==============================================================================
filterA
v1.1 
Gunnar Leffler
 5 September 2012
This script ignores single line SHEF messages it has seen in the recent past.
The history is located in the directory the script is called from. Default 
history size is 10000 lines.
usage:
 filterA <filename|history size> <filename>
examples:
 filterA  - takes input from STDIN and returns output to STDOUT
 filterA 20000 - same as above, limits dictionary to 20000 lines
 filterA 20000 in.shf - same as above, reads input from in.shf
==============================================================================
'''

import sys,os

#Global variables
shefHistory = []
shefDictionary = {}
historySize = 10000
historyPath = "/data/ldad/localapps/get_usbr_webdata/shef/SHEF.history"

def readHistory (path): 
   output = []
   if os.path.exists(path):
      theFile = open(path, "r")
      for s in theFile:
         output.append(s.strip())
      theFile.close()
   return output

def writeHistory (path, lines): 
   theFile = open(path, "w")
   count = 0
   startElement = len(shefHistory) - historySize
   for s in lines:
      count += 1
      if count > startElement:
         theFile.write(s+"\n")
   theFile.close()

def index(seq, f):
   retval = -1
   for i in xrange(len(seq)):
      if f == seq[i]:
         retval = i
         break
   return retval

def findNewMessages (path):
   if path == None:
      theFile = sys.stdin
   else:
      theFile = open(path, "r")
   lines = theFile.readlines()
   output = []
   for s in lines:
      st = s.strip()
#      if index(shefHistory,st) == -1:
      if not st in shefDictionary:
         shefHistory.append(st)
         shefDictionary[st]=0
         output.append(st)
   return output


#=============================================
#This is the "entrypoint" for the script
#=============================================
shefHistory = readHistory (historyPath)
shefDictionary = {} #Turns history into dictionary for faster searches
for line in shefHistory:
  shefDictionary[line] = 0
thePath = None #The reader function will use STDIN as default
if len(sys.argv) == 1:
   thePath = None #if no file is specified, use STDIN
elif len(sys.argv) == 2:
   if sys.argv[1].isdigit(): #You can supply history size as the parameter
      historySize = int(sys.argv[1])
   else:
      thePath = sys.argv[1] 
else:
   if sys.argv[1].isdigit(): #You can supply history size as the parameter
      historySize = int(sys.argv[1])
      thePath =sys.argv[2]
   else:
      historySize = int(sys.argv[2])
      thePath =sys.argv[1]
response = findNewMessages (thePath)  
writeHistory (historyPath,shefHistory)
for s in response:
   print(s)
