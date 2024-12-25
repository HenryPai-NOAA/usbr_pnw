#!/awips2/python/bin/python
import re,requests,datetime
from datetime import timedelta
import time, sys, os
#import pdb # debugger

os.umask(0o002)

helpStr = '''
get_usbr_webdata v1.0
04 November 2011
POC: Gunnar Leffler
modified: henry pai (noaa nwrfc), 2022 Jun --> upgrade to python 3, remove urllib calls

This program get data from the USBR's web service and converts it to SHEF for
transfer/ingestion into another database. It uses alias files to convert the
physical element codes the USBR uses to PE codes found in the SHEF manual.
As of 2018 Feb, this script incorporates multiple station & multiparameter
calls detailed here: https://www.usbr.gov/pn/agrimet/HydrometWebService.doc.

DEPENDENCIES:
 This program expects the following alias files to be in the current working directory (PWD):
  daily.alias
  realtime.alias
 For the multi-station and parameter call, make sure there are no duplicates (same station +
 usbr PE code) within the stations.realtime.list file (CSC is one station that has duplicate in original file

USAGE:
 get_usbr_webdata_hpai02 <daily|realtime|realtimeMultiLong> <lookback window in days|lookback window in hours|lookback window in hours> <stationlist>

EXAMPLE:
 get_usbr_webdata_hpai02 daily 1 stations.list
 get_usbr_webdata_hpai02 realtimeMultiLong 2 stations.realtime.list

'''

dailyURL = "https://www.usbr.gov/pn-bin/daily.pl?parameter=$LOC_ID%20$PE_CODE&syer=$START_YEAR&smnth=$START_MONTH&sdy=$START_DAY&eyer=$END_YEAR&emnth=$END_MONTH&edy=$END_DAY&format=1"
dailyYakURL = "https://www.usbr.gov/pn-bin/daily.pl?parameter=$LOC_ID%20$PE_CODE&syer=$START_YEAR&smnth=$START_MONTH&sdy=$START_DAY&eyer=$END_YEAR&emnth=$END_MONTH&edy=$END_DAY&format=1"

realtimeURL= "https://www.usbr.gov/pn-bin/instant.pl?parameter=$LOC_ID%20$PE_CODE&back=$HOURS&format=1"
realtimeYakURL= "https://www.usbr.gov/pn-bin/instant.pl?parameter=$LOC_ID%20$PE_CODE&back=$HOURS&format=1"
# https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=HFAI%20GH&syer=2011&smnth=12&sdy=07&eyer=2011&emnth=12&edy=07&format=1

# not currently used
#realtimeMultiURL="https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=$LOC_ID%20$PE_CODE,%20$LOC_ID2%20$PE_CODE&syer=$START_YEAR&smnth=$START_MONTH&sdy=$START_DAY&eyer=$END_YEAR&emnth=$END_MONTH&edy=$END_DAY&format=1"

# henry: the original command is for daily, I believe we want instant/realtime.  
# Changed webdaycsv to instant
# henry: "postfix"/suffix changed to look like realtimeURL

realtimeMultiURL_pre="https://www.usbr.gov/pn-bin/instant.pl?parameter="
realtimeMultiURL_post="&back=$HOURS&format=1"

# henry: GLOBAL VAR for now, could potentially change to argument
# set as ALL or integer as a string (with quotes around integer value)
realtimeMultiLength = "100"

# future [possible] use: dfCGI = "actual" realtime data, limited properties and datasets [12/07/2011]
# dfCGIrealtimeURL = "https://www.usbr.gov/pn-bin/dfcgi.pl?site=$LOC_ID&pcode=$PE_CODE&incr=15&back=$LOOKBACK&form=$FORMAT&last="

#opener = urllib.request.build_opener()
#opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
opener = requests.Session()
opener.headers['user-agent'] = 'NoaaBot/NWS/NWRFC'

def div1000(s):
  output = ""
  try:
    #output = str(float(s)/1000)
    output = '%0.3f' % (float(s)/1000)
  except:
    pass
  return output
  
def remove_html_tags(txt):
   p = re.compile(r'<[^<]*?/>')
   return p.sub('', txt)

#==============================================
#Error Logging Code
#==============================================
errorStack = []

def help ():
  print(helpStr)

def logError (content):
  errorStack.append(content)

def complain ():
  if errorStack :
    for errorLine in errorStack :
      sys.stderr.write("*** ERROR : %s\n" % errorLine)
  sys.exit(-1)
#==============================================
#End Error Logging Code
#==============================================

#==============================================
# file handling
#==============================================
def readAliasFile (path): #reads an alias file and returns a dictionary
  csv = readTSV (path)
  alias = []
  for line in csv:
    alias.append ((line.pop(0),line))
  return dict(alias)

  
def readTSV (path):
  #theFile = open(path, "r")
  lines = (line.rstrip('\n') for line in open(path, "r"))

  #lines = theFile.readlines()
  output = []
  for s in lines:
    if len(s) > 1 and s[0] != '#': # ignore blank lines
      row1 = s.split('\t') #split the line by ','
      output.append (row1)
  return output

def makeSHEF (locID,timeObj,tz,PEcode,value):
  output = ".A "+locID+" "+timeObj.strftime("%Y%m%d")+" "+tz+" DH"+timeObj.strftime("%H%M")+"/"+PEcode+" "+value
  return output

def makeDailySHEF (locID,timeObj,tz,PEcode,value):
  #output = ".A "+locID+" "+timeObj.strftime("%Y%m%d")+" "+tz+" DH24/"+PEcode+" "+value
  # 20200213 - Changed DH24 to D00. THIS IS TEMPORARY UNTIL DR 21875 IS FIXED.
  output = ".A "+locID+" "+timeObj.strftime("%Y%m%d")+" "+tz+" DH2359/"+PEcode+" "+value
  #print output   # debug
  return output
  
  # time component? Art ? what about the cwms-post method() for this ? s/b like the reg makeSHEF above ?
def makeRealtimeSHEF (locID,timeObj,tz,PEcode,value):
  output = ".A "+locID+" "+ timeObj.strftime("%Y%m%d")+" "+ tz +" DH" + timeObj.strftime("%H%M")+"/DUE /"+PEcode+" "+value
  #print output   # debug
  return output

#==============================================
# end file handling
#==============================================


#==============================================
# parsing and processing
#==============================================
#This removes cruft from scraped input
def stripGarbage(input):
  output = ""
  if input[0] == "-":
    output = "-"
  for c in input:
    if c.isdigit() or c == ".":
      output += c
  return output

#This removes cruft and returns a list of datetime objects & values
def processDailyInput(buffer): 
  lines = buffer.split('\n')
  flag = 0
  output = []
  errline = ""
  for s in lines:
    s = s.strip()
    #print s   # debug
    if "END DATA" in s:
      flag = 0
    if len(s) > 1 and flag > 1: #if the line isn't blank and not a header or footer
      try:
        tokens = s.split('\t')
        
        #t3 = tokens[1]   # debug
       # print t3   # debug
        
        tokens[1] = tokens[1].strip()
        if tokens[1] != "NO RECORD":
          output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y"),tokens[1]])
        else :
          print(errline+"\t"+s, file=sys.stderr)
          # example : "DATE                HFAI GH     12/07/2011      NO RECORD"
      except:
        pass
    if "BEGIN DATA" in s:
      flag = 1
    if "DATE" in s and flag == 1:
      errline = s
      flag += 1
  return output


#---------------------------------------------  
#bbaley 12-07-2011 RT/15min from arcDaily
#---------------------------------------------  
def processRealTimeInput(buffer): 
  lines = buffer.split('\n')
  flag = 0
  output = []
  errline = ""
  for s in lines:
    s = s.strip()
    if "END DATA" in s:
      flag = 0
    if len(s) > 1 and flag > 1: #if the line isn't blank and not a header or footer
      try:
        tokens = s.split('\t')
        
        tokens[1] = tokens[1].strip()
        if tokens[1] != "NO RECORD":
          # output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y %H:%M"),tokens[1]])
          # keep time portion
          output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y %H:%M"),tokens[1]])
        else :
          print(errline+"\t"+s, file=sys.stderr)
          # example : "DATE                HFAI GH     12/07/2011      NO RECORD"
      except:
        # print "exception"    # debug
        pass
    if "BEGIN DATA" in s:
      flag = 1
    if "DATE" in s or "DATE       TIME" in s and flag == 1:
      errline = s
      flag += 1
  return output

#---------------------------------------------------  
# henry 2018 Feb, processing multiple station input
#---------------------------------------------------  
def processRealTimeMultiInput(buffer): 
  lines = buffer.split('\n')
  flag = 0
  output = []
  errline = ""
  for s in lines:
    #s = s.strip() # henry: this was stripping missing values at the end of lines
    if "END DATA" in s:
      flag = 0
    if len(s) > 1 and flag > 1: #if the line isn't blank and not a header or footer
      try:
        tokens = s.split('\t')

        for i in range(1, len(tokens)):
          # henry: adding by column operations
          
          if len(tokens[i]) == 0 or tokens[i] == "\n" or tokens[i] == "\r":
            # empty/missing values and carriage returns set to "NA"
            # missing values are set to keep overall dimensions correct
            output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y %H:%M"),"NA"])
          else:
            tokens[i] = tokens[i].strip()

            if tokens[i] == "NO RECORD":
              # similar as before and this get sets to sys.stderr
              output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y %H:%M"),"NA"])
              print(errline+"\t"+s, file=sys.stderr)
              #  print >> sys.stderr, errline+"\t"+s
              # example : "DATE                HFAI GH     12/07/2011      NO RECORD"
            else:
              # henry: also making sure column isn't missing
              # output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y %H:%M"),tokens[1]])
              # keep time portion
              output.append([datetime.datetime.strptime(tokens[0],"%m/%d/%Y %H:%M"),tokens[i]])

      except:
        # print "exception"    # debug
        pass
    if "BEGIN DATA" in s:
      flag = 1
    if "DATE" in s or "DATE       TIME" in s and flag == 1:
      errline = s
      flag += 1
  return output

#==============================================
# end parsing and processing
#==============================================
    
def getDailyInput (location,pecode,lookback):
   myURL = dailyURL
   # example: https://www.usbr.gov/pn-bin/webarccsv.pl?parameter=ANTI%20QD&syer=2011&smnth=12&sdy=01&eyer=2011&emnth=12&edy=06&format=1
   # where, in stations.list = ANTI, QD

   et = datetime.datetime.now()
   st =  et - timedelta(days=int(lookback))
   myURL = myURL.replace("$LOC_ID",location)
   myURL = myURL.replace("$PE_CODE",pecode)
   myURL = myURL.replace("$START_YEAR",st.strftime("%Y"))
   myURL = myURL.replace("$END_YEAR",et.strftime("%Y"))
   myURL = myURL.replace("$START_MONTH",st.strftime("%m"))
   myURL = myURL.replace("$END_MONTH",et.strftime("%m"))
   myURL = myURL.replace("$START_DAY",st.strftime("%d"))
   myURL = myURL.replace("$END_DAY",et.strftime("%d"))
   
   #print myURL   # debug
   f = opener.get(myURL)
   return processDailyInput(f.text)

def getDailyYakInput (location,pecode,lookback):
   myURL = dailyYakURL
   # example: https://www.usbr.gov/pn-bin/webarccsv.pl?parameter=ANTI%20QD&syer=2011&smnth=12&sdy=01&eyer=2011&emnth=12&edy=06&format=1
   # where, in stations.list = ANTI, QD

   et = datetime.datetime.now()
   st =  et - timedelta(days=int(lookback))
   myURL = myURL.replace("$LOC_ID",location)
   myURL = myURL.replace("$PE_CODE",pecode)
   myURL = myURL.replace("$START_YEAR",st.strftime("%Y"))
   myURL = myURL.replace("$END_YEAR",et.strftime("%Y"))
   myURL = myURL.replace("$START_MONTH",st.strftime("%m"))
   myURL = myURL.replace("$END_MONTH",et.strftime("%m"))
   myURL = myURL.replace("$START_DAY",st.strftime("%d"))
   myURL = myURL.replace("$END_DAY",et.strftime("%d"))
   
   #print myURL   # debug
   f = opener.get(myURL)
   return processDailyInput(f.text)

def getDaily (lookback,stalistPath):
  alias = readAliasFile("/data/ldad/NWRFC/control_files/daily.alias")
  stalist = readTSV(stalistPath)
  for line in stalist:
    if line[1] in alias:
      t = alias[line[1]] #temporary variable with alias info

      input = getDailyInput (line[0],line[1],lookback)
      
      for n in input:
        if n[1] in [ "998877.00n" ]  :
            n[1] = "-9999000"
        n[1] = stripGarbage(n[1])
        if t[0] in ["LS","QR","QI","QD","QT","QU","QP"]: #BUG FIX: SHEFIT -2 can't handle large numbers so we convert all LS to kaf
          n[1] = div1000(n[1])
        print(makeDailySHEF(line[0],n[0],line[2],t[0]+t[1],n[1]))

def getDailyYak (lookback,stalistPath):
  alias = readAliasFile("/data/ldad/NWRFC/control_files/daily.alias")
  stalist = readTSV(stalistPath)
  for line in stalist:
    if line[1] in alias:
      t = alias[line[1]] #temporary variable with alias info

      input = getDailyYakInput (line[0],line[1],lookback)
      
      for n in input:
        if n[1] in [ "998877.00n" ]  :
            n[1] = "-9999000"
        n[1] = stripGarbage(n[1])
        if t[0] in ["LS","QR","QI","QD","QT","QU","QP"]: #BUG FIX: SHEFIT -2 can't handle large numbers so we convert all LS to kaf
          n[1] = div1000(n[1])
        print(makeDailySHEF(line[0],n[0],line[2],t[0]+t[1],n[1]))

#====================================================
# bbaley 12-06-2011, edited by henry 2018 Feb
#====================================================  
def getRealtimeInput (location, pecode, lookback):
   myURL = realtimeURL
   # example multi: "https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=ANTI%20QD,%20ANTI%20GH&syer=2011&smnth=12&sdy=06&eyer=2011&emnth=12&edy=06&format=1"
   # example single: "https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=ANTI%20GH&syer=2011&smnth=12&sdy=06&eyer=2011&emnth=12&edy=06&format=1"
   #https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=HFAI%20GH&syer=2011&smnth=12&sdy=07&eyer=2011&emnth=12&edy=07&format=1
   # where, in stations.list = ANTI, QD
   myURL = myURL.replace("$LOC_ID",location)
   myURL = myURL.replace("$PE_CODE",pecode)
   myURL = myURL.replace("$HOURS",lookback)
   
   #print myURL   # debug
   f = opener.get(myURL)
   return processRealTimeInput(f.text)

def getRealtimeMultiInput (location, pecode_str, lookback):
   # henry: myURL definition needs to be flexible with stations with different 
   # number of parameters

   # henry: note the changes below
   # realtimeMultiURL_pre="https://www.usbr.gov/pn-bin/instant.pl?parameter="
   # realtimeMultiURL_post="&back=$HOURS&format=1"
   
   # example multi instant: https://www.usbr.gov/pn-bin/instant.pl?parameter=AND%20AF,%20AND%20FB,AND%20GH,AND%20OB,%20AND%20PC&back=2&format=1
   pecodes = pecode_str.split(",")
   
   loc_pecodes = []
   for i in pecodes:
     loc_pecodes.append(location+"%20"+i)
   
   loc_pecodes2 = ",".join(loc_pecodes)
    
   myURL = realtimeMultiURL_pre + loc_pecodes2 + realtimeMultiURL_post
   myURL = myURL.replace("$HOURS",lookback)

   #print myURL   # debug
   f = opener.get(myURL)
   return processRealTimeMultiInput(f.text)

def getRealtimeMultiLongInput (cmd_str, lookback):
  # henry: this is getting big batch request, length defined by realtimeMultiLength
  myURL = realtimeMultiURL_pre + cmd_str + realtimeMultiURL_post
  myURL = myURL.replace("$HOURS",lookback)

  #print myURL   # debug
  f = opener.get(myURL)
  return processRealTimeMultiInput(f.text) # can use same function as multiple input
  
def getRealtimeYakInput (location, pecode, lookback):
   myURL = realtimeYakURL
   # example multi: "https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=ANTI%20QD,%20ANTI%20GH&syer=2011&smnth=12&sdy=06&eyer=2011&emnth=12&edy=06&format=1"
   # example single: "https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=ANTI%20GH&syer=2011&smnth=12&sdy=06&eyer=2011&emnth=12&edy=06&format=1"
   #https://www.usbr.gov/pn-bin/webdaycsv.pl?parameter=HFAI%20GH&syer=2011&smnth=12&sdy=07&eyer=2011&emnth=12&edy=07&format=1
   # where, in stations.list = ANTI, QD
   myURL = myURL.replace("$LOC_ID",location)
   myURL = myURL.replace("$PE_CODE",pecode)
   myURL = myURL.replace("$HOURS",lookback)
   
   #print myURL   # debug
   f = opener.get(myURL)
   return processRealTimeInput(f.text)

#====================================================
def getRealtime (lookback, stalistPath):
  alias = readAliasFile("/data/ldad/NWRFC/control_files/realtime.alias")
  #alias = readAliasFile("C:/Users/henry.pai/Desktop/usbr_scrape_hp/ldad_input/original/realtime.alias")
  
  stalist = readTSV(stalistPath)
  for line in stalist:
    if line[1] in alias:
      t = alias[line[1]] #temporary variable with alias info
      
      input = getRealtimeInput (line[0],line[1],lookback)
      
      for n in input:
        if n[1] in [ "998877.00n" ]  :
            n[1] = "-9999000"
        n[1] = stripGarbage(n[1])
        if t[0] in ["LS","QR","QI","QD","QT","QU","QP"]: #BUG FIX: SHEFIT -2 can't handle large numbers so we convert all LS to kaf
          n[1] = div1000(n[1])
        print(makeRealtimeSHEF(line[0],n[0],line[2],t[0]+t[1],n[1]))

def getRealtimeMulti (lookback, stalistPath):
  # added by henry
  alias = readAliasFile("/data/ldad/NWRFC/control_files/realtime.alias")
  
  stalist = readTSV(stalistPath)

  out_file = open("C:/Users/henry.pai/Desktop/usbr_scrape_hp/output/realtimeTestMulti_shef.txt", output_style)

  for j in range(0, len(stalist)):
    # henry: because pecode fields is a string, now the alias find function
    # needs to change a little

    line = stalist[j]
    usbr_pes = line[1].split(",")
    sta = line[0]
    tz = line[2]
    web_input = getRealtimeMultiInput (sta,line[1],lookback)
    # getRealtimeMultiInput is getting by row, then by column. Pecodes will
    # repeat by column
   
    all_pes = usbr_pes * (len(web_input)/len(usbr_pes))

    for i in range(0, len(web_input)):
      usbr_pe = all_pes[i]
      if usbr_pe in alias:
        shef = alias[usbr_pe] #temporary variable with alias info

        web_line = web_input[i]

        if web_line[1] in [ "998877.00n" ]  :
          web_line[1] = "-9999000"

        if web_line[1] == "NA" or len(web_line[1]) == 0:
          pass
        else: 
          web_line[1] = stripGarbage(web_line[1])
          if shef[0] in ["LS","QR","QI","QD","QT","QU","QP"]: #BUG FIX: SHEFIT -2 can't handle large numbers so we convert all LS to kaf
            web_line[1] = div1000(web_line[1])

          #print makeRealtimeSHEF(sta,web_line[0],tz,shef[0]+shef[1],web_line[1])
          out_text = makeRealtimeSHEF(sta,web_line[0],tz,shef[0]+shef[1],web_line[1])
          out_file.write(out_text)
          out_file.write("\n")
          out_file.flush()
  out_file.close()

def getRealtimeMultiLong (lookback, stalistPath):
  # added by henry
  alias = readAliasFile("/data/ldad/NWRFC/control_files/realtime.alias")
  
  stalist = readTSV(stalistPath)

  # loop reads in and only accepts PE commands within alias file
  pe_array = []
  station_array = []
  tzone_array = []
  shef_array = []
  for i in range(0, len(stalist)):
    usbr_pe = stalist[i][1]
    if usbr_pe in alias:
      station_array.append(stalist[i][0])
      pe_array.append(usbr_pe)
      tzone_array.append(stalist[i][2])
      shef_array.append(alias[usbr_pe])
      
  # sorting by timezone, suspicious multi station call mixing stations of multiple timezones is causing error
  # code example: https://stackoverflow.com/questions/6618515/sorting-list-based-on-values-from-another-list
  # check the first comment on the most popular reply for sorting multiple arrays the same way
  station_sorted = [station_array for _, station_array in sorted(zip(tzone_array, station_array), key=lambda pair: pair[0])]
  pe_sorted = [pe_array for _, pe_array in sorted(zip(tzone_array, pe_array), key=lambda pair: pair[0])]
  shef_sorted = [shef_array for _, shef_array in sorted(zip(tzone_array, shef_array), key=lambda pair: pair[0])]
  tzone_array.sort()

  # count example: https://stackoverflow.com/questions/23240969/python-count-repeated-elements-in-the-list
  #tz_counts = dict(Counter(tzone_array))
  tzs = list(set(tzone_array)) # doesn't sort after finding unique values
  tzs.sort() # sorts unique timzones, otherwise sorted lists above would be mixed up

  # didn't have Collection
  tz_counts = []
  
  for x in range(0, len(tzs)):
    temp_counter = 0
    for y in range(0, len(tzone_array)):
      if tzone_array[y] == tzs[x]:
        temp_counter += 1
    tz_counts.append(temp_counter)

  # max command length
  max_length_temp = int(realtimeMultiLength)
  if max_length_temp > 100: # appears to be buggy when > 130
    max_length = 100
  else:
    max_length = max_length_temp

  tzone_tracker = 0

  # go through different timezones
  for j in range(0, len(tz_counts)):
    tz = tzs[j]
    tz_count = tz_counts[j]

    modulus_val = tz_count % max_length # handling last batch call, which will likely be less than max_length

    if modulus_val == 0:
      user_loop_size = tz_count/max_length
    else:
      user_loop_size = int(tz_count/max_length) + 1 # integer operation truncates

    array_start1 = tzone_tracker

    # go through number of commands
    for k in range(0,user_loop_size):
      command_array = []
      pe_array2 = []
      station_array2 = []
      tzone_array2 = []
      shef_array2 = []
      array_end = array_start1 + max_length
      if array_end > (tzone_tracker + tz_count): # accounting for last loop
        array_end = (tzone_tracker + tz_count)

      # go through every element
      for x in range(array_start1, array_end):
        station_array2.append(station_sorted[x])
        pe_array2.append(pe_sorted[x])
        tzone_array2.append(tzone_array[x])
        shef_array2.append(shef_sorted[x])
        command_array.append(station_sorted[x]+"%20"+pe_sorted[x])

      # combine command into one command  
      command_str = ",".join(command_array)  

      web_input = getRealtimeMultiLongInput(command_str, lookback)

      array_length = array_end - array_start1
      
      rows = int(len(web_input)/array_length)
      all_stations = station_array2 * rows
      all_tzones = tzone_array2 * rows
      all_shefs = shef_array2 * rows

      # evaluate output, web input should be 100 lines or less
      for y in range(0, len(web_input)):
        web_line = web_input[y]
        if web_line[1] in [ "998877.00n" ]  :
          web_line[1] = "-9999000"

        if web_line[1] == "NA" or len(web_line[1]) == 0:
          pass
        else:
          web_line[1] = stripGarbage(web_line[1])
          sta = all_stations[y]
          tz = all_tzones[y]
          shef = all_shefs[y]
          if shef[0] in ["LS","QR","QI","QD","QT","QU","QP"]: #BUG FIX: SHEFIT -2 can't handle large numbers so we convert all LS to kaf
            web_line[1] = div1000(web_line[1])

          print(makeRealtimeSHEF(sta,web_line[0],tz,shef[0]+shef[1],web_line[1]))
      array_start1 = array_end
    tzone_tracker += tz_count
  
def getRealtimeYak (lookback, stalistPath):
  alias = readAliasFile("/data/ldad/NWRFC/control_files/realtime.alias")
  stalist = readTSV(stalistPath)
  for line in stalist:
    if line[1] in alias:
      t = alias[line[1]] #temporary variable with alias info
      
      input = getRealtimeYakInput (line[0],line[1],lookback)
      
      for n in input:
        if n[1] in [ "998877.00n" ]  :
            n[1] = "-9999000"
        n[1] = stripGarbage(n[1])
        if t[0] in ["LS","QR","QI","QD","QT","QU","QP"]: #BUG FIX: SHEFIT -2 can't handle large numbers so we convert all LS to kaf
          n[1] = div1000(n[1])
        print(makeRealtimeSHEF(line[0],n[0],line[2],t[0]+t[1],n[1]))        
  
# bbaley 12-06-2011
#====================================================

# henry: debug and performance testing area
#short_test = "C:/Users/henry.pai/Desktop/usbr_scrape_hp/ldad_input/stations.realtime.list_testAll.txt"
#getRealtimeMultiLong("2", short_test)
#time.sleep(1290) # note tests indicated ~1050 run time, 1032 min/1061 max.  Using 770 to try to ensure > 1800
#getRealtimeMultiLong("2", short_test, "a")

#=============================================
#This is the entrypoint for the script
#=============================================

if len (sys.argv) > 1:
   if sys.argv[1] == "daily" and len(sys.argv) > 3:
      getDaily(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "dailyYak":
       getDailyYak(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "realtime":
      #result = getRealtime(sys.argv[2],sys.argv[3],sys.argv[4])
      getRealtime(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "realtimeMulti":
     getRealtimeMulti(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "realtimeMultiLong":
     getRealtimeMultiLong(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "realtimeYak":
       getRealtimeYak(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "json":
      getJSON(sys.argv[2],sys.argv[3])
   elif sys.argv[1] == "params":
      getParameters(sys.argv[2],sys.argv[3],sys.argv[4])
   else:
      help()
else:
   help()
complain()
