#!/awips2/python/bin/python
import datetime, os, re, sys, time, requests
from datetime import timedelta
from requests.packages.urllib3.exceptions import InsecureRequestWarning

helpStr = '''
get_usbr_webdata v1.4.0
12 December 2016
POC: gunnar.a.leffler@usace.army.mil

USAGE:
get_usbr_webdata {daily|realtime} <lookback window> <stationlist>

EXAMPLES:
get last 2 days hours of realtime data:
  get_usbr_webdata realtime 24h stations.realtime.list
  get_usbr_webdata realtime 2   stations.list

get last weeks worth of daily data:
  get_usbr_webdata daily 1w stations.daily.list
  get_usbr_webdata daily 7  stations.list
'''

service = {    "daily":"daily.pl",
            "realtime":"instant.pl",
           "realtime2":"webdaycsv.pl",
          }

nodataset = { "MISSING" : 0,
            "NO RECORD" : 0,
                    ""  : 0
            }

debug = False

def div1000( s ):
  output = ""
  try:
    output = str( float( s ) / 1000 )
  except:
    pass
  return output
  
def help ():
  print( helpStr)

def readAliasFile( path ): #reads an alias file and returns a dictionary
  csv = readTSV( path )
  alias = []
  for line in csv:
    alias.append( ( line.pop( 0 ), line ) )
  return dict( alias )

def readTSV( path ):
  lines = ( line.rstrip( '\n' ) for line in open( path, "r" ) )
  output = []
  for s in lines:
    if len(s) > 1 and s[0] != '#':              # ignore blank lines
      row1 = s.split( '\t' )
      output.append( row1 )
  return output

def TD (input):
  '''TD takes a relative time and turns it into a timedelta
  input format: 1w7d6h9m'''
  input = input.lower()
  output = datetime.timedelta(seconds = 0)
  t = ""
  try:
    for c in input:
      if c =="w":
        output += datetime.timedelta(weeks=float(t))
        t = ""
      elif c =="Y":
        output += datetime.timedelta(days=float(t)*365)
        t = ""
      elif c =="d":
        output += datetime.timedelta(days=float(t))
        t = ""
      elif c =="h":
        output += datetime.timedelta(hours=float(t))
        t = ""
      elif c =="m":
        output += datetime.timedelta(minutes=float(t))
        t = ""
      else:
        if c != " ":
          t += c
    if output.total_seconds() == 0: #defaulting to days
      output += datetime.timedelta(days=float(t))
  except:
    status = "Could not parse"+input+"3 into a time interval, defaulting to 3 days"
    output = datetime.timedelta(days=7)
  return output


# Removes cruft from scraped input
def stripGarbage( input ):
  output = ""
  if input[0] == "-":
    output = "-"
  for c in input:
    if c.isdigit() or c == ".":
      output += c
  return output

def processInput( type, buffer ):
  format = {    "daily":"%m/%d/%Y",
             "realtime":"%m/%d/%Y %H:%M" }
  lines = buffer.split( '\n' )
  flag = 0
  output = []
  errline = ""
  for s in lines:
    s = s.strip()
    if "END DATA" in s:
      flag = 0
    if len(s) > 1 and flag > 1:         #if line not blank or header / footer
      try:
        tokens = s.split(',')
        tokens[1] = tokens[1].strip()
        tokens[1].replace("Edited","") #USBR's new webservice appends "Edited" to QCd data 
        if tokens[1] not in nodataset:
          output.append([datetime.datetime.strptime(tokens[0], format[type]),tokens[1]])
        else :
          if debug == True: print >> sys.stderr, errline+"\t"+s
      except:
        pass
    if "BEGIN DATA" in s:
      flag = 1
    if "DATE" in s or "DATE       TIME" in s and flag == 1:
      errline = s
      flag += 1
  return output

def makeSHEF( type, locID, timeObj, tz, PEcode, value ):
  output = ".A " + locID + " " + timeObj.strftime( "%Y%m%d" ) + " " + tz
  if type == "daily":
    output += " DH24/"
  elif type == "realtime":
    output += " DH" + timeObj.strftime( "%H%M" ) + "/DUE /"
  output += PEcode + " " + value
  return output

def populateURL( type, location, pecode, lookback ):
  et = datetime.datetime.now()
  #st = et - timedelta( days = int( lookback ) )
  st = et - TD( lookback )
  url = ( "https://www.usbr.gov/pn-bin/%s?parameter=%s%%20%s&syer=%s&"
          "smnth=%s&sdy=%s&eyer=%s&emnth=%s&edy=%s" % ( service[type], 
          location,
          pecode,
          st.strftime( "%Y" ),
          st.strftime( "%m" ), 
          st.strftime( "%d" ),
          et.strftime( "%Y" ),
          et.strftime( "%m" ), 
          et.strftime( "%d" ) ) )
  if debug == True: print >> sys.stderr, url
  return url

def getData( type, lookback, station_file ):
  alias = readAliasFile( type + ".alias" )
  for line in readTSV( station_file ):
    try:
      station, param, tz = line[0:3]
    except:
      station, param = line[0:2]
      tz = "P" 
    if len(tz) != 1: tz = "P"
    if param in alias:
      pe, dtsep = alias[param][0:2]
  
      url = populateURL( type, station, param, lookback )
      try:
        input = processInput( type, requests.get( url, verify = False,timeout=5).text )
      except:
         input = []
      if len (input) < 1 and type == "realtime":
        url =url.replace(service["realtime"],service["realtime2"])
        if debug == True:
          print >> sys.stderr, "No data found trying alternate service:\n"+url
        try:
          input = processInput( type, requests.get( url, verify = False, timeout=5 ).text )
        except:
          input = []

      for n in input:
        timestamp, value = n[0:2]
        value = stripGarbage( value )

        # SHEFIT -2 can't handle large numbers, so convert LS from af to kaf
        if pe == "LS":
          value = div1000( value )

        print (makeSHEF( type, station, timestamp, tz, pe + dtsep, value ))

###############################################################################
# Entry Point

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
if len( sys.argv ) > 4:
  if sys.argv[4] == "debug": debug = True

if len( sys.argv ) > 3:
  type, lookback, station_file = sys.argv[1:4]
  if re.match( r'(daily|realtime)', type ):
    getData( type, lookback, station_file )
  else:
    help()
else:
   help()
