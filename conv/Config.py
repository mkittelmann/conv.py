import re, os

def getFalconPath():        
    return 'C:\PortablePrograms\Falcon\Falcon.exe'
    
def getPattern():
    query = '^\{\$([wpft])\}[\/\\\\](.+)$'    ## matcht nur Pfadvariablen
    return re.compile(r"{}".format(query), re.IGNORECASE) 
    
# full path to application wide logfile
def getLogFile():
    return "./logs/conv.log"

# path to xquery processor
def getBaseXProcessor():
    return 'C:\\Program Files (x86)\\BaseX\\bin\\basex.bat'

# path to xslt processor
def getXSLTProcessor():
    return 'c:/local/saxon/saxonb9-1-0-1j/saxon9.jar'    