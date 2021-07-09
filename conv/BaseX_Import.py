import conv.Config as config
from datetime import datetime
import conv.Config as config
from pprint import pprint
import subprocess, re, os, sys
from shutil import copyfile

def run( file, step, ix, param, work, data):            
    pattern = config.getPattern()
    expansion = {
      'p' : param, 
      'w' : work,
      'f' : os.path.basename(file), 
      't' : str(datetime.now().timestamp())
    } 
    prms = {}   
    for p in ['db', 'input', 'processor']:        
        prms[p] = [ e for e in step['with-param'] if e['@name'] == p ]
        print(prms[p])
        prms[p] = next(iter(prms[p]), {})                           ## https://stackoverflow.com/a/23003811/4237436
        default = file if p == 'input' else ''
        prms[p] = prms[p].get( '@value', default )  
        pprint(prms)
        if prms[p] == '' and p == 'db':
            print( 'Missing mandatory argument ', p, ' in step ', str(ix), ': ', __name__  )
            sys.exit()                
        match = pattern.search( prms[p] )    
        if match:            
            prms[p] = os.path.join( expansion[ match.group(1) ], os.path.basename( prms[p] ))        
        elif prms[p] == file:
            prms[p] = os.path.join(work, os.path.basename(prms[p]))  ## file schon expandiert wg. rglob pathlist       
            if not os.path.isfile(os.path.abspath(prms[p])):         ## copy unless exists
                 copyfile( file, os.path.abspath(prms[p]))                          
        elif p == 'input':
            prms[p] = os.path.join( work, os.path.basename( prms[p] ))  
        prms[p] = prms[p].replace( '{$t}', expansion['t'] )
        prms[p] = prms[p].replace( '{$f}', expansion['f'] )     
    ## prms['processor'] if prms['processor'] != '' else 
    processor_path = os.path.abspath( config.getBaseXProcessor() )
    isCSV = prms.get( 'isCSV', '' ) if prms.get( 'isCSV', '' ) != '' else False
    ifIsCsv = " -c\"SET CSVPARSER encoding=utf-8,header=true,lax=false,quotes=false,separator=tab\" "    
    isJSON = prms.get( 'isJSON', '' ) if prms.get( 'isJSON', '' ) != '' else False
    ifIsJSON = ' -c"set PARSER json" -c"SET CREATEFILTER *.json" -c"SET JSONPARSER encoding=utf-8,lax=false" '
    parser = ifIsCsv if isCSV else '' 
    command = f"\"{processor_path}\" -c\"check {prms['db']}\" {parser} -c\"add {prms['input']}\" "       
    print( command )   
    try:
      completed_process = subprocess.run( command )   
      print(completed_process.returncode)      
      return completed_process.returncode    
    except IOError:
      return 1  
    return 1      
    
# db 	verpflichtend 		Name der BaseX-Datenbank. Wenn die DB nicht existiert, wird sie erzeugt. Die Datenbank darf nicht anderweitig in BaseX geöffnet sein (z.B. im GUI).
# xq 	verpflichtend 		Name (u. ggf. Pfad) der XQuery-Datei
# output 	verpflichtend 		Name der Ausgabedatei
# processor 	optional 		Abweichender Pfad zur basex.bat. Standard ist „C:\Program Files (x86)\BaseX\bin\basex.bat“.     