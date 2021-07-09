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
    for p in ['db', 'xq', 'output', 'processor']:        
        prms[p] = [ e for e in step['with-param'] if e['@name'] == p ]
        prms[p] = next(iter(prms[p]), {})                           ## https://stackoverflow.com/a/23003811/4237436
        prms[p] = prms[p].get( '@value', '' )  
        if prms[p] == '' and p != 'processor':
            print( 'Missing mandatory argument ', p, ' in step ', str(ix), ': ', __name__  )
            sys.exit()                
        match = pattern.search( prms[p] )    
        if match:            
            prms[p] = os.path.join( expansion[ match.group(1) ], os.path.basename( prms[p] ))        
        elif p == 'output':
            prms[p] = os.path.join( work, os.path.basename( prms[p] ))  
        prms[p] = prms[p].replace( '{$t}', expansion['t'] )
        prms[p] = prms[p].replace( '{$f}', expansion['f'] ) 
    pprint(prms)
    ## prms['processor'] if prms['processor'] != '' else 
    processor_path = os.path.abspath( config.getBaseXProcessor() )
    command = f"\"{processor_path}\" -c\"check {prms['db']}\" -c\"run {prms['xq']}\" > \"{prms['output']}\""  
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