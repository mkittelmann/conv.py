import conv.Config as config
from datetime import datetime
import conv.Config as config
from pprint import pprint
import subprocess, re, os, sys, logzero
from shutil import copyfile
from logzero import logger  

def run( file, step, ix, param, work, data):            
    pattern = config.getPattern()
    logger.debug( f"Module {sys.modules[__name__]} received step-parameters {step}" )          
    expansion = {
      'p' : param, 
      'w' : work,
      'f' : os.path.basename(file), 
      't' : str(datetime.now().timestamp())
    } 
    prms = {}     
    for p in ['db', 'input', 'processor']:       
        if isinstance( step['with-param'], list):
            prms[p] = [ e for e in  step['with-param']  if e['@name'] == p ]
            prms[p] = next(iter(prms[p]), {})                           ## https://stackoverflow.com/a/23003811/4237436            
        elif isinstance( step['with-param'], dict):
            prms[p] = step['with-param'] if step['with-param']['@name'] == p else {}
        default = file if p == 'input' else ''
        prms[p] = prms[p].get( '@value', default )  
        if p == 'db' and prms[p] == '':                             ## only db is mandatory
            print( 'Missing mandatory argument ', p, ' in step ', str(ix), ': ', __name__  )
            sys.exit()                
        match = pattern.search( prms[p] )    
        if match:            
            prms[p] = os.path.join( expansion[ match.group(1) ], os.path.basename( prms[p] ))        
        elif prms[p] == file:
            prms[p] = os.path.join(work, os.path.basename(prms[p]))  ## file schon expandiert wg. rglob pathlist       
            if not os.path.isfile(os.path.abspath(prms[p])):         ## copy unless exists
                 copyfile( file, os.path.abspath(prms[p]))                            
        prms[p] = prms[p].replace( '{$t}', expansion['t'] )
        prms[p] = prms[p].replace( '{$f}', expansion['f'] )     
    ## prms['processor'] if prms['processor'] != '' else 
    processor_path = os.path.abspath( config.getBaseXProcessor() )
    isCSV = prms.get( 'isCSV', '' ) if prms.get( 'isCSV', None ) else False
    ifIsCsv = " -c\"SET CSVPARSER encoding=utf-8,header=true,lax=false,quotes=false,separator=tab\" "    
    isJSON = prms.get( 'isJSON', '' ) if prms.get( 'isJSON', None ) else False
    ifIsJSON = ' -c"set PARSER json" -c"SET CREATEFILTER *.json" -c"SET JSONPARSER encoding=utf-8,lax=false" '
    parser = ifIsCsv if isCSV else '' 
    command = f"\"{processor_path}\" -c\"check {prms['db']}\" {parser} -c\"add {prms['input']}\" "     
    logger.debug(f"Module {sys.modules[__name__]} executes command: {command}" )    
    try:
      completed_process = subprocess.run( command )    
      # return completed_process.returncode   
    except: 
        logger.error(f"Module {sys.modules[__name__]} command did not complete: {command}" )    
        return 0           
    return 1           
        
