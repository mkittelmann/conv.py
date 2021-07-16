from datetime import datetime
from pprint import pprint
import subprocess, os
# import collections
# import sys, os, logging, multiprocessing
# from pathlib import Path
# from pprint import pprint

def run( file, step, ix, param, work, data):     
    expansion = {
      'p' : param, 
      'w' : work,
      'f' : os.path.basename(file), 
      't' : str(datetime.now().timestamp())
    } 
    prms = {}       
    for p in ['command']:      
        if isinstance( step['with-param'], list):
            prms[p] = [ e for e in  step['with-param']  if e['@name'] == p ]
            prms[p] = next(iter(prms[p]), {})                           ## https://stackoverflow.com/a/23003811/4237436            
        elif isinstance( step['with-param'], dict):
            prms[p] = step['with-param'] if step['with-param']['@name'] == p else {}        
        prms[p] = prms[p].get( '@value', '' )  
        prms[p] = prms[p].replace( '{$f}', expansion['f'] )
        prms[p] = prms[p].replace( '{$w}', expansion['w'] )    
        prms[p] = prms[p].replace( '{$t}', expansion['t'] )
        prms[p] = prms[p].replace( '{$f}', expansion['f'] )            
              
    try:
        completed_process = subprocess.run( prms['command'], shell=True)
    except:  
        return 0           
    return 1           