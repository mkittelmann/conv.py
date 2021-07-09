import conv.Config as config
from datetime import datetime
from pprint import pprint
import subprocess, re, os, sys
from shutil import copyfile
# import collections
# import sys, os, logging, multiprocessing
# from pathlib import Path
# from pprint import pprint

def run( file, step, ix, param, work, data):        
    falcon_path = config.getFalconPath()
    pattern = config.getPattern()            
    # pprint( step )        
    ## variable expansions 
    expansion = {
      'p' : param, 
      'w' : work,
      'f' : os.path.basename(file), 
      't' : str(datetime.now().timestamp())
    } 
    prms = {}   
    for p in ['input', 'script', 'format', 'output']:      
        prms[p] = [ e for e in step['with-param'] if e['@name'] == p ]
        prms[p] = next(iter(prms[p]), {})                           ## https://stackoverflow.com/a/23003811/4237436
        default = file if p == 'input' else ''
        prms[p] = prms[p].get( '@value', default )  
        if prms[p] == '':
            print( 'Missing mandatory argument ', p, ' in step ', str(ix) )
            sys.exit()                            
        if prms[p] == file:
            prms[p] = os.path.join(work, os.path.basename(prms[p]))  ## file schon expandiert wg. rglob pathlist       
            if not os.path.isfile(os.path.abspath(prms[p])):         ## copy unless exists
                copyfile( file, os.path.abspath(prms[p]))               
    for p in ['input', 'script', 'output']:       
        match = pattern.search( prms[p] )    
        if match:            
            prms[p] = os.path.join( expansion[ match.group(1) ], os.path.basename( prms[p] ))        
        else:
            prms[p] = os.path.join( work, prms[p])
        prms[p] = prms[p].replace( '{$t}', expansion['t'] )
        prms[p] = prms[p].replace( '{$f}', expansion['f'] )
    # print(prms)    
    command = f"\"{falcon_path}\" -d {prms['input']} -f {prms['format']} -e {prms['script']}/{prms['output']} -x1 -h1"
    try:
      completed_process = subprocess.run( command )   
      print(completed_process.returncode)      
      return completed_process.returncode    
    except IOError:
      return 1  
    return 1    