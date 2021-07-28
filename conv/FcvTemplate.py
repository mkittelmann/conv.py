import conv.Config as config
from datetime import datetime
from pprint import pprint
import subprocess, re, os, sys
from shutil import copyfile
from logzero import logger  
# import collections
# import sys, os, logging, multiprocessing
# from pathlib import Path
# from pprint import pprint

def run( file, step, ix, param, work, data):        
    falcon_path = config.getFalconPath()
    pattern = config.getPattern()
    logger.debug( f"Module {sys.modules[__name__]} received step-parameters {step}" )          
    expansion = {
      'p' : param, 
      'w' : work,
      'f' : os.path.basename(file), 
      't' : str(datetime.now().timestamp()),
      'c' :  os.getcwd()
    } 
    prms = {}   
    for p in ['script', 'path', 'path_to_file']:        
        prms[p] = [ e for e in step['with-param'] if e['@name'] == p ]
        prms[p] = next(iter(prms[p]), {})                           ## https://stackoverflow.com/a/23003811/4237436
        default = work if p == 'path' else ''                       ## default for path is work
        prms[p] = prms[p].get( '@value', default )  
        if prms[p] == '' and p != 'path_to_file':
            print( 'Missing mandatory argument ', p, ' in step ', str(ix), ': ', __name__  )
            sys.exit()                
        match = pattern.search( prms[p] )                           ## falls p für path to file verwendet wird
        if match:            
            prms[p] = os.path.join( expansion[ match.group(1) ], os.path.basename( prms[p] ))              
        prms[p] = prms[p].replace( '{$t}', expansion['t'] )
        prms[p] = prms[p].replace( '{$f}', expansion['f'] )               
    
    prms['path'] = prms['path'].replace( '{$p}', expansion['p'] )
    source = os.path.join( prms['path'], prms['script'] + '.tmpl' )
    target = os.path.join( work, prms['script'] + '.fcv' )
    logger.debug(f"Module {sys.modules[__name__]} executes command: copyfile({source}, {target})" )    
    try: 
        copyfile( source, target)   
        
        for prm in [ e for e in step['with-param'] if e['@name'] not in ['script', 'path'] ]:        
            print(prm)
            name = prm.get( '@name', '' ) 
            value = prm.get( '@value', '' ) 
            match = pattern.search( value )                              ## falls p für path to file verwendet wird
            if match:            
                prms[name] = os.path.join( expansion[ match.group(1) ], os.path.basename( value ))               
            prms[name] = value.replace( '{$ww}', work + '\\'  )     
            txt = ''
            with open(target, 'r') as f:
                txt = f.read()
            with open(target, 'w') as f:
                txt = txt.replace( f'[% {name} %]', prms[name] )
                f.write( txt )   
    except IOError as err:
        logger.error( f"Module {sys.modules[__name__]} I/O error:\n{err.strerror}" )
        sys.exit()
    except: 
        logger.error(f"Module {sys.modules[__name__]} did not complete Template rewrite:\n{ sys.exc_info()[0] }" )       
        return 0
    return 1
    
    
    
    
    
    