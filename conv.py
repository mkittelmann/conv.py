#!/usr/bin/env python

########################################################################
# conv.py command line tool                                            #
# using parallel execution for input files                             #
# Maike Kittelmann (kittelmann@sub.uni-goettingen.de)                  #
# based on conv.pl by Alex Jahnke (jahnke@sub.uni-goettingen.de)       #
# based on CMA Metadata Aggregator (Oct. 2008)                         #
# last change: 2021-06-29                                              #
########################################################################

## call with
## python conv.py -q .\data -p .\param -w .\work -i test.xml

import argparse, sys, os, logging, multiprocessing, logzero
import xmltodict
from functools import partial
from pathlib import Path
from pprint import pprint
from datetime import datetime
from logzero import logger  ##logzero.json() ## for log as json
## application specific modules
import conv.Config
import conv.FalconConvert 
import conv.FalconIndex 
import conv.FcvTemplate
import conv.Copy 
import conv.BaseX
import conv.BaseX_Import
import conv.System
# import conv.SysMerge

########################################################################
###                          functions                               ###
########################################################################

def run_workflow(path, workflow, param, work, data):
    if os.path.isfile( path ):
        file = os.path.abspath(path)  ### str() path is object not string          
        logger.info(f"Workflow with {str(len(workflow))} step{'s' if len(workflow) > 1 else ''} ready for processing of {file}.")                        
        for ix, step in enumerate(workflow):
            return_code = run_module( file, step, str(ix), param, work, data )          
            if return_code == 1:
                logger.info(f"Step {str(ix + 1)} {step['@name']} completed successfully for {file}.")                
                continue
            else:
                logger.error(f"Step {str(ix + 1)} {step['@name']} not completed for {file}.")                
        
def run_module( file, step, ix, param, work, data):
    module = step['@name']
    return eval( 'conv.' + module + '.run( file, step, ix, param, work, data)' )       ## run respective module    

########################################################################
###                           main                                   ###
########################################################################
if __name__ == "__main__":
    start = datetime.now().replace(microsecond=0)       
    parser=argparse.ArgumentParser()
    parser.add_argument('-q', '-queue', help='', dest='inputdir')    ## https://stackoverflow.com/questions/28638813/how-to-make-a-short-and-long-version-of-a-required-argument-using-python-argpars            
    parser.add_argument('-w', '-work', help='', dest='workdir', required=True)   
    parser.add_argument('-p', '-param', help='', dest='paramdir')   
    parser.add_argument('-i', '-instructions', help='', dest='xmlfile', required=True )   
    parser.add_argument('-b', '-begin', help='Step from which to start (not 0-based)', dest='begin')
    parser.add_argument('-e', '-end', help='Number of steps to perform', dest='end')
    parser.add_argument('-d', '-die', help='', dest='die')
    parser.add_argument('-v', '-verbose', help='', dest='verbose')
    parser.add_argument('-l', '-library', help='', default='$Conv::Config::MODULE_LIBRARY', dest='lib')  ## TODO
    args=parser.parse_args()  
    try:
        logzero.logfile(".\conv.log")                        
    except:
        print( "Unable to source log file\n", sys.exc_info()[0] )
    logger.info('Converter process started.')
    ## dirs      
    work_dir_in_str=args.workdir
    param_dir_in_str=args.paramdir   
    data_dir_in_str=args.inputdir  
    instr_as_str=args.xmlfile      
    logger.info('Preparing to perform workflow: ' + instr_as_str)
    ## instructions xml
    try:
        xml_as_string = Path(instr_as_str).read_text() ## TODO xmltodict erkennt keine Kommentare
    except IOError as err:
        logger.error( f"I/O error reading XML instructions.\n{err.strerror}" )
        sys.exit()
    except:
        logger.error( f"Unexpected error reading XML instructions.\n{ sys.exc_info()[0] }")
        sys.exit()
    try:
        xml_as_dict = xmltodict.parse(xml_as_string, dict_constructor=dict)
    except:
        logger.error( f"Unexpected error parsing XML instructions. Malformed XML.\n{sys.exc_info()[0]}" )
        sys.exit()
    try:  
        wf = xml_as_dict['conversion']['workflow']['module']  
        if not wf:
            logger.error("Invalid XML") 
        if isinstance( wf, dict):  
            wf = [ wf ]        
    except:
        logger.error( f"Unexpected error accessing XML instruction modules. Invalid XML.\n{sys.exc_info()[0]}" )    
        sys.exit()    
    
    ## begin / end  ## nicht 0-basiert
    b = int(args.begin) -1 if args.begin else 0
    e = b + int(args.end) if args.end else len(wf) - b
    ## apply begin / end 
    wf = wf[b:e]
        
    ## execute workflow on each path in pathlist    
    pathlist = Path(data_dir_in_str).rglob('*.*')                
    run_workflow_partial = partial(run_workflow, workflow=wf, param=os.path.abspath(param_dir_in_str), work=os.path.abspath(work_dir_in_str), data=os.path.abspath(data_dir_in_str))       
    with multiprocessing.Pool() as pool:
        results = pool.map(run_workflow_partial, pathlist) 
        results = [x for x in results if x is not None ]
        results = [x for x in results if x != 0 ]
    ## TODO     
    # if results:        
        # results
    logger.info('Converter process completed. Process took ' + str(datetime.now().replace(microsecond=0)-start))
