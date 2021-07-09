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

##TODO
import conv.Config;
# # use Conv::Logfile;
import argparse, sys, os, logging, multiprocessing
import xmltodict
from functools import partial
from pathlib import Path
from pprint import pprint
from datetime import datetime
## application specific modules
import conv.FalconConvert 
import conv.FalconIndex 
import conv.FcvTemplate
import conv.Copy 
import conv.BaseX
import conv.BaseX_Import
# import conv.System
# import conv.SysMerge

########################################################################
###                          functions                               ###
########################################################################

def run_workflow(path, workflow, param, work, data):
    if os.path.isfile( path ):
        file = os.path.abspath(path)  ### str() path is object not string        
        for ix, step in enumerate(workflow):
            return_code = run_module( file, step, str(ix), param, work, data )
            # pprint( return_code )
            if return_code == 0:
                continue
            else:
                print( 'Step not processed orderly.' + "\n\n\n\n") #TODO error message
        
def run_module( file, step, ix, param, work, data):
    module = step['@name']
    ## run respective module
    return eval( 'conv.' + module + '.run( file, step, ix, param, work, data)' )   

########################################################################
###                           main                                   ###
########################################################################
if __name__ == "__main__":
    start = datetime.now().replace(microsecond=0)       
    ## process command line options
    parser=argparse.ArgumentParser()
    # group = parser.add_mutually_exclusive_group(required=True)
    # group.add_argument('--foo',action=.....)
    # group.add_argument('--bar',action=.....)    
    parser.add_argument('-queue', help='')
    parser.add_argument('-q', help='')
    parser.add_argument('-work', help='' )
    parser.add_argument('-w', help='')
    parser.add_argument('-param', help='')
    parser.add_argument('-p', help='')
    parser.add_argument('-instructions', help='')
    parser.add_argument('-i', help='')
    parser.add_argument('-begin', help='')
    parser.add_argument('-b', help='')
    parser.add_argument('-end', help='')
    parser.add_argument('-e', help='')
    parser.add_argument('-die', help='')
    parser.add_argument('-d', help='')
    parser.add_argument('-verbose', help='')
    parser.add_argument('-v', help='')
    parser.add_argument('-step', help='')
    parser.add_argument('-s', help='')
    parser.add_argument('-library', help='', default='$Conv::Config::MODULE_LIBRARY')
    parser.add_argument('-l', help='')
    args=parser.parse_args()  
    ## TODO: add error handling    
    ## dirs
    work_dir_in_str=args.work or args.w
    param_dir_in_str=args.param or args.p
    data_dir_in_str=args.queue or args.q    
    instr_as_str=args.instructions or args.i      
    print(instr_as_str)
    ## instructions xml
    xml_as_string = Path(instr_as_str).read_text() ## TODO xmltodict erkennt keine Kommentare
    xml_as_dict = xmltodict.parse(xml_as_string, dict_constructor=dict)
    wf = xml_as_dict['conversion']['workflow']['module']    
    
    ## execute workflow on each path in pathlist    
    pathlist = Path(data_dir_in_str).rglob('*.*')                
    run_workflow_partial = partial(run_workflow, workflow=wf, param=os.path.abspath(param_dir_in_str), work=os.path.abspath(work_dir_in_str), data=os.path.abspath(data_dir_in_str))       
    with multiprocessing.Pool() as pool:
        results = pool.map(run_workflow_partial, pathlist) 
        results = [x for x in results if x is not None ]
        results = [x for x in results if x != 0 ]
    # sys.exit()        
    # if results:        
        # results
    print('This took in total: ')
    print(datetime.now().replace(microsecond=0)-start) 
