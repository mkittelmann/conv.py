from datetime import datetime
from pprint import pprint
import subprocess, re, os, sys
from shutil import copyfile
# import collections
# import sys, os, logging, multiprocessing
# from pathlib import Path
# from pprint import pprint

def run( file, step, ix, param, work):
    pprint(step)      
    file = '.\\' + file    ## schon expandiert wg. rglob pathlist
    input_list = [ p for p in step['with-param'] if p['@name'] == 'input' ]
    if len(input_list) != 0:
        input = input_list[0].get( '@value', file ) 
    else:
        input = file
    print( os.path.basename( file ) )
    if file == os.path.basename( file ):
        file =  work + '\\' + file
        print(file)
    input_file_in_work_dir = input.replace('.\data', '.\work')    ## TODO variables
    copyfile(input, input_file_in_work_dir)   
    format_list = [ p for p in step['with-param'] if p['@name'] == 'format' ]
    if len(format_list) != 0:
        format = format_list[0].get( '@value', 0 ) 
    else:
        print( 'Missing mandatory argument #format#' )
        sys.exit()        
    script_list = [ p for p in step['with-param'] if p['@name'] == 'script' ]
    if len(script_list) != 0:
        script = script_list[0].get( '@value', 0 ) 
    else:
        print( 'Missing mandatory argument #script#' )
        sys.exit()
    output_list = [ p for p in step['with-param'] if p['@name'] == 'output' ]
    if len(output_list) != 0:
        output = output_list[0].get( '@value', 0 )
    else:
        print( 'Missing mandatory argument #output#' )
        sys.exit()        
    ## path expansions 
    switch = {
      'p' : param, 
      'w' : work,
      'f' : file, 
      't' : datetime.now().timestamp()
    } 
    ## TODO: Was ist denn, wenn mehrere matchen und ersetzt werden müssen?    
    query = '\{\$([wpft])\}[\/\\\\](.+)$'
    pattern = re.compile(r"{}".format(query), re.IGNORECASE)   
    m = pattern.search( script )
    if m:
        expansion = switch[ m.group(1).strip() ] + '\\' + m.group(2).strip() 
        script = expansion
    m = pattern.search( param )
    if m:
        expansion = switch[ m.group(1).strip() ] + '\\' + m.group(2).strip() 
        param = expansion
    m = pattern.search( input )    
    if m:
        expansion = switch[ m.group(1).strip() ] + '\\' + m.group(2).strip() 
        input = expansion           
    ## run sys process
    
    input_file_in_work_dir = os.path.abspath( input_file_in_work_dir )
    script = os.path.abspath( param + '\\' + script )
    output = os.path.abspath( work + '\\' + output )
    script = os.path.abspath( script )
    # while not os.path.isfile(input_file_in_work_dir):    
    command = f"\"C:\PortablePrograms\Falcon\Falcon.exe\" -d {input_file_in_work_dir} -f {format} -e {script}/{output} -x1 -h1"
    try:
      completed_process = subprocess.run( command )   
      print(completed_process.returncode)      
      return completed_process.returncode    
    except Error:
      return 1  
    return 1
    