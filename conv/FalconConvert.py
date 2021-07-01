from datetime import datetime
from pprint import pprint
import subprocess, re
# import collections
# import sys, os, logging, multiprocessing
# from pathlib import Path
# from pprint import pprint

def run( file, step, ix, param, work):
    epoch = datetime.now().timestamp()
    # pprint(step)      
    file = '.\\' + file    
    input_list = [ p for p in step['with-param'] if p['@name'] == 'input' ]
    if len(input_list) != 0:
        input = input_list[0].get( '@value', file ) 
    else:
        input = file
    print(input)
    
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

    # print( file )   ### schon expandiert
    # print( script )
    # print( work )
    
    switch = {
      'p' : param, 
      'w' : work,
      'f' : file, 
      't' : epoch
    }    
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
    
    
    # for v in [ script, output, format ]:
        # m = pattern.search( v )
        # if m:
            # expansion = switch[ m.group(1).strip() ] + '\\\\' + m.group(2).strip() 
            # v = expansion
            # print(expansion)    
    # print( script )
    # print( param )    
  
    command = f"\"C:\PortablePrograms\Falcon\Falcon.exe\" -d {file} -f {format} -e {script}\{output} -x1 -h1"
    print( command )   
    completed_process = subprocess.run( command )    
    print(completed_process.returncode)
        # return 
        
    # if len(input) != 0:
        # pprint(  input[0].get( '@value', 0 ) )
    # if len(format) != 0:
        # pprint(  format[0].get( '@value', 0 ) )
    # if len(script) != 0:
        # pprint(  script[0].get( '@value', 0 ) )
    # if len(output) != 0:



    ## for s in [ 'input', 'script', 'output', 'format' ]:

      
    1   
    # else:
        # return 0
    