#!/usr/bin/env python

########################################################################
# conv.py command line tool                                            #
# using parallel execution for input files                             #
# Maike Kittelmann (kittelmann@sub.uni-goettingen.de)                  #
# based on conv.pl by Alex Jahnke (jahnke@sub.uni-goettingen.de)       #
# based on CMA Metadata Aggregator (Oct. 2008)                         #
# last change: 2021-06-29                                              #
########################################################################

# # use Conv::Instructions;
# # use Conv::Config;
# # use Conv::Logfile;
import argparse, sys, os, logging, multiprocessing
import xmltodict
from functools import partial
from pathlib import Path
from pprint import pprint
from datetime import datetime
## application specfic modules
import conv.FalconConvert 
import conv.FalconIndex 
import conv.FcvTemplate
import conv.Copy 
import conv.System
import conv.SysMerge

########################################################################
###                          functions                               ###
########################################################################

def run_workflow(path, workflow, param, work):
    file = str(path)  ### because path is object not string        
    for ix, step in enumerate(workflow):
        run_module( file, step, ix, param, work )
        
def run_module( file, step, ix, param, work):
    module = step['@name']
    ## run respective module
    exec( 'conv.' + module + '.run( file, step, ix, param, work)')         

########################################################################
###                           main                                   ###
########################################################################
if __name__ == "__main__":
    ## process command line options
    parser=argparse.ArgumentParser()
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
    print( args )
    
    ## add error handling
    
    ## instructions 
    instr_in_str=args.instructions or args.i
    # print(instr_in_str)       
    xml_as_string = Path('test.xml').read_text() ## TODO
    xml_as_dict = xmltodict.parse(xml_as_string, dict_constructor=dict)
    wf = xml_as_dict['conversion']['workflow']['module']      
    
    ## directories
    work_dir_in_str=args.work or args.w
    print(work_dir_in_str)    
    param_dir_in_str=args.param or args.p
    print(param_dir_in_str)
    data_dir_in_str=args.queue or args.q
    print(data_dir_in_str)    
    
    pathlist = Path(data_dir_in_str).rglob('*.*')   
    run_workflow_ = partial(run_workflow, workflow=wf, param=param_dir_in_str, work=work_dir_in_str)    
    start = datetime.now().replace(microsecond=0)
        
    ## execute workflow on each path in pathlist
    with multiprocessing.Pool() as pool:
        results = pool.map(run_workflow_, pathlist) 
        results = [x for x in results if x is not None ]
        results = [x for x in results if x != 0 ]
    # sys.exit()        
    # if results:        
        # results
    print('This took in total: ')
    print(datetime.now().replace(microsecond=0)-start) 





# import Conv:Instructions
# import Conv::Config
# import Conv::Logfile
# # my $opt = Getopt::Lucid->getopt( \@option_spec );
# # my $queue = add_slash($opt->get_queue)       || undef;
# # my $work  = add_slash($opt->get_work)        || die "Missing mandatory command line argument -w";
# # my $instr = $opt->get_instruction            || die "Missing mandatory command line argument -i";
# # my $lib   = $opt->get_library                || $Conv::Config::MODULE_LIBRARY;
# # my $pdir  = $opt->get_param                  || undef;
# # my $dof   = $opt->get_die                    || undef;
# # my $talk  = $opt->get_verbose                || undef;
# # my $step  = $opt->get_step                   || undef;
# # my $stwth = $opt->get_begin                  || 0;
# # my $endat = $opt->get_end                    || undef;
# # die "Missing mandatory command line argument -q" unless ($step || $queue);
# # # create a file-util object
# # my $fu = File::Util->new();
# # # create objects for logfile and messages
# # my $logfile    = Conv::Logfile->new(substr($instr,0,index($instr,'.xml'))."\t",[$FindBin::RealBin.$Conv::Config::LOGFILE,substr($instr,0,index($instr,'.xml')).".log"]);
# # # predeclare variables
# # my $upload_success = 0;
# # my $upload_failed  = 0;
# # my $cstatus        = 0;




# # # check if the lock-file is set on the working directory
# # if (-e $work.'isrunning.lock')
# # {
	# # $logfile->event("Attempt to start the converter process failed, another converter process is already running on $work.");	
	# # print "Cannot start the conversion workflow, since another conversion process is already using this directory.\n" if ($talk);
# # }
# # else
# # {
	# # # the actual program
	# # $logfile->event("Converter process started");

	
	# # # set the isrunning lockfile
	# # $fu->write_file('file' => $work.'isrunning.lock', 'content' => "1");
	
	# # # create file list, set dummy filename for step mode
	# # my @filelist = ('default');
	
	# # # in standard mode override file list with file names found in -q
	# # unless ($step)
	# # {
		# # # get the records from $queue
		# # @filelist    = $fu->list_dir($queue, qw/--files-only/);

	# # }
	# # my $no_of_files = $#filelist +1;
	# # my $j           = 0;
	# # print "$no_of_files files found in $queue\n" if ($talk);	

		   
	# # # process all files, one by one
	# # foreach my $file (@filelist)
	# # {

		# # # file counter
		# # $j++;
		# # # copy file to working directory
		# # copy ($queue.$file,$work.$file) unless ($step);
		
		# # # conversion
		# # # get the instructions
		# # my $inst         = Conv::Instructions->new($instr,$work,$queue,$file,$pdir);
		# # my @instructions = $inst->get_instructions;

		# # if (@instructions)
		# # {
			# # # set status to "success" (may be changed during processing)
			# # $cstatus = 1;
			
			# # # set last module to be processed
			# # my $run_until = $endat || $#instructions;
			   # # $run_until = $#instructions if ($run_until > $#instructions);
			   # # $run_until = $#instructions if ($run_until < $stwth);
			
			# # for (my $i = $stwth; $i <= $run_until; $i++)
			# # {
				# # my $instruction = $instructions[$i];
				# # my $module_to_load = $lib."::".$instruction->{NAME};

							
				# # # try to call execute() from the loaded module
				# # eval
				# # {
					# # load $module_to_load, 'execute';
					# # execute($instruction,$work,$file);
					# # print "executed $module_to_load on file $file [file $j out of $no_of_files, step $i of ".$#instructions."]\n" if ($talk);
				# # }; 
				# # # if failed, write error messages to logfile and quit current workflow
				# # if ($@)
				# # {
					# # # nice error message for the logfile
					# # my $errormsg = $@;
		   			   # # $errormsg =~ s/\n/ /g;
		   			
		   			# # # log it, set status to "failed" and end the loop
					# # $logfile->event("Processing of file $file failed in module $module_to_load: $errormsg");
					# # print "Processing of file $file failed in module $module_to_load: $errormsg"  if ($talk);
					# # $cstatus = 0;
					# # $upload_failed++;
					# # if ($dof) {die("Processing of file $file failed in module $module_to_load: $errormsg");} else {last;}
				# # }
			# # }
			
			# # # log success
			# # if ($cstatus)
			# # {
				# # $logfile->event("Processing of file $file successfully completed");
				# # $upload_success++;
			# # }
		# # }
		# # else
		# # {

			# # # failed reading instruction file
			# # $logfile->event("Processing of $file failed: Cannot read instruction file $instr");
			# # $upload_failed++;
		# # }
		
	# # }
	
	# # # printout messages
	# # print "$upload_success files out of $no_of_files in $queue successfully processed using instructions from $instr.\n" if ($upload_success);
	# # print "Processing failed for $upload_failed files out of $no_of_files in $queue using instructions from $instr.\n"   if ($upload_failed);
	
	# # # finish: remove lock-file
	# # unlink $work.'isrunning.lock';
	# # $logfile->event("Converter process stopped");
# # }
# # exit(1);

# # sub add_slash
# # {
	# # # add a slash to the end of the path if there isn't one already
	# # my $pathname = shift;
	# # $pathname .= '/' if ($pathname && (substr($pathname,-1) ne '/'));
	# # return $pathname;
# # }