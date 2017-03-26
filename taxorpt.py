#!/prj/idinfo/prog/local_python_2.7.9/bin/python
#!/bin/env python
## /prog/python/2.7.9/bin/python    # started using in v .6 (2016.0304)
## /prog/python/3.4.2/bin/python    # used till v .5.1

# this script generate a report from an input file of GI list
# this is a driver script that use fn defined in pyphy_ext

# declaring this as version 1.0.
# next dev will leverage spark/hdfs
# - added minor changes to deal with input output file argument parsing
#   html vs text output, 
#   since scriptr not sure how to make it use position ars

#from __future__ import print_function
#http://stackoverflow.com/questions/12592544/typeerror-expected-a-character-buffer-object
import argparse 
import os
import sys
import time
#from pyphy import *
#from pyphy_ext import *
import pyphy        # needed so can set db path
import pyphy_ext    # needed so can set dbglevel



if 'PYTHONPATH' not in os.environ:
        os.environ['PYTHONPATH'] = "/prj/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/"
PYTHONPATH = os.environ['PYTHONPATH']
sys.path.insert(1, os.path.join(PYTHONPATH, "/prj/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/", "/prog/python/2.7.9/lib/python2.7/site-packages/"))

def process_cli() :
        # https://docs.python.org/2/howto/argparse.html#id1
        parser = argparse.ArgumentParser( description='Create a taxonomic report for a file with list of GI')
        # https://docs.python.org/2/library/argparse.html#nargs
        parser.add_argument("infile",  help="name of input file to process (def STDIN)",  nargs='?', type=argparse.FileType('r'), default=sys.stdin )
        parser.add_argument('outfile', help="name of output file           (def STDOUT)", nargs='?', type=argparse.FileType('w'), default=sys.stdout)
        # scriptr manifest json will present arguments in same order as defined in the json file.  
        # so long as input is followed by output files, passing to python will work.  
        # no need to use -i and -o flags, which i hate as it breaks unix cli convention.
        ##parser.add_argument('-i', '--infile',  help="name of input file to process (def STDIN)",  type=argparse.FileType('r'), default=sys.stdin )
        ##parser.add_argument('-o', '--outfile', help="name of output file           (def STDOUT)", type=argparse.FileType('w'), default=sys.stdout)
        # the above will open the file handle already, so output file will be touched/zeroed even if nothing else is printed to it.
        parser.add_argument('-s', '--separator', '--ifs', dest='IFS',  help="the column separator character (defualt TAB)",      default='	' ) 
        parser.add_argument('-c', '--col',  help="0-indexed column number containing the Accession.Ver (default 3)",  type=int,  default=3, required=False ) 
        parser.add_argument(      '--db',   help="full path to taxonomy db (defualt ./ncbi-taxo-acc.db)",             default='./ncbi-taxo-acc.db'  ) 
        parser.add_argument(      '--html', help="produce output in html format (default)", default="true",    action="store_true"  )
        parser.add_argument(      '--text', dest="html", help="produce output in text format instead of html", action="store_false" )
        ## the --html and --text logic is a bit reversed, due to historically wanted text but scriptr wants to default to html and checkbox default don't work.
        parser.add_argument(      '--name', dest='jobName',     help="name for this job",                 default='' )
        parser.add_argument(      '--desc', dest='jobDesc',     help="quoted string describing this job", default='' )
        parser.add_argument('-d', '--debuglevel', help="Debug mode. Up to -ddd useful for troubleshooting input file parsing. -ddddd intended for coder. ", action="count", default=0)
        #parser.add_argument('-d', '--debuglevel', help="increase debug (verbosity) level", type=int, choices=[0,1,2,3])
        parser.add_argument('--version', action='version', version='%(prog)s 0.7.5  For help, email tin.ho@novartis.com')
        args = parser.parse_args()
        if args.jobName == '' or args.jobName == ' ' :
            args.jobName = 'taxo_report_' + time.strftime("%Y-%m-%d") 
        if args.jobDesc == '' or args.jobDesc == ' ' :
            if "stdin" in args.infile.name : 
                args.jobDesc = 'data from standard input'
            else :
                args.jobDesc = 'data from ' + args.infile.name 
        return args
# end process_cli() 




def run_taxo_reporter( args ) :

        # need to prep html file with header early so that debug message will print to it correctly.
        if args.html :
            pyphy_ext.prepHtmlHeader( args.outfile )

        ### this block just print various debug info if -d(dd) is specified
        infile  = args.infile
        outfile = args.outfile
        if( args.debuglevel >= 2 ) :
            print( "<!--input  file is %s-->"   % infile  )
            print( "<!--output file is %s-->"   % args.outfile )
        if( args.debuglevel >= 1 ) :
            print( "<!--column    to use is '%s'-->" % args.col )
            print( "<!--separator to use is '%s'-->" % args.IFS )
        if( args.debuglevel >= 4 ) :
            print( "<!--job name, desc: '%s', '%s'-->" ) % (args.jobName, args.jobDesc) 

        # configure parameters for imported modules
        pyphy_ext.dbgLevel = args.debuglevel 
        pyphy.db = args.db  # --db should have full path

        ### process input, then call fn to print it out to plain text or html file
        ## FIXME (giFreqList, recProcessed, rejectedRowCount) = file2giList( infile, args.col, args.IFS )  
        (accVerFreqList, recProcessed, rejectedRowCount) = pyphy_ext.file2accVerList( infile, args.col, args.IFS )  
        uniqAccVerCount    = len(accVerFreqList) 
        ## FIXME >>
        taxidFreqTable = pyphy_ext.summarizeAccVerList( accVerFreqList )
        if args.html :
            pyphy_ext.prettyPrintHtml( taxidFreqTable, args.jobName, args.jobDesc, uniqAccVerCount, recProcessed, rejectedRowCount, outfile ) 
            pyphy_ext.prepHtmlEnding( outfile )
            pass
        else :
            pyphy_ext.prettyPrint( taxidFreqTable, outfile ) 
            print( "Uniq Accession.V count: %d.  Total row processed: %d. Rejected: %d." % ( uniqAccVerCount, recProcessed, rejectedRowCount ) )
        infile.close()
        outfile.close()
        pass
        
def main():
        args = process_cli()
        run_taxo_reporter( args ) 
# main()-end


### end of all fn definition, begin of main program flow.
main()

