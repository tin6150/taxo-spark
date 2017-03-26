##  #!/usr/lib/spark/bin/pyspark


## this version will use spark, create and hold s spark context
## and run the taxorpt with queries to spark
## will need to run a submit script for this.
## see spark-eg/submit.sh for that at this point.
## 2016.0727

## spark code adapted from spark_acc2taxid_3.py

# this script generate a report from an input file of GI list
# this is a driver script that use fn defined in pyphy_ext

# now will leverage spark/hdfs
# - added minor changes to deal with input output file argument parsing
#   html vs text output, 
#   since scriptr not sure how to make it use position ars

#http://stackoverflow.com/questions/12592544/typeerror-expected-a-character-buffer-object
from __future__ import print_function

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

import argparse 
import os
import sys
import time

# need to set PYTHONPATH in the spark-submit script, can't do it here.
#if 'PYTHONPATH' not in os.environ:
#        os.environ['PYTHONPATH'] = "/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/"
#PYTHONPATH = os.environ['PYTHONPATH']
#sys.path.insert(1, os.path.join(PYTHONPATH, "/usr/prog/python/2.7.9-goolf-1.5.14-NX/lib/python2.7/site-packages/"))
#sys.path.insert(1, os.path.join(PYTHONPATH, "/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/", "/usr/prog/python/2.7.9-goolf-1.5.14-NX/lib/python2.7/site-packages/"))
#sys.path.insert(1, os.path.join(PYTHONPATH, "/home/hoti1/code/svn/taxo-spark/", "/home/hoti1/local_python_2.7.9/lib/python2.7/site-packages/" ))

#from pyphy import *
#from pyphy_ext import *
## may need to cretae SparkContext before invoking these...   or else the app somehow get context initiated from those modules...
#import pyphy        # needed so can set db path
import pyphy_spark  # no longer need db, but may need SparkContext...
import pyphy_ext    # needed so can set dbglevel



#def process_cli() :
def process_cli( outFH ) :
        print( "inside process_cli()", file = outFH )
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
        parser.add_argument('--version', action='version', version='%(prog)s sparking 0.9.1  For help, email tin.ho@novartis.com')
        print( "about to parse process_cli()", file = outFH )
        ## FIXME  this crashes spark:
        args = parser.parse_args()
        print( "done  parse process_cli()", file = outFH )
        if args.jobName == '' or args.jobName == ' ' :
            args.jobName = 'taxo_report_' + time.strftime("%Y-%m-%d") 
        if args.jobDesc == '' or args.jobDesc == ' ' :
            if "stdin" in args.infile.name : 
                args.jobDesc = 'data from standard input'
            else :
                args.jobDesc = 'data from ' + args.infile.name 
        print( "about to return/complete process_cli()", file = outFH )
        return args
# end process_cli() 




#def run_taxo_reporter( args ) :
def run_taxo_reporter( outFH, args ) :

        print( "inside run_taxo_reporter()", file = outFH )
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
        pyphy_spark.db = args.db  # --db should have full path

        ### process input, then call fn to print it out to plain text or html file
        (accVerFreqList, recProcessed, rejectedRowCount) = pyphy_ext.file2accVerList( infile, args.col, args.IFS )  
        uniqAccVerCount    = len(accVerFreqList) 
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

def main_with_spark():
        outfile = "/home/hoti1/pub/taxorpt-spark.out"
        outFH = open( outfile, 'w' )
        print( "   *** hello world.  start of spark job, before declaring a SparkContext...", file = outFH )


        ## ch 8 of Learning Spark
        conf = SparkConf()
        conf.set( "spark.app.name", "taxorpt_spark_conf_2016_0729_local")     # better off to leave conf as spark-submit params
        conf.set( "spark.master", "local" )                                     # if use this, not in history server, but at least see some better output!
        #conf.set( "spark.master", "yarn" )
        #conf.set( "spark.submit.deployMode", "cluster" )
        conf.set( "spark.eventLog.enabled", True )                             # maybe spark 1.5 don't support these, can't get em to work :(
        conf.set( "spark.eventLog.dir", "file:///home/hoti1/pub" )             # will create app-id subdir in there.

        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark
        ## https://spark.apache.org/docs/1.5.0/configuration.html
        #sc = SparkContext( 'local', 'taxorpt_pyspark_local_0727' )
        #sc = SparkContext( appName='taxorpt_spark_yarn_0729_wSQLite' )         # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        sc = SparkContext( conf=conf )                                          # conf= is needed for spark 1.5
        # for now need to run in local mode, don't know why can't get output from yarn mode
        print( "   *** hello world sparkContext created" )
        print( "   *** hello world sparkContext created", file = outFH )


        # maybe the sqlContext should be declared later, after some scheme things are added?
        # but they are done to sc.... anyway, so may not matter??
        sqlContext = SQLContext(sc)
        #print( "   *** hello world spark sql context created", file = outFH )
        ### let child create sqlc...

        print( "   *** hello world spark sql.  setting up sc beffore creaing sqlContext and see if works better...", file = outFH )
        ## see spark-eg/spark_acc2taxid_3.py for eg of some other constructs...
        #lines = sc.textFile("nucl_gss.accession2taxid")                # 484.763253 sec to count(*), ie 8 min to cound 39,517,524 rows
        #lines = sc.textFile("prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv")  # largest file... 
        lines = sc.textFile("nucl_gss.accession2taxid.head100")         # 0.550907 sec to count (*)
        #lines = sc.textFile("nucl_gss.accession2taxid")         # 0.550907 sec to count (*)
        parts = lines.map(lambda l: l.split("\t"))
        acc_taxid = parts.map(lambda p: (p[0], p[1].strip(), p[2].strip(), p[3].strip() ))
        schemaString = "acc acc_ver taxid gi" 
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)
        
        
        ###+++   these seems to be needed by child fn...
        ###      at this point some code are redundant and need to be cleaned (after confirm move to OOP)
        schemaAccTaxid = sqlContext.createDataFrame(acc_taxid,schema)
        schemaAccTaxid.registerTempTable("acc_taxid")
        print( "  ** learning about the schema setup... ", file = outFH )
        print( "  ** learning about the schema setup... "  )
        schemaAccTaxid.printSchema()                            # not sure how to redirect this to file...


        print( "  * (*) * running Query of  SELECT taxid from acc_taxid WHERE acc_ver = '...' ", file = outFH )
        sqlResult = sqlContext.sql( "SELECT taxid from acc_taxid WHERE acc_ver = 'T02634.1' " )  # spark does NOT allow for ; at end of SQL !!
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        print( myList, file = outFH )           #  next line works too, so things are fine here
        print( myList[0].taxid, file = outFH )  # taxid is the name of the column specified in select


        
        #print( "   *** Running traditional taxorpt code", file = outFH )
        #args = process_cli()
        #run_taxo_reporter( args ) 
        #args = process_cli( outFH )
        #run_taxo_reporter( outFH, args ) 

        print( "   *** Testing run of sparksqlContext via import...", file = outFH )
        myList = pyphy_spark.testQueryWithSqlContext(sc,"T02634.1")              # this get called okay, actually code the fn is needed now.
        #myList = pyphy_spark.testQueryWithSqlContext(sc,sqlContext,"T02634.1")              # this get called okay, actually code the fn is needed now.
        print( myList, file = outFH )
        #print( myList[0].taxid, file = outFH )
        #++ more def needed before able to call column by name

        sc.stop() 
        print( "   *** good bye world !!" )
        print( "   *** good bye world !!", file = outFH )
        outFH.close()
        return 0

# main_with_spark()-end
        

### end of all fn definition, begin of main program flow.
#main()
main_with_spark()

