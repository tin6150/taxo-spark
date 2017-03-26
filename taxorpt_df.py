### taxorpt_df.py.2017.0306.beforeReadProtein ###

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

# notes from 2017.0224
# at this point, taxorpt_spark.py does ineeed work to produce a report and does use spark
# however, it is largely just old taxorpt sql queries casted into spark, no parallelization
# To gain performance, need to parallelize, which means using data frame
# this version of taxorpt_spark.py , together with pyphy_ext_spark.py, pyphy_spark.py, 
# does NOT use dataframe and thus things are run in series.  slow.  took 20 min to process /home/hoti1/code/svn/taxo/db/protein_blast_hits.txt.head100

# taxorpt_df.py
# will be a new version that use data frame and thus parallelize things.
# After having played with taxoTraceTbl.py and taxoTraceTblOop.py, decided the new approach will be:
# - use pyphy.py/pyphy_ext.py  in simple python/sqlite version.
#     - the taxid->parent table will be kept in sqllite
# - dataframe will be created as high level trace table.
#     - getParentByRank will be UDF that utilize pyphy/sqlite to get parent, outside of spark
#     - parellelization will come from spark processing each row of dataFrame independently, using UDF
#     - the UDF, cuz it don't seems possible to do another sqlContext call, thus leaving that as sqlite, using good old school python.
# - before proceeding, need to make sure cannot indeed do multiple sqlContext for query and join.  
#   Have tried much, can't write complex join statement that may skip some taxonomy level... 
#   The difficulty was that while inside a DF, when calling UDF, am not able to have a spark table for more queies.  
#   It maybe possible, but after much effort have not found technical way to accomplish this
#   in interest of time, doing the sqlite for simple parent lookup in simple python so that can host it in UDF would 
#   reap most of the parellelization of Spark.  
# ==> taxorpt_df.py will be a new file (this is just recording where progress will go in this old file).
# 2017.0224.    start with creating a trace table, a wide table with species taxid, parent taxid at genus, family, kingdom level.
     

# taxoTraceTbl_df.py  [planned fork] [2017.0228]
# - forked from taxorpt_df.py when it had a good usable trace table
# - create trace table, utilize data frame, adding 1 column at a time for species, genus, family...
#    -- this column add use UDF, which use sqlite, and seems to take a long time
#    -- thus this trace table is treated as a prep step 
# - trace table saved as parquet
# - parquet will be loaded in future by taxorpt_df.py and do inner join with BLAST output data.     
#    -- report run may only need the paquet and not the sqlite db...

# 2017.0303  taxorpt_df.py    
# - will read blast output
# - will read parquet containing trace table (code mostly from sn-bb/trace_load.py)
# - have two spark table, do inner join
# - old code to create trace table (that has been moved to taxoTraceTbl_df.py) have been removed from this file
# - process_cli fn are from old stuff, need adjustment.

#http://stackoverflow.com/questions/12592544/typeerror-expected-a-character-buffer-object
from __future__                 import print_function
from pyspark                    import SparkContext, SparkConf
from pyspark.sql                import SQLContext, Row
from pyspark.sql.functions      import lit, udf
from pyspark.sql.types          import StringType, StructField, StructType  # needed as return type for UDF.  ref: loc 4939 spark book 
#from pyspark.sql.types         import *                                    # may as well import * ... 


# unable to find these modules :(   from doesn't complain, but fn usage below in UDF can't find it.)
from pyphy      import *
from pyphy_ext  import *
## may need to cretae SparkContext before invoking these...   or else the app somehow get context initiated from those modules...
#import pyphy                # needed so can set db path
#import pyphy_spark         # no longer need db, but may need SparkContext...
#import pyphy_ext            # needed so can set dbglevel
#import pyphy_ext_spark     # needed so can set dbglevel



import argparse 
import os
import sys
import time


# static variable definition, adjust according to run requirement
#taxoInputCsv       = "/home/hoti1/code/hoti-bb/taxo-spark/db/tree.table.csv.special2k" 
#outputParquetFile  = "/home/hoti1/pub/taxoTraceTblParquet_allRank_sp2k"

#outputParquetFile   = "/home/hoti1/pub/taxorpt_Parquet_tmp_pe8"
progOutputFile      = "/home/hoti1/pub/taxorpt_df.rpt306.out"
#taxoInputCsv        = "/home/hoti1/code/hoti-bb/taxo-spark/db/tree.table.csv" 
#taxoInputCsv        = taxoInputCsv   # should really replace it with the sqlite db used by pyphy,   ~ line 210 +++
# but may not be so trivial...  if req export to csv, 
# then should get to the source dir of the ncbi download that was used to build the sqlite db.


spkSqlTraceTableName    = "taxoTraceTblParquet0228"              
inputParquetFile        = "/home/hoti1/pub/taxoTraceTblParquet_full_0228"


# most likely won't need to change these:
pythonPathAddition="/home/hoti1/code/hoti-bb/taxo-spark/:/db/idinfo/prog/local_python_2.7.9/lib/python2.7:/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages"
sparkContextAppName="sparksql_df_uge"
sparkEventLogDir="file://clscratch/hoti1/pub/sparkEvent"

# need to set PYTHONPATH in the spark-submit script, 
# BUT can't do it here, it has no effect
# at least for UDF, set in sparkContext setExecutorEnv()
#if 'PYTHONPATH' not in os.environ:
#        os.environ['PYTHONPATH'] = "/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/"
#PYTHONPATH = os.environ['PYTHONPATH']
#sys.path.insert(1, os.path.join(PYTHONPATH, "/usr/prog/python/2.7.9-goolf-1.5.14-NX/lib/python2.7/site-packages/"))
#sys.path.insert(1, os.path.join(PYTHONPATH, "/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/", "/usr/prog/python/2.7.9-goolf-1.5.14-NX/lib/python2.7/site-packages/"))
#sys.path.insert(1, os.path.join(PYTHONPATH, "/home/hoti1/code/svn/taxo-spark/", "/home/hoti1/local_python_2.7.9/lib/python2.7/site-packages/" ))
#sys.path.insert(1, os.path.join("/home/hoti1/code/hoti-bb/taxo-spark/", "/home/hoti1/local_python_2.7.9/lib/python2.7/site-packages/" ))


# load the blast output, with list of accession number.
# need to convert this to taxid, maybe with uniq and count, 
# then ultimately join (maybe in diff fn)
# may not need this after all, can use old fn... 
def load_protein_list() :
        print( "ie, load blast output")
#end load_protein_list() 



### from trace_load.py
def load_trace_table() :                # most likely version w/o accession/taxid info, likely not needed here anymore
        #outfile = "/home/hoti1/pub/trace_load_1108.out"
        outfile = progOutputFile
        #outfile = "/home/hoti1/pub/spark_acc2taxid_4.out"
        outFH = open( outfile, 'w' )
        #outFH.write( "Test output to file from spark\n" )
        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark 
        ##spkCtx = SparkContext( 'local', 'tin_pyspark_0724_local' )
        #spkCtx = SparkContext( appName='tin_pyspark_31gb' )
        #spkCtx = SparkContext( appName='tin_pyspark_yarn_trace_load_1108' )
        #spkCtx = SparkContext( appName=sparkContextAppName )
        #print( "   *** hello world sparkContext created" )
        #print( "   *** hello world sparkContext created", file = outFH )


        conf = SparkConf().setAppName( sparkContextAppName )
        conf.set( "spark.eventLog.enabled", False )                             
        conf.set( "spark.eventLog.dir", sparkEventLogDir )                     # will create app-id subdir in there.
        conf.setExecutorEnv( "PYTHONPATH", pythonPathAddition )             # 
        #https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python to quiet log4j
        spkCtx = SparkContext( conf=conf)                                       # conf= is needed for spark 1.5
        print( "   ### hello world sparkContext created *L32*" )
        print( "   ### hello world sparkContext created *L32*", file = outFH )

        # turn off loggin  http://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-pyspark 
        #log4j = spkCtx._jvm.org.apache.log4j
        #log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)
        # log4j settings is pretty good in trimming the log messages, but next one is simpler and does same thing
        spkCtx.setLogLevel("FATAL")
        

        sqlContext = SQLContext(spkCtx)         # can only have one instance of SQLContext
        #tabABsqlCtx = SQLContext(spkCtx)       # a second delcaration will result in not so useful object
        print( "   *** hello world spark sql context created" )
        print( "   *** hello world spark sql context created", file = outFH )

        #treeSqlCtx = spkCtx.textFile("tree.table.csv")  # tree table 
        #partsT = treeSqlCtx.map(lambda l: l.split(","))
        #treeTab = partsT.map(lambda p: (p[0], p[1].strip('"'), p[2].strip(), p[3].strip('"')))    
        #schemaStringT = "taxid name parent rank"
        #fieldsT = [StructField(field_name, StringType(), True) for field_name in schemaStringT.split()]
        #schemaT = StructType(fieldsT)
        #treeDF  = sqlContext.createDataFrame(treeTab,schemaT) 
        #treeDF.printSchema()
        #treeDF.show()
        #treeDF = DataFrame      # ??

        #parquetFile = sqlContext.read.parquet("people.parquet")
        #parquetFile.registerTempTable("parquetFile");
        #trace_table = sqlContext.read.parquet("trace_table.parquet")
        # above didn't work in c3po.  but stripping .parquet worked
        #trace_table = sqlContext.read.parquet("trace_table")
        trace_table = sqlContext.read.parquet(inputParquetFile)                 # oldSqlQuery
        taxoTraceTblParquet = trace_table                                       # this work as dataframe obj, but not as table name in SQL.
        #taxoTraceTblParquet = sqlContext.read.parquet(inputParquetFile)         # a 2nd reg used by newSqlQuery (copied from taxorpt_dt)  


        print( "  ### parquet as loaded from disk " )
        trace_table.printSchema()
        trace_table.show(n=30)
        #taxoTraceTblParquet.show(n=300)

        #print( "  ### parquet after filter " )
        # ref for .where() : https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html?highlight=select#pyspark.sql.DataFrame.where 
        #trace_table.select("*").where("rank_T0='species'").write.save( outputParquetFile, "parquet", "overwrite")    # this works, filter into smaller set befor save        
        #trace_table_filter1 = trace_table.select("*").where("rank_T0<>'species'")           
        #trace_table.show(n=300)   # not affected by select above...
        #trace_table_filter1.show(n=300)

        #tabT[lastIdx-1].registerTempTable("trace_table")
        trace_table.registerTempTable("trace_table")                  # primary table name used by SQL query
        #trace_table.registerTempTable("taxoTraceTblParquet")         # alt table name used by SQL (due to historical reason)
        trace_table.registerTempTable(spkSqlTraceTableName)           # does table registration persist?  need change name for next instance?  should drop when done??
        #trace_table_filter1.registerTempTable("taxoTraceTblFilter1") # table name used by SQL in runSqlQuery3        

        # running some sample SQL queries for sanity check (works, but not really needed anymore)
        runOldSqlQuery=0
        if( runOldSqlQuery ) : 
                print( "  ### (*) * running SQL Query COUNT... " )
                print( "  ### (*) * running SQL Query COUNT... ", file = outFH )
                sqlResult = sqlContext.sql("SELECT COUNT( taxid_T0 ) FROM trace_table") # spark does NOT allow for ; at end of SQL !!
                print( "   ### sqlResult: ***", file = outFH )
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                print( "myList is: %s", myList )                        # [Row(_c0=1402066)]   # took about 20 sec (53sec if omit .show() after all the joins)
                # run time on c3po with pe 16 and 16 GB e/a is also ~50 sec (w/ show).  
                print( "myList is: %s", myList, file = outFH )          # this works too! 
                print( "sqlResult is: %s", sqlResult )                  # sqlResult is: %s DataFrame[_c0: bigint]
                print( "sqlResult is: %s", sqlResult, file = outFH )
                #if( sqlResult.count() > 1 ):
                #        print( "Houston, we got more than one element returned!")

                # may want to try to query and show this record
                # |taxid_T0|             name_T0|parent_T0|rank_T0|
                # |  287144|Influenza A virus...|   119210|no rank|
                # for plant, even lastIdx=15 wasn't enough to get the full lineage trace


                print( "  ### (*) * running SQL Query SELECT... " )
                print( "  ### (*) * running SQL Query SELECT... ", file = outFH )
                #sqlResult = sqlContext.sql( "SELECT taxid from acc_taxid WHERE acc_ver = 'T02888.1' " )  # spark does NOT allow for ; at end of SQL !!
                sqlResult = sqlContext.sql( "SELECT * from trace_table WHERE taxid_T0 = '287144' " )  # spark does NOT allow for ; at end of SQL !!
                print( "   ### sqlResult: ***", file = outFH )
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                ## collect put all data into a single machine, so avoid running till the end for true big data
                print( "myList is: %s", myList )                        # [Row(_c0=1402066)]   # took about 20 sec (53sec if omit .show() after all the joins)
                print( "myList is: %s", myList, file = outFH )          # this works too! 
                print( "sqlResult is: %s", sqlResult )                  # sqlResult is: %s DataFrame[_c0: bigint]
                print( "sqlResult is: %s", sqlResult, file = outFH )

                #myList = sqlResult.collectAsMap()      # sqlResult is DF, not RDD, no collectAsMap fn.
                # sqlResult is DF, not RDD.  specifically, it is DataFrame[taxid: string]
                #myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( myList[0].taxid )                # taxid is the name of the column specified in select
                #print( myList[0].taxid, file = outFH )  # taxid is the name of the column specified in select
        #end if( runSqlQuery )

        print( "   ### good bye world !!" )
        print( "   ### good bye world !!", file = outFH )
        outFH.close()
        spkCtx.stop()           # close things off nicely to avoid error in log file
        exit 
# end load_trace_table()








####
#### process_cli and run_taxo_report may eventually be needed, 
#### but for now not using till i get trace table created in spark in good parallilized enough manner
####


#def process_cli() :
def process_cli( outFH ) :
        print( "   *** inside process_cli() L51", file = outFH )
        print( "   *** inside process_cli() L51" )
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
        print( "   *** about to return/complete process_cli() L86", file = outFH )
        print( "   *** about to return/complete process_cli() L86" )
        return args
# end process_cli() 




#def run_taxo_reporter( args ) :
#def run_taxo_reporter( outFH, args ) :
def run_taxo_reporter( args, taxoHandle, outFH ) :

        print( "   *** inside run_taxo_reporter() L96", file = outFH )
        print( "   *** inside run_taxo_reporter() L96" )
        # need to prep html file with header early so that debug message will print to it correctly.
        if args.html :
            pyphy_ext_spark.prepHtmlHeader( args.outfile )
            #pyphy_ext.prepHtmlHeader( args.outfile )

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
        pyphy_ext_spark.dbgLevel = args.debuglevel 
        #--pyphy_spark.db = args.db  # --db should have full path

        ### process input, then call fn to print it out to plain text or html file
        #(accVerFreqList, recProcessed, rejectedRowCount) = pyphy_ext_spark.file2accVerList( infile, args.col, args.IFS )  
        (accVerFreqList, recProcessed, rejectedRowCount) = pyphy_ext_spark.file2accVerList( infile, args.col, args.IFS, taxoHandle )  

        print( "   ### tmp checking accVerFreqList, result of parsing blast input " )
        print( "   ### tmp checking accVerFreqList, result of parsing blast input ", file = outFH )
        print( accVerFreqList )
        print( accVerFreqList, file = outFH )
        print( "   *** tmp exiting run_taxo_reporter() L346" )
        print( "   *** tmp exiting run_taxo_reporter() L346", file = outFH )
        return

        #% tmp exit, till get new code in.
        #% file2acc... is where action begin, and need to get the spark handle...   TODO ++ FIXME ++
        #% but file2acc process one line at a time, and build the accVerFreqList on the fly... 
        #% either do this whole step in spark and produce the same list as return value
        #% or potentially leave this mostly alone for now, and convert the list into DataFrame and do the join as next step...
        uniqAccVerCount    = len(accVerFreqList) 
        #% 2017.0306 ++ TODO ++ start dataframing here 
        #% summarizeAccVerList() is where it take acc list and their freq, and lookup the lineage trace.
        #% so that's where spark join will replace much of its original fn
        taxidFreqTable = pyphy_ext_spark.summarizeAccVerList( accVerFreqList, taxoHandle )
        if args.html :
            pyphy_ext_spark.prettyPrintHtml( taxidFreqTable, args.jobName, args.jobDesc, uniqAccVerCount, recProcessed, rejectedRowCount, outfile ) 
            pyphy_ext_spark.prepHtmlEnding( outfile )
            pass
        else :
            pyphy_ext_spark.prettyPrint( taxidFreqTable, outfile, taxoHandle ) 
            print( "Uniq Accession.V count: %d.  Total row processed: %d. Rejected: %d." % ( uniqAccVerCount, recProcessed, rejectedRowCount ) )
        infile.close()
        outfile.close()
        pass
        print( "   *** exiting run_taxo_reporter() L136", file = outFH )
# end run_taxo_reporter( args, taxoHandle, outFH )




####
####    fn def below should no longer be needed anymore in taxorpt_df.py      2017.0224
####
        

def _abandone__main_with_spark_pyphy_oo():
        #print( "hello OOP :) " )
        outfile = "/home/hoti1/pub/taxorpt-spark.out"
        outFH = open( outfile, 'w' )
        print( "   *** hello OOP world :)  start of spark job, before declaring a SparkContext...", file = outFH )
        print( "   *** outfile is set to %s " % outfile, file = outFH )

        ## ch 8 of Learning Spark
        conf = SparkConf()
        conf.set( "spark.app.name", "taxorpt_spark_conf_2017_0216_c3po")        # better off to leave conf as spark-submit params
        #conf.set( "spark.master", "local" )                                     # if use this, not in history server, but at least see some better output!
        #conf.set( "spark.master", "yarn" )
        #conf.set( "spark.submit.deployMode", "cluster" )
        conf.set( "spark.eventLog.enabled", True )                             # maybe spark 1.5 don't support these, can't get em to work :(
        conf.set( "spark.eventLog.dir", "file:///home/hoti1/pub" )             # will create app-id subdir in there.

        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark
        ## https://spark.apache.org/docs/1.5.0/configuration.html
        #sc = SparkContext( 'local', 'taxorpt_pyspark_local_0727' )
        #sc = SparkContext( appName='taxorpt_spark_yarn_0812_noSQLite' )         # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        sc = SparkContext( appName='taxorpt_spark_c3po_0216_noSQLite' )         # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        #sc = SparkContext( conf=conf )                                          # conf= is needed for spark 1.5
        # for now need to run in local mode, don't know why can't get output from yarn mode
        print( "   *** hello world sparkContext created" )
        print( "   *** hello world sparkContext created", file = outFH )

        taxoHandle = pyphy_spark.pyphy_spark_class(sc,outFH)

        ## here really call the taxorpt processing





        runTest = 1             # c3po result in ~/pub/uge*spark*run3
        if( runTest ):
                print( "   *** taxorpt-spark running test L181..." )
                print( "   *** taxorpt-spark running test L181...", file = outFH )
                ##result = taxoHandle.testQuery( "T02634.1" )
                result = taxoHandle.testQuery( "T02732.1" )        # acc.ver T02732.1 is in svn/taxo/db/download/nucl_gss.accession2taxid.head100
                print( result, file = outFH )
                

        ## here really call the taxorpt processing
        print( "   *** taxorpt_spark starting main report work L190" )
        print( "   *** taxorpt_spark starting main report work L190", file = outFH )
        args = process_cli( outFH )
        run_taxo_reporter( args, taxoHandle, outFH ) 
        print( "   *** taxorpt_spark completes main report work" )
        print( "   *** taxorpt_spark completes main report work", file = outFH )



        runMoreTest = 0         # c3po result in ~/pub/uge*spark*run3
        if( runMoreTest ):
                ## test a few more fn, from pyphy_tester.py

                print( "   *** taxorpt-spark running MOre test L206..." )
                print( "   *** taxorpt-spark running MOre test L206...", file = outFH )
                print( "   *** taxorpt-spark test: getSonsBy...", file = outFH )
                #result = taxoHandle.getSonsByTaxid("33630",50)
                #print( result, file = outFH )
                result = taxoHandle.getSonsByName("Plasmodium")
                print( result, file = outFH )


                print( "   *** taxorpt-spark test: AccVer...", file = outFH )
                result = taxoHandle.getTaxidByAccVer("T02634.1")
                print( result, file = outFH )
                result = taxoHandle.getAccVerByTaxid("5833")
                print( result, file = outFH )

                ## both of these return list, hope that's what caller expected...
                result = taxoHandle.getTaxidByName("Bacteria",1)
                print( result, file = outFH )

                result = taxoHandle.getTaxidByName("Bacteria",2)
                print( result, file = outFH )

                print( "   *** taxorpt-spark test: getRankByTaxid...", file = outFH )
                result = taxoHandle.getRankByTaxid("5833")
                print( result, file = outFH )

                print( "   *** taxorpt-spark test: getNameByTaxid...", file = outFH )
                result = taxoHandle.getNameByTaxid("5833")
                print( result, file = outFH )
                result = taxoHandle.getNameByTaxid("9999111555000")
                print( result, file = outFH )

                print( "   *** taxorpt-spark test: getParentByTaxid...", file = outFH )
                result = taxoHandle.getParentByTaxid("5833")
                print( result, file = outFH )

                #### recursive lookup crosses data partion, so only work in cluster mode
                #### local mode yield a network connectivity error
                #print( "   *** taxorpt-spark test: 422676..." )
                #result = taxoHandle.getPathByTaxid("5833")
                result = taxoHandle.getPathByTaxid("422676")
                print( result, file = outFH )
                #print( "   *** taxorpt-spark test: 33630..." )
                result = taxoHandle.getPathByTaxid("33630")
                print( result, file = outFH )
                #print( "   *** taxorpt-spark test: 5794..." )
                result = taxoHandle.getPathByTaxid("5794")
                print( result, file = outFH )
        # end if runMoreTest section

        sc.stop() 
        print( "   *** good bye world L254 !!" )
        print( "   *** good bye world L254 !!", file = outFH )
        outFH.close()
        return 0
# end main_with_spark_pyphy_oo()




## when fixed and done, move this global declaration to top of file for easy file name change
## consider moving schemaFields as global def as well, but then the parser/splitting part is still not trivial to change...
#acc2taxid_db_file = "nucl_gss.accession2taxid.head100"  # don't remember where file was found, but think was last file used in last prog that read acc2taxid table
acc2taxid_db_file       = "/home/hoti1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv"  # 31GB, 818,215,989 rows, job 2786873 took 23 min to read file and COUNT(*) w/ pe 5
#acc2taxid_db_file       = "/home/hoti1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv.head100" 
#acc2taxid_db_file       = "/home/hoti1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv.head5k" 
acc2taxid_tablename     = "acc_taxid"
taxidUniqList_tablename = "taxid_uniq_list_tab" 

#def dont_use_anymore_main_with_spark():
#def may_use_again_with_new_df__dont_use_anymore_main_with_spark():
def load_acc2taxid_datafile() :
        #outfile = "/home/hoti1/pub/taxorpt-spark.out"
        #outFH = open( outfile, 'w' )
        outFH = open( progOutputFile, 'w' )
        print( "   *** hello world.  start of spark job, before declaring a SparkContext...", file = outFH )


        ## ch 8 of Learning Spark
        conf = SparkConf()
        #conf.set( "spark.app.name", "taxorpt_spark_conf_2016_0729_local")     # better off to leave conf as spark-submit params
        conf.set( "spark.app.name", sparkContextAppName) 
        #conf.set( "spark.master", "local" )                                     # if use this, not in history server, but at least see some better output!
        #conf.set( "spark.master", "yarn" )
        #conf.set( "spark.submit.deployMode", "cluster" )
        conf.set( "spark.eventLog.enabled", False )                            # maybe spark 1.5 don't support these, can't get em to work :(
        conf.set( "spark.eventLog.dir", sparkEventLogDir )                     # will create app-id subdir in there.
        #conf.set( "spark.eventLog.dir", "file:///home/hoti1/pub" )            # will create app-id subdir in there.
        conf.setExecutorEnv( "PYTHONPATH", pythonPathAddition )
        spkCtx = SparkContext( conf=conf ) 
        spkCtx.setLogLevel("FATAL")

        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark
        ## https://spark.apache.org/docs/1.5.0/configuration.html
        #sc = SparkContext( 'local', 'taxorpt_pyspark_local_mmdd_oldFn' )
        #sc = SparkContext( appName='taxorpt_spark_yarn_mmdd_oldFnNoLongerUsed_wSQLite' )         # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        #sc = SparkContext( conf=conf )                                          # conf= is needed for spark 1.5
        # for now need to run in local mode, don't know why can't get output from yarn mode
        print( "   *** hello world sparkContext created" )
        print( "   *** hello world sparkContext created", file = outFH )


        # maybe the sqlContext should be declared later, after some scheme things are added?
        # but they are done to sc.... anyway, so may not matter??
        sqlContext = SQLContext(spkCtx)
        #print( "   *** hello world spark sql context created", file = outFH )
        ### let child create sqlc...

        print( "   *** hello world spark sql.  setting up sc beffore creaing sqlContext and see if works better...", file = outFH )
        ## see spark-eg/spark_acc2taxid_3.py for eg of some other constructs...
        #lines = sc.textFile("nucl_gss.accession2taxid")                # 484.763253 sec to count(*), ie 8 min to cound 39,517,524 rows
        #lines = sc.textFile("prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv")  # largest file... 
        #lines = sc.textFile("nucl_gss.accession2taxid.head100")         # 0.550907 sec to count (*)
        #lines = sc.textFile("nucl_gss.accession2taxid")         # 0.550907 sec to count (*)
        #lines = sc.textFile(acc2taxid_db_file)
        #lines = spkCtx.textFile(acc2taxid_db_file)
        lines = spkCtx.textFile(acc2taxid_db_file, use_unicode=False) # https://spark.apache.org/docs/1.6.0/api/python/pyspark.html?highlight=textfile#pyspark.SparkContext.wholeTextFiles
        # no way to declare file has header line, which seems to have in scala (or a spark 2.0 feature?)


        parts = lines.map(lambda l: l.split("\t"))
        acc_taxid = parts.map(lambda p: (p[0], p[1].strip(), p[2].strip(), p[3].strip() ))
        schemaString = "acc acc_ver taxid gi" 
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)
        schemaAccTaxidDf = sqlContext.createDataFrame(acc_taxid,schema)
        #schemaAccTaxidDf.registerTempTable("acc_taxid")
        schemaAccTaxidDf.registerTempTable( acc2taxid_tablename )
        print( "  ### learning about the schema setup #L555#... ", file = outFH )
        print( "  ### learning about the schema setup #L555#... "  )
        schemaAccTaxidDf.printSchema()                            # not sure how to redirect this to file...
        schemaAccTaxidDf.show(n=5)                                # not sure how to redirect this to file...


        #print( "  * (*) * running Query of  SELECT taxid from acc_taxid WHERE acc_ver = '...' ", file = outFH )
        #sqlResult = sqlContext.sql( "SELECT taxid from acc_taxid WHERE acc_ver = 'T02634.1' " )  # spark does NOT allow for ; at end of SQL !!
        sqlCmd = "SELECT COUNT(*) from %s" % acc2taxid_tablename                                  # spark does NOT allow for ; at end of SQL !!
        print( "  ### running Query of %s" % sqlCmd )
        print( "  ### running Query of %s" % sqlCmd, file = outFH )
        sqlResult = sqlContext.sql( sqlCmd )  
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        print( myList ) 
        print( myList, file = outFH )    
        #print( myList[0].taxid, file = outFH )  # taxid is the name of the column specified in select

        #print( "  ### trying dataframe filter:" )
        #schemaAccTaxidDf.groupBy("taxid").count().show()   # works.  https://spark.apache.org/docs/1.6.0/sql-programming-guide.html#dataframe-operations
        #taxidUqList = schemaAccTaxidDf.groupBy("taxid")   # https://spark.apache.org/docs/1.6.0/sql-programming-guide.html#dataframe-operations
        # c3po has spark 1.6.0, so does have the above....  but after groupBy, many other DataFrame fn not avail :(
        print( "  ### trying dataframe/sql fn filter :" )
        #taxidUqList = schemaAccTaxidDf.select("taxid") # work
        #taxidUqList = schemaAccTaxidDf.select("UNIQUE(taxid)" #  dont work
        #taxidUqList = schemaAccTaxidDf.sql("SELECT UNIQUE(taxid)") # nope #ref https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html
        taxidUqList = sqlContext.sql("SELECT DISTINCT taxid FROM %s" % acc2taxid_tablename ) 
        #taxidUqList.printSchema()   
        #taxidUqList.show(n=25)

        #print( "  ### trying dataframe/sql after registerTable :" )
        #taxidUqListDf = sqlContext.sql("SELECT UNIQUE taxid FROM %s" % acc2taxid_tablename ) 
        taxidUqList.registerTempTable( taxidUniqList_tablename )
        taxidUqList.printSchema()   
        taxidUqList.show(n=15)
        sqlCmd = "SELECT COUNT(*) from %s" % taxidUniqList_tablename
        print( "  ### running Query of %s" % sqlCmd )
        print( "  ### running Query of %s" % sqlCmd, file = outFH )
        sqlResult = sqlContext.sql( sqlCmd )  
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        print( myList ) 
        print( myList, file = outFH )    

        
        #print( "   *** Running traditional taxorpt code", file = outFH )
        #args = process_cli()
        #run_taxo_reporter( args ) 
        #args = process_cli( outFH )
        #run_taxo_reporter( outFH, args ) 

        ##~ this block prob never fully worked when last abandoned
        ##~print( "   *** Testing run of sparksqlContext via import...", file = outFH )
        ##~myList = pyphy_spark.testQueryWithSqlContext(sc,"T02634.1")              # this get called okay, actually code the fn is needed now.
        #myList = pyphy_spark.testQueryWithSqlContext(sc,sqlContext,"T02634.1")              # this get called okay, actually code the fn is needed now.
        ##~print( myList, file = outFH )
        #print( myList[0].taxid, file = outFH )
        ##~#++ more def needed before able to call column by name

        spkCtx.stop() 
        print( "   *** good bye world !!" )
        print( "   *** good bye world !!", file = outFH )
        outFH.close()
        return 0
#end fn: load_acc2taxid_datafile()    # formerly end fn : dont_use_anymore_main_with_spark()




def main():
        args = process_cli()
        run_taxo_reporter( args ) 
# main()-end

### end of all fn definition, begin of main program flow.
main()
#main_with_spark()
##main_with_spark_pyphy_oo()      # expect to be called with an input (and output) file arg.  (?) or uge/spark may wait forever for input in stdin...

# main execution of this file for taxorpt_spark.py 2017.0304
#load_trace_table()             # most likely old version w/o accession2taxid info
#load_acc2taxid_datafile()       # forked/adopted by taxoTraceTbl_df.py, will likely not needed here anymore



"""
results so far.
even when appending treeDF, don't seems to be making 4 column addition at a time, so can use this for now.
root
 |-- taxid_T0: string (nullable = true)
 |-- name_T0: string (nullable = true)
 |-- parent_T0: string (nullable = true)
 |-- rank_T0: string (nullable = true)
 |-- P@subspecies_udf1: string (nullable = true)
 |-- N@subspecies_udf1: string (nullable = true)
 |-- P@species_udf2: string (nullable = true)
 |-- N@species_udf2: string (nullable = true)
 |-- P@genus_udf3: string (nullable = true)
 |-- N@genus_udf3: string (nullable = true)
 |-- P@family_udf4: string (nullable = true)
 |-- N@family_udf4: string (nullable = true)
 |-- P@superkingdom_udf5: string (nullable = true)
 |-- N@superkingdom_udf5: string (nullable = true)

+--------+--------------------+---------+-----------+--------------------+-----------------+------------------+--------------------+----------------+------------+-----------------+-------------+--------------------+-------------------+
|taxid_T0|             name_T0|parent_T0|    rank_T0|   P@subspecies_udf1|N@subspecies_udf1|    P@species_udf2|      N@species_udf2|    P@genus_udf3|N@genus_udf3|    P@family_udf4|N@family_udf4| P@superkingdom_udf5|N@superkingdom_udf5|
+--------+--------------------+---------+-----------+--------------------+-----------------+------------------+--------------------+----------------+------------+-----------------+-------------+--------------------+-------------------+
| "taxid"|                name| "parent"|       rank|-1:no rank,subspe...|          unknown|-1:no rank,species|             unknown|-1:no rank,genus|     unknown|-1:no rank,family|      unknown|-1:no rank,superk...|            unknown|
| 1035824|Trichuris sp. ex ...|    36086|    species|                   1|             root|           1035824|Trichuris sp. ex ...|           36086|   Trichuris|           119093|  Trichuridae|                2759|          Eukaryota|
|    9606|        Homo sapiens|     9605|    species|                   1|             root|              9606|        Homo sapiens|            9605|        Homo|             9604|    Hominidae|                2759|          Eukaryota|
| 1383439|Homo sapiens/Mus ...|  1002697|    species|                   1|             root|           1383439|Homo sapiens/Mus ...|               1|        root|                1|         root|                2759|          Eukaryota|
|    9605|                Homo|   207598|      genus|                   1|             root|                 1|                root|            9605|        Homo|             9604|    Hominidae|                2759|          Eukaryota|
|  207598|           Homininae|     9604|  subfamily|                   1|             root|                 1|                root|               1|        root|             9604|    Hominidae|                2759|          Eukaryota|
|    9604|           Hominidae|   314295|     family|                   1|             root|                 1|                root|               1|        root|             9604|    Hominidae|                2759|          Eukaryota|
|  314295|          Hominoidea|     9526|superfamily|                   1|             root|                 1|                root|               1|        root|                1|         root|                2759|          Eukaryota|
|    9604|           Hominidae|   314295|     family|                   1|             root|                 1|                root|               1|        root|             9604|    Hominidae|                2759|          Eukaryota|
|    9577|         Hylobatidae|   314295|     family|                   1|             root|                 1|                root|               1|        root|             9577|  Hylobatidae|                2759|          Eukaryota|
|    9526|          Catarrhini|   314293|  parvorder|                   1|             root|                 1|                root|               1|        root|                1|         root|                2759|          Eukaryota|
|  314293|         Simiiformes|   376913| infraorder|                   1|             root|                 1|                root|               1|        root|                1|         root|                2759|          Eukaryota|
|  376913|         Haplorrhini|     9443|   suborder|                   1|             root|                 1|                root|               1|        root|                1|         root|                2759|          Eukaryota|
|    9443|            Primates|   314146|      order|                   1|             root|                 1|                root|               1|        root|                1|         root|                2759|          Eukaryota|

"""
