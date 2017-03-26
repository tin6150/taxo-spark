### taxorpt_df.py.2017.0227.beforeReorg ###

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


# need to set PYTHONPATH in the spark-submit script, 
# BUT can't do it here, it has no effect
#if 'PYTHONPATH' not in os.environ:
#        os.environ['PYTHONPATH'] = "/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/"
#PYTHONPATH = os.environ['PYTHONPATH']
#sys.path.insert(1, os.path.join(PYTHONPATH, "/usr/prog/python/2.7.9-goolf-1.5.14-NX/lib/python2.7/site-packages/"))
#sys.path.insert(1, os.path.join(PYTHONPATH, "/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages/", "/usr/prog/python/2.7.9-goolf-1.5.14-NX/lib/python2.7/site-packages/"))
#sys.path.insert(1, os.path.join(PYTHONPATH, "/home/hoti1/code/svn/taxo-spark/", "/home/hoti1/local_python_2.7.9/lib/python2.7/site-packages/" ))
#sys.path.insert(1, os.path.join("/home/hoti1/code/hoti-bb/taxo-spark/", "/home/hoti1/local_python_2.7.9/lib/python2.7/site-packages/" ))






# from taxoTraceTbl.py (sn-bb)
# this will become UDF
# will use pyphy/pyphy_ext/sqlite, essentially serial python, to lookup parent.
# first param, desiredRank, is at what rank level the parent being sought at is (eg species, genus)
def createColForParentOfRank( taxid, desiredRank ): 
        # new alg for real use by taxorpt_df:
        currentRank = "need to get pyspark to import pyphy..."
        currentRank = getRankByTaxid(taxid)
        #returnString = "%s,%s=%s" % (taxid, desiredRank, currentRank )
        #return returnString # tmp     # finally the IO is working out!!
        #getRankNameByTaxid(taxid)  # this seems to get parent... not sure why named that wasy in phyphy_ext...
        # ranking = compareRank( currentRank, desiredRank )
        # 0 if equal
        # -1 if currentRank is lower
        # +1 if currentRank is higher than desiredRank (which should then stay put)
        # if ranking > 0 : 
        # ==> NO <==  
        # don't need to be so careful.  if find out is lower, then need the parent rak anyway
        # so, just ask for parent, and only if not found, then handle it differently
        # the == case seems handled by getParentByTaxidRank already
        #if( 0 ):               # if same rank, seems to take too long if use getParentByTaxidRank...!
        if currentRank == desiredRank :
                return taxid
                #returnString = "%s==%s" % (currentRank, desiredRank )
                #return returnString
        else :
                #returnString = "%s v %s" % (currentRank, desired )
                #return returnString # tmp     # finally the IO is working out!!
                # assume/try to get parent rank
                # check to see if able to get it, 
                # if can't, then handle as no parent... which should be treated as skip 
                # and so can be same case as currentRank == desiredRank
                # recursion maybe the way...
                # getParentByTaxidRank was in pyphy_ext.py, hope rank is just the string...
                #parentId = "fn call to getParentByTaxidRank seems to return err of html fn not found... may still need to fiddle with PYTHONPATH :( "
                parentId = getParentByTaxidRank( taxid, desiredRank )
                if( parentId == -1 ) : 
                        # handle the return value of if it is "none" or "not found"...
                        returnString = "-1:%s,%s" % (currentRank, desiredRank )
                        return returnString
                else :
                        return parentId
        return "should_never_get_here_err_Lin99"
# end fn createColForParentOfRank( desiredRank ) 




# adapting from taxoTraceTbl.py (sn-bb)
# in  file: tree*csv                                 # taxonomy tree input
# out file = "/home/bofh1/pub/node2trace_1107.out"   # output from various print cmd
# out file: see: save("trace_table", "parquet")      # data table output (cwd of job)
# create a wide taxonomy trace table by repeated self join of node ID with parent ID.
def createTaxoTraceTable() :
        outfile = "/home/hoti1/pub/taxoTraceTbl.out"
        outFH = open( outfile, 'w' )
        #outFH.write( "Test output to file from spark\n" )
        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark 

        print( "   ### can i call pyphy fn here outside of UDF??   yes, so it is UDF not finding it.  hope setExecutorEnv fixes it ###")
        tmpTest = getRankByTaxid("9606")
        print(tmpTest)

        ##spkCtx = SparkContext( 'local', 'tin_pyspark_0724_local' )
        conf = SparkConf().setAppName( 'sparksql_df_uge' )
        conf.set( "spark.eventLog.enabled", True )                             # maybe spark 1.5 don't support these, can't get em to work :(
        conf.set( "spark.eventLog.dir", "file:///home/hoti1/pub" )             # will create app-id subdir in there.
        #conf.setExecutorEnv( "PYTHONPATH", "/home/hoti1/code/hoti-bb/taxo-spark/" )             # 
        conf.setExecutorEnv( "PYTHONPATH", "/home/hoti1/code/hoti-bb/taxo-spark/:/db/idinfo/prog/local_python_2.7.9/lib/python2.7:/db/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages" )             # 

        #conf = conf.
        #https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python to quiet log4j
        #spkCtx = SparkContext( appName='pyspark_uge' )
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
        print( "   ### hello world spark sql context created" )
        print( "   ### hello world spark sql context created", file = outFH )

        #treeSqlCtx = spkCtx.textFile("tree.table.csv")  # taxonomy tree table (this is req input file)
        #treeSqlCtx = spkCtx.textFile("/home/hoti1/code/hoti-bb/taxo-spark/db/tree.table.csv.special10")  # taxonomy tree table (this is req input file)
        #treeSqlCtx = spkCtx.textFile("/home/hoti1/code/hoti-bb/taxo-spark/db/tree.table.csv")  # taxonomy tree table (this is req input file)
        treeSqlCtx = spkCtx.textFile("/home/hoti1/code/hoti-bb/taxo-spark/db/tree.table.csv.special2k")  # taxonomy tree table (this is req input file)
        partsT = treeSqlCtx.map(lambda l: l.split(","))
        treeTab = partsT.map(lambda p: (p[0], p[1].strip('"'), p[2].strip(), p[3].strip('"')))    
        schemaStringT = "taxid name parent rank"
        fieldsT = [StructField(field_name, StringType(), True) for field_name in schemaStringT.split()]
        schemaT = StructType(fieldsT)
        treeDF  = sqlContext.createDataFrame(treeTab,schemaT) 
        #treeDF.printSchema()
        #treeDF.show()

        # seeding an initial table before any join
        tabT = []
        tabT.append(treeDF)     

        #treeCsv = tabT      # the source csv as a DF called treeCsv for many future queries to build up trace table
        #tabT[0].registerTempTable("taxoTable")

        tabT[0] = treeDF.withColumnRenamed("taxid", "taxid_T0").withColumnRenamed("name","name_T0").withColumnRenamed("parent","parent_T0").withColumnRenamed("rank","rank_T0")


        ### *** 2017.0219
        ### start with just appending a simple calculated column to a simple table... eg length_of_string( species ) or a count(*)... 
        #   how to add col: http://stackoverflow.com/questions/33681487/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark
        #print( "   ### Table before and after added column #L58#" )
        #print( "   ### Table before and after added column #L58#", file = outFH )
        #tabT[0].printSchema()
        #tmpT  = tabT[0].withColumn( "parent_T1", lit("tba") ).withColumn( "rank_T1", lit("tba")  # add two columnX
        #tmpT4 =   tmpT3.withColumn( "new5_name", getattr(tabT[0], "name_T0") )           # getattr returns a column
        #tmpT.printSchema()
        #tmpT.show()

        #print( "   ### trying UDF #L92#" )
        # need to declare user's fn as spark UDF after spark context is created
        #udfGetParentOfRank = udf( getParentOfRank, StringType() )
        #tmpT5 = tabT[0].withColumn( "udf_id",   udfGetParent(   "taxid_T0", "name_T0", "parent_T0", "rank_T0" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   udfGetParentOfRank(   lit("species"), "taxid_T0", "name_T0", "parent_T0", "rank_T0" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   udfGetParentOfRank(   lit("species"), "taxid_T0", "name_T0", "parent_T0", "rank_T0", sqlContext ) )
        # above don't work with sqlContext as param.... not sure what technicality is missing... 
        # to add four column may need to create a new DF then join() 
        # or maybe udf registration don't have to return StringType but a DF? RDD?
        #tmpT5.printSchema()
        #tmpT5.show()

        #print( "   ### reassign back tabT[0] = tmpT... #L70#" )
        #tabT[0] = tmpT5          # can reassign it back :_
        #tabT[0].printSchema()
        #tabT[0].show()


        # join tree table with itself repeatedly to get parent info, forming a lineage in each row
        # but this can go on a long chain.  lastIdx of 16 still only ended in phylum for
        # 441936|Neodiprion scutel...|   270857|species  
        # http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=441936
        # 23 ranks before reaching to "cellular org", it is an arthopod
        ##lastIdx = 16
        lastIdx = 1       # 1 = skip join for now, 2 = join once (get to name_T1 column)
        LX = lastIdx - 1              # for quick access at last join table     ##defined in 2017.0221
        for j in range(1,lastIdx):    # range(2,3) produces [2], so exclude last index.  think of calculus [2,3)
            i = j - 1                 # i comes before j, and i has 1 less than j
            #print( "running i = %s" % i ) 
            print( "running j = %s" % j ) 
            fieldname = "parent_T%s" % i
            #print( "fieldname is set to %s" % fieldname ) 
            #                                           *** T1 is wrong below .   need something dynamic, equiv to T[i]
            #tabT[j] = tabT[j].join(treeDF, tabT[i].parent_T1 == treeDF.taxid, "inner")
            tabT.append(treeDF)
            ###
            ### *** 2017.0219
            ### *** code are still old.  most likely won't need to use for loop, at least not for number, maybe for enum class...
            ### but prob should start with single species to genus merge first.
            ### the join condition is what need to be rewritten with a complex fn that does lookup.  how to do that?? 
            ###
            tabT[j] = tabT[i].join(treeDF, getattr(tabT[i], fieldname) == treeDF.taxid, "inner") # http://stackoverflow.com/questions/31843669/how-to-pass-an-argument-to-a-function-that-doesnt-take-string-pyspark
            tabT[j] = tabT[j].withColumnRenamed("taxid", "taxid_T%s" % j).withColumnRenamed("name","name_T%s" % j).withColumnRenamed("parent","parent_T%s" % j).withColumnRenamed("rank","rank_T%s" % j)
            #tabT[j].show()      
        #end for loop


        # saving data for future use... 
        tabT[LX].printSchema()      
        #tabT[lastIdx-1].show()      
        tabT[LX].show()      
        #tabT[lastIdx-1].collect()       # lastIdx=15, .collect() crashed!!
        ## output file: saving to parquet file.  loc: cwd of job.  it is actually a dir with many files
        ## save works, but need to ensure col names are uniq
        #~ disabling save for now, as don't currently use during dev and want to save time
        #~~tabT[LX].select("*").write.save("taxoTraceTblParquet", "parquet", "overwrite")           # parquet format seems to be the default.  took 3.1 min
        # https://spark.apache.org/docs/1.5.2/sql-programming-guide.html#generic-loadsave-functions     # Generic Load/Save
        # http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes 

        ## printing rdd:
        ## https://spark.apache.org/docs/latest/programming-guide.html#printing-elements-of-an-rdd
        ## maybe of interest in removing extra column...
        ## https://blogs.msdn.microsoft.com/azuredatalake/2016/02/10/pyspark-appending-columns-to-dataframe-when-dataframe-withcolumn-cannot-be-used/



        # 2017.0221 
        #LX = lastIdx - 1         # for quick access at last join table, defined earlier now
        #print( "   ### new UDF most 20-self-joins  #L208#" )
        # need to declare user's fn as spark UDF after spark context is created
        udfCreateColForParentOfRank = udf( createColForParentOfRank, StringType() )
        tmpT5 = tabT[LX].withColumn(       "u5_spe",   udfCreateColForParentOfRank(  "taxid_T0", lit("species") ) )
        tmpT5 =    tmpT5.withColumnRenamed("u5_spe", "taxid_T%s" % "1")
        tmpT5 =    tmpT5.withColumn(       "u5_gen",   udfCreateColForParentOfRank(  "taxid_T0", lit("species") ) )      # more efficient if use taxid_T1 when done correctly
        tmpT6 =    tmpT5.withColumn(       "u6_gen",   udfCreateColForParentOfRank(  "taxid_T1", lit("genus") ) )      # more efficient if use taxid_T1 when done correctly
        tmpT6 =    tmpT6.withColumnRenamed("u6_gen", "taxid_T%s" % "2")
        tmpT6 =    tmpT6.withColumn(       "u6_gen",   udfCreateColForParentOfRank(  "taxid_T0", lit("genus") ) )       # tmp
        tmpT7 =    tmpT6.withColumn(       "u7_fam",   udfCreateColForParentOfRank(  "taxid_T2", lit("family") ) )
        tmpT7 =    tmpT7.withColumnRenamed("u7_fam", "taxid_T%s" % "3")
        tmpT7 =    tmpT7.withColumn(       "u7_fam",   udfCreateColForParentOfRank(  "taxid_T0", lit("family") ) )      # tmp
        tmpT8 =    tmpT7.withColumn(       "u8_ord",   udfCreateColForParentOfRank(  "taxid_T3", lit("order") ) )
        tmpT8 =    tmpT8.withColumnRenamed("u8_ord", "taxid_T%s" % "4")

        
        #tmpT5 = tabT[LX].withColumn( "udf_1",   udfCreateColForParentAtRank(   "taxid_T0" ) )
        #tmpT5 = tabT[LX].withColumn( "udf_1",   udfCreateColForParentAtRank(   "species" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   XXcreateColForParentAtRank(   lit("species") )
        # hmm... should I base it on tabT[0] and create a new series of table?
        #tmpT5 = tabT[0].withColumn( "udf_id",   udfGetParent(   "taxid_T0", "name_T0", "parent_T0", "rank_T0" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   udfGetParentOfRank(   lit("species"), "taxid_T0", "name_T0", "parent_T0", "rank_T0" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   udfGetParentOfRank(   lit("species"), "taxid_T0", "name_T0", "parent_T0", "rank_T0", sqlContext ) )
        # above don't work with sqlContext as param.... not sure what technicality is missing... 
        # to add four column may need to create a new DF then join() 
        # or maybe udf registration don't have to return StringType but a DF? RDD?
        
        print( "   ### Result from column additions ... " )
        tabT[LX] = tmpT8
        tabT[LX].printSchema()
        tabT[LX].show()

        #tabT[lastIdx-1].registerTempTable("trace_table")
        tabT[LX].registerTempTable("taxoTraceTblParquet")
        print( "   ### (*) * running SQL Query ... " )
        print( "   ### (*) * running SQL Query ... ", file = outFH )

        sqlCmd1 = "SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet" # spark does NOT allow for ; at end of SQL !!
        sqlResult1 = sqlContext.sql( sqlCmd1 )
        #myList = sqlResult1.collect()            # need .collect() to consolidate result into "Row"   ... still needed?  takes a long time... hmm... may not be collect taking long... maybe changed UDF...
                                                # job 2717927  died cuz unable to finish collect.  
        print( "   ### sqlQuery '%s': ###" % sqlCmd1, file = outFH )
        print( "   ### myList is: %s", myList )                        # COUNT() : [Row(_c0=1,402,066)]   # full csv   
        print( "   ### myList is: %s", myList, file = outFH )          # COUNT() : [Row(_c0=2034)]        # special2k input file
        print( "   ### sqlResult is: %s", sqlResult1 )                  # sqlResult is: %s DataFrame[_c0: bigint]
        print( "   ### sqlResult is: %s", sqlResult1, file = outFH )


        sqlCmd2 = "SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet where rank_T0 = 'species'" # spark does NOT allow for ; at end of SQL !!
        sqlResult2 = sqlContext.sql( sqlCmd2 )
        #myList = sqlResult2.collect()            # need .collect() to consolidate result into "Row"   ... still needed? 
        print( "   ### sqlQuery '%s': ###" % sqlCmd2, file = outFH )
        print( "   ### myList is: %s", myList )                        # 
        print( "   ### myList is: %s", myList, file = outFH )          # 

        sqlCmd3 = "SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet where rank_T0 = 'no rank'" # spark does NOT allow for ; at end of SQL !!
        sqlResult3 = sqlContext.sql( sqlCmd3 )
        #myList = sqlResult3.collect()            # need .collect() to consolidate result into "Row"   ... still needed? 
        print( "   ### sqlQuery '%s': ###" % sqlCmd3, file = outFH )
        print( "   ### myList is: %s", myList )                        # 
        print( "   ### myList is: %s", myList, file = outFH )          # 


        #if( sqlResult.count() > 1 ):
        #        print( "Houston, we got more than one element returned!")



        print( "   ### good bye world !!" )
        print( "   ### good bye world !!", file = outFH )
        outFH.close()
        spkCtx.stop()   # need to close off spark context to avoid err message in output/log
        exit 
#end createTaxoTraceTable() 










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
        ### file2acc... is where action begin, and need to get the spark handle...   TODO ++
        uniqAccVerCount    = len(accVerFreqList) 
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


"""
def dont_use_anymore_main_with_spark():
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
        #sc = SparkContext( 'local', 'taxorpt_pyspark_local_mmdd_oldFn' )
        sc = SparkContext( appName='taxorpt_spark_yarn_mmdd_oldFnNoLongerUsed_wSQLite' )         # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        #sc = SparkContext( conf=conf )                                          # conf= is needed for spark 1.5
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

# end fn : dont_use_anymore_main_with_spark()
"""
        



def main():
        args = process_cli()
        run_taxo_reporter( args ) 
# main()-end

### end of all fn definition, begin of main program flow.
#main()
#main_with_spark()
##main_with_spark_pyphy_oo()      # expect to be called with an input (and output) file arg.  (?) or uge/spark may wait forever for input in stdin...


# main execution of this file for taxorpt_spark.py 2017.0224
createTaxoTraceTable() 

