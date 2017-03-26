##  #!/usr/lib/spark/bin/pyspark


## this version will use spark, create and hold s spark context
## and run the taxorpt with queries to spark
## will need to run a submit script for this.
## see spark-eg/submit.sh for that at this point.
## 2016.0727



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
 
########################################## 
### ### ### taxoTraceTbl_df.py ### ### ###     
##########################################
#
# taxoTraceTbl_df.py  [forked from taxorpt_df.py] [2017.0228]
# - forked from taxorpt_df.py when it had a good usable trace table
# - create trace table, utilize data frame, adding 1 column at a time for species, genus, family...
#    -- this column add use UDF, which use sqlite, and seems to take a long time
#    -- thus this trace table is treated as a prep step 
# - trace table saved as parquet
# - parquet will be loaded in future by taxorpt_df.py and do inner join with BLAST output data.     
#    -- report run may only need the paquet and not the sqlite db...

# qacct -j 2729719 : 29 min 744 cores x 10 GB.  special 2k csv process, save to parquet.  no sql query
# qacct -j ... full file... 8+ hours

# taxorpt_df.py  v1 2017.0302 this should work.  fixed the input csv file and with thought cpu time it should produce trace table.
#                             Given how long it takes, the code that read the blast output and do join will 
#                               be done back at taxorpt_df.py
#                v2 2017.0304 will use accession_taxid table to get a uniq list of taxid as seed
#                             then left inner join so that higher level taxid such as genus, family will not become a row in the trace table
#                             Much of that code was coded in taxorpt_df.py (2017.0304).
#                             May need to restruct to share sqlContext...



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
#taxoInputCsv       = "/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv.special2k" 
#outputParquetFile  = "/home/bofh1/pub/taxoTraceTblParquet_allRank_sp2k"

progOutputFile      = "/home/bofh1/pub/taxoTraceTbl_df.distinctTaxid309_opt2.out"
outputParquetFile   = "/home/bofh1/pub/taxoTraceTblParquet_distinctTaxid309_opt2"
taxoInputCsv        = "/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv"                   # ~1.4M rows
#taxoInputCsv        = "/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv.special2k"         
#taxoInputCsv        = "/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv.special10"          # test optimization 2 code first
taxoInput           = taxoInputCsv   
## instead of reading from the csv file, should read from the sqlite DB that pyphy.py use
## as that DB is updated automatically now and most current
# should really replace it with the sqlite db used by pyphy,   ~ line 200
# but may not be so trivial...  if req export to csv, 
# then should get to the source dir of the ncbi download that was used to build the sqlite db.


# most likely won't need to change these:
pythonPathAddition="/home/bofh1/code/hoti-bb/taxo-spark/:/prj/idinfo/prog/local_python_2.7.9/lib/python2.7:/prj/idinfo/prog/local_python_2.7.9/lib/python2.7/site-packages"
sparkContextAppName="sparksql_df_uge_305"
sparkEventLogDir="file:///home/bofh1/pub"


# Input file for accession to taxid lookup table (4 columns)
# change this input file to use smaller input...
acc2taxid_db_file       = "/home/bofh1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv"  # 31GB, 818,215,989 rows, job 2786873 took 23 min to read file and COUNT(*) w/ pe 5    1,222,746 uniq taxid
#acc2taxid_db_file       = "/home/bofh1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv.special200" 
#acc2taxid_db_file       = "/home/bofh1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv.head5k" 
#acc2taxid_db_file       = "/home/bofh1/code/hoti-bb/taxo-spark/db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv.head5" 
acc2taxid_tablename     = "acc_taxid"
taxidUniqList_tablename = "taxid_uniq_list_tab" 


def makeSpkCtx(outFH) :
        print( "   *** hello world.  start of spark job, before declaring a SparkContext..." )
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
        #conf.set( "spark.eventLog.dir", "file:///home/bofh1/pub" )            # will create app-id subdir in there.
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
        return spkCtx
# end makeSpkCtx() 

# fn adopted from taxorpt_df.py 2017.0304 
# how to deal with multiple sparkContext ?  pass as arg?
def load_acc2taxid_datafile(spkCtx,outFH) :
        #outFH = open( progOutputFile, 'w' )

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

        runSqlQuery = 0
        if( runSqlQuery ) :
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
        #end-if( runSqlQuery )

        #print( "  ### trying dataframe filter:" )
        #schemaAccTaxidDf.groupBy("taxid").count().show()   # works.  https://spark.apache.org/docs/1.6.0/sql-programming-guide.html#dataframe-operations
        #taxidUqList = schemaAccTaxidDf.groupBy("taxid")   # https://spark.apache.org/docs/1.6.0/sql-programming-guide.html#dataframe-operations
        # c3po has spark 1.6.0, so does have the above....  but after groupBy, many other DataFrame fn not avail :(
        print( "  ### use dataframe/sql fn filter :" )
        #taxidUqList = schemaAccTaxidDf.select("taxid") # work
        #taxidUqList = schemaAccTaxidDf.sql("SELECT UNIQUE(taxid)") # nope #ref https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html
        taxidUqList = sqlContext.sql("SELECT DISTINCT taxid FROM %s" % acc2taxid_tablename ) 
        taxidUqList.registerTempTable( taxidUniqList_tablename )
        taxidUqList.printSchema()   
        taxidUqList.show(n=15)
        runSqlQuery = 0
        if( runSqlQuery ) :
                sqlCmd = "SELECT COUNT(*) from %s" % taxidUniqList_tablename
                print( "  ### running Query of %s" % sqlCmd )
                print( "  ### running Query of %s" % sqlCmd, file = outFH )
                sqlResult = sqlContext.sql( sqlCmd )  
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                print( myList ) 
                print( myList, file = outFH )    
        #end if

        #spkCtx.stop() 
        #print( "   *** good bye world !!" )
        #print( "   *** good bye world !!", file = outFH )
        #outFH.close()
        #return 0
        return taxidUqList
#end fn: load_acc2taxid_datafile()    # formerly end fn : dont_use_anymore_main_with_spark()







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
        if( 0 ):                            #  9min if let getParentByTaxidRank handle equal cases as well
        #if currentRank == desiredRank :    # 11min.  both tst for the special2k file
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
                elif( parentId == 1 and desiredRank != "root" ) : 
                        # likely went all the way to root/superkingdom... 
                        # this should work out okay...
                        return taxid
                else :
                        return parentId
        return "should_never_get_here_err_Lin99"
# end fn createColForParentOfRank( desiredRank ) 



# mostly a wrapper for UDF, since need to make result into a col for a data frame
def createColForName( taxid ): 
        return getNameByTaxid( taxid )

# end fn createColForName

# adapting from taxoTraceTbl.py (sn-bb)
# in  file: tree*csv                                 # taxonomy tree input
# out file = "/home/bofh1/pub/node2trace_1107.out"   # output from various print cmd
# out file: see: save("trace_table", "parquet")      # data table output (cwd of job)
# create a wide taxonomy trace table by repeated self join of node ID with parent ID.
#def createTaxoTraceTable(taxidUqListDf, spkCtx, outFH) :
def createTaxoTraceTable(taxidUqList, spkCtx, outFH) :
        print( "   ### inside createTaxoTraceTable with shared sparkContext.  checking passed DataFram...  *L285*" )
        #taxidUqListDf.show(n=50)
        #tmpDf = taxidUqList.withColumnRenamed("taxid","tid")
        taxidUqList.printSchema()
        taxidUqList.show(n=50)
        
        sqlContext = SQLContext(spkCtx)         # can only have one instance of SQLContext
        #tabABsqlCtx = SQLContext(spkCtx)       # a second delcaration will result in not so useful object
        print( "   ### hello world spark sql context created" )
        print( "   ### hello world spark sql context created", file = outFH )

        #treeSqlCtx = spkCtx.textFile("tree.table.csv")  # taxonomy tree table (this is req input file)
        #treeSqlCtx = spkCtx.textFile("/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv.special10")  # taxonomy tree table (this is req input file)
        treeSqlCtx = spkCtx.textFile(taxoInput )                                                         # taxonomy tree table (this is req input file)
        partsT = treeSqlCtx.map(lambda l: l.split(","))
        treeTab = partsT.map(lambda p: (p[0], p[1].strip('"'), p[2].strip(), p[3].strip('"')))    
        schemaStringT = "taxid name parent rank"
        fieldsT = [StructField(field_name, StringType(), True) for field_name in schemaStringT.split()]
        schemaT = StructType(fieldsT)
        treeDF  = sqlContext.createDataFrame(treeTab,schemaT) 
        #treeDF.printSchema()
        #treeDF.show()

        # seeding an initial table before any join
        tabT = []       # a list at this pt
        tabT.append(treeDF)     # think this was needed to make tabT a list of the right kind of object

        #treeCsv = tabT      # the source csv as a DF called treeCsv for many future queries to build up trace table
        #tabT[0].registerTempTable("taxoTable")

        tabT[0] = treeDF.withColumnRenamed("taxid", "taxid_T0").withColumnRenamed("name","name_T0").withColumnRenamed("parent","parent_T0").withColumnRenamed("rank","rank_T0")

        print( "   ### Table before join with uniq taxid table  #L310#" )
        tabT[0].printSchema()
        tabT[0].show()

        ### 2017.0304  join with uniq taxid list extracted from acc2taxid table
        tabT[0] = taxidUqList.join(tabT[0], taxidUqList.taxid == tabT[0].taxid_T0, "inner" )
        #syntax ref only: tabT[j] = tabT[j].join(treeDF, tabT[i].parent_T1 == treeDF.taxid, "inner")
        print( "   ### Table *after* join with uniq taxid table  #L310#" )
        tabT[0].printSchema()
        tabT[0].show()
        # join works!! :)

        #tabT[0].select("*").write.save(outputParquetFile, "parquet", "overwrite")           # parquet format seems to be the default.  this found it is 1.1M uniq taxid
        # https://spark.apache.org/docs/1.5.2/sql-programming-guide.html#generic-loadsave-functions     # Generic Load/Save
        # http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes 





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


        # 2017.0227, 0305
        print( "   ### UDF inside loop #L272#" )
        # need to declare user's fn as spark UDF after spark context is created
        udfCreateColForParentOfRank = udf( createColForParentOfRank, StringType() )
        udfCreateColForName = udf( createColForName, StringType() )
        #rankSetFirstIdx = 2
        #rankSetLastIdx  = 3 
        rankSetOffset = 1       # don't want to start as subspecie, so increment by 1
        rankSetFirstIdx = 1 + rankSetOffset     # cuz species is at idx 2
        rankSetLastIdx  = 5 
        #lastIdx = 4
        LX = rankSetLastIdx - rankSetOffset  # no - 1 from for loop index, but need to acc for offset 
        for j in range(rankSetFirstIdx,rankSetLastIdx + 1):     # ie j=2..6
            i = j - 1                 # i comes before j, and i has 1 less than j
            queryRankNum  = RankSet(j).value     # really just j
            queryRankName = RankSet(j).name
            currentLoopNum     = j-rankSetOffset   # should range from (2)-1 to (5+1)-1
            prevLoopNum        = currentLoopNum - 1
            #fieldname1 = "P@%s_udf%s" % (queryRankName,j)
            fieldname1 = "taxid_T%s" % currentLoopNum                        # need simpler name for optimized UDF col creation
            #fieldname1 = "P@%s_udf%s" % (queryRankName,j)
            fieldname2 = "N@%s_udf%s" % (queryRankName,j)
            print( "   ### running j = %s (loopNum %s), adding column named %s" % (j, currentLoopNum, fieldname1) ) 
            print( "   ### using RankSet Idx,name = %s,%s." % (queryRankNum,queryRankName) ) 
            tabT.append(treeDF)
            #tabT[j] = tabT[i].withColumn(       fieldname,   udfCreateColForParentOfRank(  "taxid_T0", lit("genus") ) )       # work essentially
            # now that have uniq taxid list from acc2taxid table, the entries are guaranteed to be species level or below.  
            # can optimize UDF add of column , knowing it will not be querying for genus when given a family taxid
            #tabT[j-rankSetOffset] = tabT[i-rankSetOffset].withColumn(       fieldname1,   udfCreateColForParentOfRank(  "taxid_T0",     lit(queryRankName) ) )
            #tabT[j-rankSetOffset] = tabT[i-rankSetOffset].withColumn(       fieldname1,   udfCreateColForParentOfRank(  "taxid_T%s" % j, lit(queryRankName) ) )
            tabT[currentLoopNum] = tabT[prevLoopNum].withColumn(       fieldname1,   udfCreateColForParentOfRank(  "taxid_T%s" % prevLoopNum, lit(queryRankName) ) )
            tabT.append(treeDF)
            #tabT[j-rankSetOffset] = tabT[j-rankSetOffset].withColumn(       fieldname2,   udfCreateColForName(  fieldname1 ) )
            tabT[currentLoopNum] = tabT[currentLoopNum].withColumn(       fieldname2,   udfCreateColForName(  fieldname1 ) )
            tabT[currentLoopNum].printSchema()
            tabT[currentLoopNum].show(n=3)
        # end for loop
        #tabT[]
        tabT[LX].printSchema()
        tabT[LX].show(n=100)    # default show 20 rows only

        # filtering for species, subspecies, "no rank" may make smaller trace table, but there maybe surprises, like "varietas"
        # some good example entries to spot check: 
        # 441936|Neodiprion scutel...|   270857|species  
        # http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=441936
        # 23 ranks before reaching to "cellular org", it is an arthopod
        # +--------+--------------------+---------+-------------+------------------+--------------------+----------------+------------------+-----------------+--------------------+--------------------+-------------------+
        # |taxid_T0|             name_T0|parent_T0|      rank_T0|    P@species_udf2|      N@species_udf2|    P@genus_udf3|      N@genus_udf3|    P@family_udf4|       N@family_udf4| P@superkingdom_udf5|N@superkingdom_udf5|
        # +--------+--------------------+---------+-------------+------------------+--------------------+----------------+------------------+-----------------+--------------------+--------------------+-------------------+
        # | "taxid"|                name| "parent"|         rank|-1:no rank,species|             unknown|-1:no rank,genus|           unknown|-1:no rank,family|      unknown|-1:no rank,superk...|            unknown|
        # | 1035824|Trichuris sp. ex ...|    36086|      species|           1035824|Trichuris sp. ex ...|           36086|         Trichuris|           119093|  Trichuridae|                2759|          Eukaryota|
        # |    9606|        Homo sapiens|     9605|      species|              9606|        Homo sapiens|            9605|              Homo|             9604|    Hominidae|                2759|          Eukaryota|
        # |  257659|Apterostigma pilo...|    34696|species group|                 1|                root|           34696|      Apterostigma|            36668|          Formicidae|                2759|          Eukaryota|
        # |  233038|Moniliophthora ro...|   221103|     varietas|            221103|Moniliophthora ro...|          221102|    Moniliophthora|           654128|        Marasmiaceae|                2759|          Eukaryota|
        # |  432990|Carex fissa var. ...|   432989|     varietas|            432989|         Carex fissa|           13398|             Carex|             4609|          Cyperaceae|                2759|          Eukaryota|
        # |  218098|Pustularia globul...|   218096|   subspecies|            218096| Pustularia globulus|          218092|        Pustularia|            69557|          Cypraeidae|                2759|          Eukaryota|  
        # | 1477678|Pectis linifolia ...|   169593|     varietas|            169593|    Pectis linifolia|          169590|            Pectis|             4210|          Asteraceae|                2759|          Eukaryota|
        #
        # example of plant, 15 level wasn't enough for full lineage trace
        # |taxid_T0|             name_T0|parent_T0|rank_T0|
        # |  287144|Influenza A virus...|   119210|no rank|
        # 
        # maybe problem, or 
        # maybe 54275 is obsolete which is why is not findable
        # |   54275|           Rhiniinae|     7371|    subfamily|-1:no rank,species|             unknown|-1:no rank,genus|             unknown|-1:no rank,family|             unknown|-1:no rank,superk...|            unknown|




        # saving data for future use... 
        #tabT[LX].printSchema()      
        #tabT[LX].show()      
        #tabT[lastIdx-1].collect()       # lastIdx=15, .collect() crashed!!
        ## output file: saving to parquet file.  loc: cwd of job.  it is actually a dir with many files
        ## save works, but need to ensure col names are uniq
        #~ disabling save for now, as don't currently use during dev and want to save time
        tabT[LX].select("*").write.save(outputParquetFile, "parquet", "overwrite")           # parquet format seems to be the default.  
        #tabT[LX].select("*").where("rank_T0='species'").write.save("taxoTraceTblParquet_species_sp2k", "parquet", "overwrite")           # this works too, filtering result to smaller set
        # https://spark.apache.org/docs/1.5.2/sql-programming-guide.html#generic-loadsave-functions     # Generic Load/Save
        # http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes 

        ## printing rdd:
        ## https://spark.apache.org/docs/latest/programming-guide.html#printing-elements-of-an-rdd
        ## maybe of interest in removing extra column...
        ## https://blogs.msdn.microsoft.com/azuredatalake/2016/02/10/pyspark-appending-columns-to-dataframe-when-dataframe-withcolumn-cannot-be-used/


        #tabT[lastIdx-1].registerTempTable("trace_table")
        tabT[LX].registerTempTable("taxoTraceTblParquet")
        ## not sure why get  pyspark.sql.utils.AnalysisException: u'Table not found: taxoTraceTblParquet;'

        # most test query run by the trace_load.py 
        runSqlQuery = 0
        if( runSqlQuery ) : 
                print( "   ### (*) * running SQL Query ... " )
                print( "   ### (*) * running SQL Query ... ", file = outFH )

                # this query took 37 min (9-11 of which is for creating the trace table above)
                sqlCmd1 = "SELECT * FROM taxoTraceTblParquet WHERE rank_T0 = 'species'" # spark does NOT allow for ; at end of SQL !!
                sqlResult1 = sqlContext.sql( sqlCmd1 )
                myList = sqlResult1.collect()            # need .collect() to consolidate result into "Row"   ... still needed?  takes a long time... hmm... may not be collect taking long... maybe changed UDF...
                print( "   ### sqlQuery '%s': ###" % sqlCmd1, file = outFH )
                print( "   ### myList is: %s", myList )                        # COUNT() : [Row(_c0=1,402,066)]   # full csv   
                print( "   ### myList is: %s", myList, file = outFH )          # COUNT() : [Row(_c0=2034)]        # special2k input file
        # end if


        # without collect below, it broke.  run time = 50 min (48 cores x 24 GB)
        # skipping these sql queries, and it still takes a long time, so must be the col addition queries...
        runSqlQuery = 0
        if( runSqlQuery ) : 
                print( "   ### (*) * running SQL Query ... " )
                print( "   ### (*) * running SQL Query ... ", file = outFH )


                sqlCmd1 = "SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet" # spark does NOT allow for ; at end of SQL !!
                sqlResult1 = sqlContext.sql( sqlCmd1 )
                myList = sqlResult1.collect()            # need .collect() to consolidate result into "Row"   ... still needed?  takes a long time... hmm... may not be collect taking long... maybe changed UDF...
                                                        # job 2717927  died cuz unable to finish collect.  
                print( "   ### sqlQuery '%s': ###" % sqlCmd1, file = outFH )
                print( "   ### myList is: %s", myList )                        # COUNT() : [Row(_c0=1,402,066)]   # full csv   
                print( "   ### myList is: %s", myList, file = outFH )          # COUNT() : [Row(_c0=2034)]        # special2k input file
                print( "   ### sqlResult is: %s", sqlResult1 )                  # sqlResult is: %s DataFrame[_c0: bigint]
                print( "   ### sqlResult is: %s", sqlResult1, file = outFH )

                sqlCmd2 = "SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet where rank_T0 = 'species'" # spark does NOT allow for ; at end of SQL !!
                sqlResult2 = sqlContext.sql( sqlCmd2 )
                myList = sqlResult2.collect()            # need .collect() to consolidate result into "Row"   
                print( "   ### sqlQuery '%s': ###" % sqlCmd2, file = outFH )
                print( "   ### myList is: %s", myList )                        # 
                print( "   ### myList is: %s", myList, file = outFH )          # 

                sqlCmd3 = "SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet where rank_T0 = 'no rank'" # spark does NOT allow for ; at end of SQL !!
                sqlResult3 = sqlContext.sql( sqlCmd3 )
                myList = sqlResult3.collect()            # need .collect() to consolidate result into "Row"   
                print( "   ### sqlQuery '%s': ###" % sqlCmd3, file = outFH )
                print( "   ### myList is: %s", myList )                        # 
                print( "   ### myList is: %s", myList, file = outFH )          # 
        #end: if( runSqlQuery )


        #if( sqlResult.count() > 1 ):
        #        print( "Houston, we got more than one element returned!")



        ##print( "   ### good bye world !!" )
        ##print( "   ### good bye world !!", file = outFH )
        ##outFH.close()
        ##spkCtx.stop()   # need to close off spark context to avoid err message in output/log
        ##exit 
        return
#end createTaxoTraceTable() 






### end of all fn definition, begin of main program flow.

def main() :
        outFH = open( progOutputFile, 'w' )
        print( "   *** hello world.  main() before creating 'SHARED' SparkContext..." )
        print( "   *** hello world.  main() before creating 'SHARED' SparkContext...", file = outFH )
        spkCtx = makeSpkCtx(outFH)
        taxidUqList = load_acc2taxid_datafile(spkCtx,outFH)
        #print( "   *** SparkContext used in call, and got a DF obj back, checking it #L537#..." )
        #print( "   *** SparkContext used in call, and got a DF obj back, checking it #L537#...", file = outFH )
        #taxidUqList.printSchema()   
        #taxidUqList.show(n=2)
        createTaxoTraceTable(taxidUqList,spkCtx,outFH)         # this worked before, but now need to share spkContext 
        print( "   ### good bye world !!" )
        print( "   ### good bye world !!", file = outFH )
        outFH.close()
        spkCtx.stop()   # need to close off spark context to avoid err message in output/log
# end main()

main()



"""
sample cached results, actuall run produce better trace table by now.
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
