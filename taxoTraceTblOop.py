## #!/usr/lib/spark/bin/spark-submit  

## #!/usr/lib/spark/bin/spark-shell    # this load a scala shell


## spark_acc2taxid_3.py    this version does sql lookup one row at a time.  works, but slow.
## spark_acc2taxid_4.py    this version use join on dataframe, should be done in ~5 min as per Victor Hong estimate
## spark_acc2taxid_5.py    		(can ignore this version)
## node2trace.py           create taxonomy table, really wide, using dataframe
## trace_load.py           load parquet stored by node2trace.py
## taxoTraceTbl.py         taxonomy heritage/trace table with predefined parent ranks.  adopt from node2trace.py.
##                         should only need public ncbi taxonomy data, no priv data/structure.
##                         2017.0220 UDF for getParentOfRank has some code, can interact in DF manner, 
##                              but road block due to lack of access to sqlContext for query
##                         2017.0221 dev a new approach... use 20x self join table and 
##                              add columns of desired Parent/Rank from there
##                         2017.0221a *sigh*  no easy way to feed all the data to the UDF
##                              moving to use OOP approach,
##                              which is what pyphy_spark was already doing.  just need this driver to use DF to see if it parallelizes...
## taxoTraceTblOop.py      2017.0221b OOP version modeled after taxorpt_spar.py
##                                    Could not get it to work.  would need SQLcontext inside UDF, which complains about a sqlcontext inside another.
## taxorpt_df.py           2017.0224  abandoning these taxoTraceTbl*py files.  work moving to taxorpt_df.py.

                     



from __future__         import print_function
#from pyspark            import SparkContext, SparkConf
from pyspark            import SparkContext, SparkConf
from pyspark.sql        import SQLContext, Row
from pyspark.sql.functions import lit, udf
#from pyspark.sql.functions import udf
from pyspark.sql.types     import StringType, StructField, StructType  # maybe too many to define :)
#from pyspark.sql.types     import *             # may as well import *, needed as return type for UDF.  ref: loc 4939 spark book
#from pyspark.sql.functions import rand          # random,for debug only

import taxonomyTreeTableOop

# define a future  UDF so that it can be called from data frame (join) fn
# http://stackoverflow.com/questions/33681487/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark 
# see 2nd best answer with 18 pts
# also see registerFunction http://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html#pyspark.sql.SQLContext.registerFunction
# UDF http://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html#pyspark.sql.functions.udf
# maybe something useful here to pass whole row as struct to udf: http://stackoverflow.com/questions/36584812/pyspark-row-wise-function-composition/36596450

def _prob_abandone__createColForParentAtRank( desiredRank ): 
        # first param, desiredRank, is at what rank level the parent being sought at is (eg species, genus)
        if    desiredRank == "species" : 
                # working on species, look at the *_T0 colums for current info
                # hmm... need to have access to the whole Row...
                return "tba"
# end fn createColForParentAtRank( desiredRank ) 


## plan to abandone fn below, the resulting UDF can't do what is desired due to lack of access to sqlContext
def _abandone__getParentOfRank( desiredRank, currentTaxId, currentName, currentParent, currentRank ) : 
#def getParentOfRank( desiredRank, currentTaxId, currentName, currentParent, currentRank, taxoTSqlContext ) : 
#def getParent( currentRank ) : 
        # first param, desiredRank, is at what rank level the parent being sought at is (eg species, genus)
        # next 4 params are the content of the current row (whose parent is being sought)
        # once registered as udf, output goes to la la land !! ??
        #print( "   ###dbg: desiredRank, currentTaxId, currentName, currentParent, currentRank ") 
        #print( "   ###dbg: %s, %s, %s, %s, %s "  % (desiredRank, currentTaxId, currentName, currentParent, currentRank ) )
        #return len( desiredRank )       # 7
        if    desiredRank == currentRank : 
                # this is special case, say, at species level (checking if sub-species) and so seeking for species
                #return currentRank
                return [currentTaxId, currentName, currentParent, currentRank ] 
                #df = sqlContext.createDataFrame(  [(currentTaxId, currentName, currentParent, currentRank)] )
                #return df
        else : 
                #return "need lookup"
                #return (currentParent)
                return [currentParent]
                #taxoTree.select("parent", df.taxid=currentParent )
                #sqlResult = taxoTSqlContext.sql("SELECT parent from taxoTbl where parent = %s" % currentParent) 
                #return sqlResult
#getParent()-end



# these may still be applicable to main_with_spark_oo() :
# in  file: tree*csv                                 # taxonomy tree input
# out file = "/home/bofh1/pub/node2trace_1107.out"   # output from various print cmd
# out file: see: save("trace_table", "parquet")      # data table output (cwd of job)
# create a wide taxonomy trace table by repeated self join of node ID with parent ID.
# 
## modeled after taxorpt_spark.py  main_with_spark_pyphy_oo
def main_with_spark_oo():
        #print( "hello OOP :) " )
        outfile = "/home/bofh1/pub/taxoTraceTblOO.out"
        outFH = open( outfile, 'w' )
        print( "   *** hello OOP world :)  start of spark job, before declaring a SparkContext...", file = outFH )
        print( "   *** outfile is set to %s " % outfile, file = outFH )

        ## ch 8 of Learning Spark
        conf = SparkConf()
        conf.set( "spark.app.name", "taxoTraceTblOO_2017_0216_c3p")              # better off to leave conf as spark-submit params
        #conf.set( "spark.master", "local" )                                     # if use this, not in history server, but at least see some better output!
        #conf.set( "spark.master", "yarn" )
        #conf.set( "spark.submit.deployMode", "cluster" )
        conf.set( "spark.eventLog.enabled", False )                             # maybe spark 1.5 don't support these, can't get em to work :(
        conf.set( "spark.eventLog.dir", "file:///home/bofh1/pub" )             # will create app-id subdir in there.



        ## http://stackoverflow.com/questions/24996302/setting-sparkcontext-for-pyspark
        ## https://spark.apache.org/docs/1.5.0/configuration.html
        #sc = SparkContext( 'local', 'taxorpt_pyspark_local_0727' )
        #sc = SparkContext( appName='taxorpt_spark_yarn_0812_noSQLite' )         # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        #sc = SparkContext( appName='taxorpt_spark_c3p_0216_noSQLite' )          # taxorpt w/ sqlite runs in yarn mode, get .html output, but UI don't capture stdout or stderr, hard to debug!
        spkCtx = SparkContext( conf=conf )                                          # conf= is needed for spark 1.5
        # for now need to run in local mode, don't know why can't get output from yarn mode
        print( "   ### *** hello world sparkContext created #L106#" )
        print( "   ### *** hello world sparkContext created #L106#", file = outFH )

        # turn off loggin  http://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-pyspark 
        #log4j = spkCtx._jvm.org.apache.log4j
        #log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)
        # log4j settings is pretty good in trimming the log messages, but next one is simpler and does same thing
        spkCtx.setLogLevel("FATAL")

        ## here really call the taxorpt processing  (test code for now)

        taxoHandle = taxonomyTreeTableOop.taxonomyTreeTable_class(spkCtx,outFH)  # tba
        testTaxId = "287144"
        print( "   ### about to make oop call query #L120#" )
        print( "   ### about to make oop call query #L120#", file = outFH )
        testRow = taxoHandle.getRow(testTaxId)      # this works
        print( testRow )


        #print( "   ### *** end of main_with_spark_oo for now  #L121#")
        #print( "   ### *** end of main_with_spark_oo for now  #L121#", file = outFH )


        ## ++ FIXME ++
        ## previous incarnation found that can't have two SQLContext, but that was in same fn.
        ## this prob still fail even in a diff OO, but let's see...   
        sqlContext = SQLContext(spkCtx)         # can only have one instance of SQLContext
        #tabABsqlCtx = SQLContext(spkCtx)       # a second delcaration will result in not so useful object
        print( "   ### hello world spark sql context (2nd) created *L140*" )
        print( "   ### hello world spark sql context (2nd) created *L140*", file = outFH )


        

        #treeSqlCtx = spkCtx.textFile("tree.table.csv")  # taxonomy tree table (this is req input file)
        #treeSqlCtx = spkCtx.textFile("/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv.special10")  # taxonomy tree table (this is req input file)
        treeSqlCtx = spkCtx.textFile("/home/bofh1/code/hoti-bb/taxo-spark/db/tree.table.csv")  # taxonomy tree table (this is req input file)
        partsT = treeSqlCtx.map(lambda l: l.split(","))
        treeTab = partsT.map(lambda p: (p[0], p[1].strip('"'), p[2].strip(), p[3].strip('"')))    
        schemaStringT = "taxid name parent rank"
        fieldsT = [StructField(field_name, StringType(), True) for field_name in schemaStringT.split()]
        schemaT = StructType(fieldsT)
        treeDF  = sqlContext.createDataFrame(treeTab,schemaT) 
        print( "   ### sucessfully created a 2nd sqlContext #L149#", file = outFH )

        treeDF.printSchema()
        treeDF.show()

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
        lastIdx = 2
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

        #tabT[lastIdx-1].registerTempTable("trace_table")
        tabT[LX].registerTempTable("taxoTraceTblParquet")
        print( "   ### (*) * running SQL Query COUNT... " )
        print( "   ### (*) * running SQL Query COUNT... ", file = outFH )
        sqlResult = sqlContext.sql("SELECT COUNT( taxid_T0 ) FROM taxoTraceTblParquet") # spark does NOT allow for ; at end of SQL !!
        print( "   ### sqlResult: ###", file = outFH )
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        print( "   ### myList is: %s", myList )                        # [Row(_c0=1402066)]   # took about 20 sec (53sec if omit .show() after all the joins)
        print( "   ### myList is: %s", myList, file = outFH )          # this works too! 
        print( "   ### sqlResult is: %s", sqlResult )                  # sqlResult is: %s DataFrame[_c0: bigint]
        print( "   ### sqlResult is: %s", sqlResult, file = outFH )
        #if( sqlResult.count() > 1 ):
        #        print( "Houston, we got more than one element returned!")



        # 2017.0221 
        #LX = lastIdx - 1         # for quick access at last join table, defined earlier now
        print( "   ### new UDF most 2-self-joins  #L208#" )
        print( "   ### new UDF most 2-self-joins  #L208#", file = outFH )
        # need to declare user's fn as spark UDF after spark context is created
        #testRow = taxoHandle.getRow(testTaxId)      # this works in test above
        udfGetRow = udf( taxoHandle.getRow, StringType() )
        #udfGetRow = udf( taxonomyTreeTableOop.getRow, StringType() )   # module don't have fn getRow :(
        tmpT5 = tabT[LX].withColumn( "udf_1",   udfGetRow (  "parent_T0" ) )
        #udfCreateColForParentAtRank = udf( createColForParentAtRank, StringType() )
        #tmpT5 = tabT[LX].withColumn( "udf_1",   createColForParentAtRank(   lit("species") ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   createColForParentAtRank(   lit("species") )
        # hmm... should I base it on tabT[0] and create a new series of table?
        #tmpT5 = tabT[0].withColumn( "udf_id",   udfGetParent(   "taxid_T0", "name_T0", "parent_T0", "rank_T0" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   udfGetParentOfRank(   lit("species"), "taxid_T0", "name_T0", "parent_T0", "rank_T0" ) )
        #tmpT5 = tabT[0].withColumn( "udf_1",   udfGetParentOfRank(   lit("species"), "taxid_T0", "name_T0", "parent_T0", "rank_T0", sqlContext ) )
        # above don't work with sqlContext as param.... not sure what technicality is missing... 
        # to add four column may need to create a new DF then join() 
        # or maybe udf registration don't have to return StringType but a DF? RDD?
        #tmpT5.printSchema()
        #tmpT5.show()
        tabT[LX] = tmpT5
        tabT[LX].printSchema()
        tabT[LX].show()


        print( "   ### good bye world !!" )
        print( "   ### good bye world !!", file = outFH )
        outFH.close()
        spkCtx.stop()   # need to close off spark context to avoid err message in output/log
        exit 
# end main_with_spark_oo()   was end createTaxoTraceTable() 


# main execution of this file
##createTaxoTraceTable() 
main_with_spark_oo()


"""
## getParentOfRank of this version can produce result like
## (removed)
"""
