
## #!/bin/env python
## #!/prog/python/3.4.2-goolf-1.5.14-NX/bin/python3   (code is not python3 compatible).
##  when deploy as  python module, don't need the magic line 


## pyphy_spark.py, adapted from pyphy.py
## will need SparkContext for the main Accession->TaxID query eg getTaxIdByAccVer(gi) 
## but sqlite3 will still be used for the small thing (name/nodes lookup, as that had some special sause in the load)

## version 1.1, with queries to Spark instead of sqlite
##              OOP should be completed 
##              no need to import sqlite3 !!  yeay!!
# tin 2016.08.08

from __future__ import print_function
#import sqlite3          # this work in pyspark...

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *


# this is for the acc2taxid db
# need an sc_name sqlcontect for the name and parent taxid db
## don't want to instantiate sparkContext here, as each query would create a context and that's high overhead
## so taxorpt will instantiate the context once only, and this fn will only query against that context...
#--sc_acc = SparkContext( 'local', 'pyspark' )             
#print( "   *** hello world sparkContext created" )
#--sqlContext = SQLContext(sc_acc)
#print( "   *** hello world spark sql context created" )
#--lines = sc_acc.textFile("nucl_gss.accession2taxid.head100")         # 0.550907 sec to count (*)

#db = "ncbi-taxo-acc.db"   # ncbi-taxo-gi.db  
#db = "ncbi-taxo-acc.db"   # ncbi-taxo-gi.db  
unknown = -1                    # used in getPathByTaxid to check for bad rank...
no_rank = "no rank"

class pyphy_spark_class :


        def __init__(self,sc,outputFH) :
                #pyphy_spark_instanceVar = 'dummy var for now'
                self.outFH = outputFH
                #outfile = "/home/bofh1/pub/pyphy_spark.out"
                #out_FH = open( outfile, 'w' )
                print( "  ** Inside OOP ** start of __init__ *L45*"  )
                print( "  ** Inside OOP ** start of __init__ *L45*", file = self.outFH  )
                self.verStamp = "2016.0810.1"
                self.sqlContext = SQLContext(sc)

                #self.lines = sc.textFile("nucl_gss.accession2taxid.head100")                           # ~2 min for 1 select in LOCAL
                #self.lines = sc.textFile("nucl_gss.accession2taxid")         
                ###++~~self.lines = sc.textFile("prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv") # 700s for 1 SELECT in YARN
                self.lines = sc.textFile("db/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid.csv") # 700s for 1 SELECT in YARN
                ###++~~ above was the last file used in cloudera era.... got from hdfs back to c3po now
                ##self.lines = sc.textFile("/home/bofh1/code/svn/taxo/db/download/nucl_gss.accession2taxid.head100")                           # cloudera era: ~2 min for 1 select in LOCAL
                #self.lines = sc.textFile("/home/bofh1/code/svn/taxo/db/download/nucl_gss.accession2taxid")                          
                # history server: http://nrusca-clp24001.nibr.novartis.net:18088/ 
                self.parts = self.lines.map(lambda l: l.split("\t"))
                self.acc_taxid = self.parts.map(lambda p: (p[0], p[1].strip(), p[2].strip(), p[3].strip() ))
                self.schemaString = "acc acc_ver taxid gi"
                self.fields = [StructField(field_name, StringType(), True) for field_name in self.schemaString.split()]
                self.schema = StructType(self.fields)

                self.schemaAccTaxid = self.sqlContext.createDataFrame(self.acc_taxid,self.schema)
                self.schemaAccTaxid.registerTempTable("acc_taxid")

                print( "  ** Inside OOP ** registering tree table", file = self.outFH  )
                #self.tree_lines = sc.textFile("tree.table.csv")
                self.tree_lines = sc.textFile("db/tree.table.csv")
                self.tree_parts = self.tree_lines.map(lambda l: l.split(","))
                self.tree = self.tree_parts.map(lambda p: (p[0], p[1].strip('"'), p[2].strip('"'), p[3].strip('"') ))  # hope this strip the quotes...
                ## sample tree.table.csv:
                ## "taxid","name","parent","rank"
                ## 287144,"Influenza A virus (A/Taiwan/2040/2003(H3N2))",119210,"no rank"
                ## 378467,"Platytheca galioides",4272,"species"
                self.tree_schemaString = "taxid name parent rank"
                self.tree_fields = [StructField(field_name, StringType(), True) for field_name in self.tree_schemaString.split()]
                self.tree_schema = StructType(self.tree_fields)
                self.schemaTree = self.sqlContext.createDataFrame(self.tree,self.tree_schema)
                self.schemaTree.registerTempTable("tree")
                # so far the tree registration table didn't cause any problem...

                print( "  ** Inside OOP [end of __init__]", file = self.outFH  )
                #print( "  ** learning about the schema setup... [in child fn]"  )                       # local mode will see this in web log
                #self.schemaAccTaxid.printSchema()                            # not sure how to redirect this to file...
        # end __init__()



        ## this is just a test method defined during early dev
        ## no longer has a real use for this.
        def testQuery(self,tax_id,limit=1):
                print( "  ** Inside OOP [start testQuery()] Li92"  )
                print( "  ** Inside OOP [start testQuery()] Li92", file = self.outFH  )
                print( self.verStamp )
                print( "  ** learning about the schema setup... [in child fn]", file = self.outFH  )                       # local mode will see this in web log
                self.schemaAccTaxid.printSchema()                            # not sure how to redirect this to file...
                #sqlResult = sqlContext.sql( "SELECT count(*) from acc_taxid" ) # spark does NOT allow for ; at end of SQL !!

                print( "  ** sampling acc_ver table", file = self.outFH  )                       # local mode will see this in web log
                command = "SELECT * from acc_taxid WHERE acc_ver ='" + tax_id + "'" # spark does NOT allow for ; at end of SQL !!
                sqlResult = self.sqlContext.sql( command ) 
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                print( myList )
                print( myList, file = self.outFH  )

                print( "  ** sampling tree table", file = self.outFH  )                       # local mode will see this in web log
                command = "SELECT * from tree WHERE taxid ='" + str(5833)  + "'" # spark does NOT allow for ; at end of SQL !!
                sqlResult = self.sqlContext.sql( command ) 
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                print( myList )
                print( myList, file = self.outFH  )


                command = "SELECT taxid from acc_taxid WHERE acc_ver ='" + tax_id + "'" # spark does NOT allow for ; at end of SQL !!
                sqlResult = self.sqlContext.sql( command ) 
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                print( myList[0].taxid )
                print( "  ** Inside OOP [end testQuery()] Li119" )
                print( "  ** Inside OOP [end testQuery()] Li119", file = self.outFH  )
                return myList[0].taxid 
        #end testQuery()


        #pyphy.getTaxidByName("Bacteria",2)
        # return: [u'2', u'629395']     # works!! :)
        # but it is a list, hope that's okay
        def getTaxidByName(self,name,limit=1):
                command = "SELECT taxid FROM tree WHERE name = '" + str(name) +  "'"
                sqlResult = self.sqlContext.sql( command )
                results = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                temp = []
                for result in results:
                        ##temp += result[0]
                        temp.append(result[0])
                if len(temp) != 0:
                        temp.sort()                     # not sure why want it sorted.  ensure consistent result over many runs?
                        return temp[:limit]
                else:
                        return [unknown]


        #pyphy.getRankByTaxid("2")
        def getRankByTaxid(self,taxid):
                command = "SELECT rank FROM tree WHERE taxid = '" + str(taxid) +  "'"   # no ; at end!!
                sqlResult = self.sqlContext.sql( command )
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( myList, file = self.outFH )
                if len( myList ) == 0:          # added this cuz input db maybe a subset for test use
                        return "unknown"        # rank is text string, not the -1 int value, thus quoted
                return myList[0].rank
                ##self.tree_schemaString = "taxid name parent rank"

        #pyphy.getRankByName("Bacteria")
        #def getRankByName(name):
        def getRankByName(self,name):
            try:
                return self.getRankByTaxid(self.getTaxidByName(name)[0])
                #return getRankByTaxid(getTaxidByName(name)[0])
            except:
                return no_rank



        #pyphy.getNameByTaxid("2")
        def getNameByTaxid(self,taxid):
                command = "SELECT name FROM tree WHERE taxid = '" + str(taxid) +  "'"
                sqlResult = self.sqlContext.sql( command )
                result = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( result, file = self.outFH )
                if result:                              # not sure if this logic still works in hadoop for checking if result found.
                        #return result[0]
                        return str(result[0].name)
                else:
                        return "Unknown"        # since expect name, returning string Unknown is correct...
                        #return "unknown"        # since expect name, returning string Unknown is correct...


        # taxoHandle.getParentByTaxid("5833")
        # return 418107 ## correct! :)
        # ie, should hit this record:
        # 5833,"Plasmodium falciparum",418107,"species"
        def getParentByTaxid(self,taxid):
                #print( "in getParentByTaxid", file = self.outFH )
                command = "SELECT parent FROM tree WHERE taxid = '" + str(taxid) +  "'"
                #command = "SELECT * FROM tree WHERE taxid = '" + str(taxid) +  "'"
                sqlResult = self.sqlContext.sql( command )
                result = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( result )
                #print( result, file = self.outFH )
                if result:                              # not sure if this logic still works in hadoop for checking if result found.
                        #return result[0]
                        return str(result[0].parent)    ## result is good!
                else:
                        return unknown           # this should return a parent/taxid, which is munber, so use the unknown = -1
                        #return "unknown"        # should not return a string.
                ##self.tree_schemaString = "taxid name parent rank"


        #pyphy.getParentByName("Flavobacteriia")
        #def getParentByName(name):
        def getParentByName(self,name):
            try:
                return self.getParentByTaxid(self.getTaxidByName(name)[0])
                #return getParentByTaxid(getTaxidByName(name)[0])
            except:
                return unknown
            

        #def getPathByTaxid(taxid):
        #        print( "getPathByTaxid not implemented anymore, dont think it was used")
        #        print( "getPathByTaxid not implemented anymore, dont think it was used", file = self.outFH )

        def getPathByTaxid(self,taxid):
            path = []
            
            current_id = int(taxid)
            path.append(current_id)
            
            #while current_id != 1 and current_id != unknown:
            while current_id != 1 and current_id != unknown:
                #print current_id
                #current_id = int(getParentByTaxid(current_id))
                current_id = int(self.getParentByTaxid(current_id))
                path.append(current_id)
            return path[::-1]
            


        ## this is first fn called by taxorpt_spark.py, thus first to go as OOP :    
        ##def getTaxidByGi(gi):
        def getTaxidByAccVer(self,gi):
                command = "SELECT taxid from acc_taxid WHERE acc_ver ='" + str(gi) + "'" # spark does NOT allow for ; at end of SQL !!
                sqlResult = self.sqlContext.sql( command )
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( myList[0].taxid, file = self.outFH )
                if len( myList ) == 0:          # added this cuz input db maybe a subset for test use
                        return unknown
                return myList[0].taxid

           
            
        def getSonsByTaxid(self,taxid,limit=1):
                command = "SELECT taxid from tree WHERE parent ='" + str(taxid) + "'" # spark does NOT allow for ; at end of SQL !!
                sqlResult = self.sqlContext.sql( command )
                results = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( results )
                #print( results, file = self.outFH )
                temp = []
                for result in results:
                        temp.append(result[0])
                        ##temp += result[0]
                if len(temp) != 0:
                        temp.sort()                     # want to sort it to provide deterministic result
                        return temp[:limit]
                else:
                        return [unknown]


        def getSonsByName(self,name,limit=1):
                return self.getSonsByTaxid( str(self.getTaxidByName(name)[0]),limit )
                ##   don't know why didn't use self calling before

        ##def getGiByTaxid(taxid):
        ##  not sure if taxorpt ever called/used this fn...
        def getAccVerByTaxid(self,taxid):
            command = "SELECT acc_ver from acc_taxid WHERE taxid ='" + str(taxid) + "'" # spark does NOT allow for ; at end of SQL !!
            sqlResult = self.sqlContext.sql( command )
            try:
                myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
                #print( myList, file = self.outFH )
                # this seems to return many elements, eg taxid = 5833 
                return str(myList[0].acc_ver)
            except Exception as e:      # python3 syntax
                #print e
                #print( format(e) )
                print( format(e), file = self.outFH )

        def erase_getAccVerByTaxid(taxid):
            conn = sqlite3.connect(db)
            command = "SELECT acc_ver FROM acc_taxid WHERE taxid = '" + str(taxid) +  "';"
            cursor = conn.cursor()
            #print command
            try:
                result = [row[0] for row in cursor.execute(command)] 
                cursor.close()
                #return result
                return str(result)
            #except Exception, e:
            except Exception as e:      # python3 syntax
                #print e
                print( format(e) )


        ## Don't think taxorpt ever used these fn, 
        ## so changed to OOP here, but not tested

        # multithreaded version of getSonsByName.  this one should be  a lot faster
        # http://dgg32.blogspot.com/2013/07/retrieve-all-sub-taxa-and-gi-from-ncbi.html?view=sidebar
        ## oh this is not worth rewriting to use SparkSQL, should really use DataFrame!!

        def getAllSonsByTaxid(taxid):
            from threading import Thread
            import Queue
            in_queue = Queue.Queue()
            out_queue = Queue.Queue()
            #what a thread is supposed to do
            def work():
                while True:
                    sonId = in_queue.get()
                    #if here use getAllSonsByTaxid, it will be true recursive,
                    #but it will run over the thread limit set by the os by 
                    #trying "Flavobacteriia" (6000+)
                    #error: can't start new thread
                    #it is more elegant here
                    ##for s_s_id in getSonsByTaxid(sonId):
                    for s_s_id in self.getSonsByTaxid(sonId):
                        #print "s_s_id" + str(s_s_id)
                        out_queue.put(s_s_id)
                        in_queue.put(s_s_id)
                    in_queue.task_done()
                #while-end
            #work()-end
            #spawn 20 threads
            for i in range(20):
                t = Thread(target=work)
                t.daemon = True
                t.start()
            #feed the queue
            for son in getSonsByTaxid(taxid):
                out_queue.put(son)
                in_queue.put(son)
            in_queue.join()   
            result = []
            #reap the results
            while not out_queue.empty():
                result.append( out_queue.get())
            #print result
            #return result
            return str(result)
        #getAllSonsByTaxid()-end





        ##def getAllGiByTaxid(taxid):
        def getAllAccVerByTaxid(taxid):
            from threading import Thread
            import Queue
            in_queue = Queue.Queue()
            out_queue = Queue.Queue()
            allSons = getAllSonsByTaxid(taxid)
            def work():
                while True:
                    sonId = in_queue.get()		## missed in_
                    ##out_queue.put(getAccVerByTaxid(sonId))
                    out_queue.put(self.getAccVerByTaxid(sonId))
                    in_queue.task_done()
            for i in range(20):				## 20 threads for speed
                t = Thread(target=work)
                t.daemon = True
                t.start()
            in_queue.put(taxid)
            for son in allSons:
                in_queue.put(son)
            in_queue.join()        
            result = []
            while not out_queue.empty():
                result += out_queue.get()
            #return result
            return str(result)
        #end getAllAccVerByTaxid()


# end class




