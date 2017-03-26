### pyphy_spark.py.bak1_workingTestFn ###

## #!/bin/env python
## #!/usr/prog/python/3.4.2-goolf-1.5.14-NX/bin/python3   (code is not python3 compatible).
##  when deploy as  python module, don't need the magic line 


## pyphy_spark.py, adapted from pyphy.py
## will need SparkContext for the main Accession->TaxID query eg getTaxIdByAccVer(gi) 
## but sqlite3 will still be used for the small thing (name/nodes lookup, as that had some special sause in the load)

## version 1.1, with queries to Spark instead of sqlite
## tin 2015.0729

import sqlite3          # this work in pyspark...

from pyspark import SparkContext
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



# caller is expected to chagne db when this module is imported   (set to a default that does not exist)
#db = "ncbi-taxo-acc.db"   # ncbi-taxo-gi.db  
db = "ncbi-taxo-acc.db"   # ncbi-taxo-gi.db  

unknown = -1
no_rank = "no rank"

# sqlc is sqlContext
# lot of code for each fn, really pushing for OOP...
def testQueryWithSqlContext(sc,tax_id,limit=1):
        ###+++   these are copied from parent/caller fn, as they are set after sqlcontext was created
        sqlContext = SQLContext(sc)

        lines = sc.textFile("nucl_gss.accession2taxid.head100")         # 0.550907 sec to count (*)
        #lines = sc.textFile("nucl_gss.accession2taxid")         # 0.550907 sec to count (*)
        parts = lines.map(lambda l: l.split("\t"))
        acc_taxid = parts.map(lambda p: (p[0], p[1].strip(), p[2].strip(), p[3].strip() ))
        schemaString = "acc acc_ver taxid gi"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)

        schemaAccTaxid = sqlContext.createDataFrame(acc_taxid,schema)
        schemaAccTaxid.registerTempTable("acc_taxid")
        print( "  ** learning about the schema setup... [in child fn]"  )                       # local mode will see this in web log
        schemaAccTaxid.printSchema()                            # not sure how to redirect this to file...


        #sqlResult = sqlContext.sql( "SELECT count(*) from acc_taxid" ) # spark does NOT allow for ; at end of SQL !!
        command = "SELECT taxid from acc_taxid WHERE acc_ver ='" + tax_id + "'" # spark does NOT allow for ; at end of SQL !!
        sqlResult = sqlContext.sql( command ) 
        myList = sqlResult.collect()            # need .collect() to consolidate result into "Row"
        #print( myList[0].taxid )
        return myList[0].taxid 
# end testQueryWithSqlContext()


#pyphy.getTaxidByName("Bacteria",1)
def getTaxidByName(name,limit=1):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    command = "SELECT taxid FROM tree WHERE name = '" + str(name) +  "';"
    cursor.execute(command)
    results = cursor.fetchall()
    cursor.close()
    temp = []
    for result in results:
        ##temp += result[0]
        temp.append(result[0])
    if len(temp) != 0:
        temp.sort()
        return temp[:limit]
    else:
        return [unknown]

#pyphy.getRankByTaxid("2")
def getRankByTaxid(taxid):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    command = "SELECT rank FROM tree WHERE taxid = '" + str(taxid) +  "';"
    cursor.execute(command)
    result = cursor.fetchone()
    cursor.close()   
    if result:
        #return result[0]
        return str(result[0])
    else:
        return no_rank

#pyphy.getRankByName("Bacteria")
def getRankByName(name):
    try:
        return getRankByTaxid(getTaxidByName(name)[0])
    except:
        return no_rank



#pyphy.getNameByTaxid("2")
def getNameByTaxid(taxid):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    command = "SELECT name FROM tree WHERE taxid = '" + str(taxid) +  "';"
    cursor.execute(command)
    result = cursor.fetchone()
    cursor.close()   
    if result:
        #return result[0]
        return str(result[0])
    else:
        return "unknown"

    

def getParentByTaxid(taxid):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    command = "SELECT parent FROM tree WHERE taxid = '" + str(taxid) +  "';"
    cursor.execute(command)
    result = cursor.fetchone()
    cursor.close()
    if result:
        #return result[0]
        return str(result[0])
    else:
        return unknown
    

#pyphy.getParentByName("Flavobacteriia")
def getParentByName(name):
    try:
        return getParentByTaxid(getTaxidByName(name)[0])
    except:
        return unknown
    

def getPathByTaxid(taxid):
    path = []
    
    current_id = int(taxid)
    path.append(current_id)
    
    while current_id != 1 and current_id != unknown:
        #print current_id
        current_id = int(getParentByTaxid(current_id))
        path.append(current_id)
    
    return path[::-1]
    


## ++ this is prob first fn called by taxorpt_spark.py :    
##def getTaxidByGi(gi):
def getTaxidByAccVer(gi):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    ##command = "SELECT taxid FROM gi_taxid WHERE gi = '" + str(gi) +  "';"
    command = "SELECT taxid FROM acc_taxid WHERE acc_ver = '" + str(gi) +  "';"
    cursor.execute(command)
    result = cursor.fetchone()
    cursor.close()
    if result:
        #return result[0]
        return str(result[0])
    else:
        return unknown
    
    
def getSonsByTaxid(taxid):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    command = "SELECT taxid FROM tree WHERE parent = '" + str(taxid) +  "';"
    result = [row[0] for row in cursor.execute(command)]
    cursor.close()
    #return result
    return str(result)


def getSonsByName(name):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    command = "SELECT taxid FROM tree WHERE parent = '" + str(getTaxidByName(name)[0]) +  "';"
    result = [row[0] for row in cursor.execute(command)]
    cursor.close()
    #return result
    return str(result)




##def getGiByTaxid(taxid):
def getAccVerByTaxid(taxid):
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



# multithreaded version of getSonsByName.  this one should be  a lot faster
# http://dgg32.blogspot.com/2013/07/retrieve-all-sub-taxa-and-gi-from-ncbi.html?view=sidebar

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
            for s_s_id in getSonsByTaxid(sonId):
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
            ##out_queue.put(getGiByTaxid(sonId))
            out_queue.put(getAccVerByTaxid(sonId))
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


## main code could go here ##
## but setting this as a module for import 

