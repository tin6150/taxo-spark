
old def

def dbg( level, strg ):

def getParentByTaxidRank( taxid, rank, tH ) :
def getRankByAccVer( accV, tH ) :
def getRankNameByAccVer(gi,rank):
def getRankNameByTaxid( taxid, rank ):

def getTraceByTaxid( taxid ):
def getTraceByAccVer( gi ):
def getTraceByTaxid2( taxid, tree ):
def getTraceByTaxid2_old( taxid, tree ):
def getAccVerFromBlastLine( line, colidx, ifs ) :

getLineageByTaxid = getRankNameByTaxid


def file2accVerList( filename, colidx, ifs, tH ):
        def __new__( cls ) :
        def getLowest( cls ) :
        def getHighest( cls ) :
        def getParent( cls, rank ) :
        def getChild( cls, rank ) :
def summarizeAccVerList( giList, tH ):
def prettyPrint( table, outfile ) :
def prepHtmlHeader( outFH ) :
def prettyPrintHtml( table, jobName, jobDesc, uniqAccVerCount, recProcessed, rejectRowCount, outFH ) :
def prepHtmlEnding( outFH ) :
def prettyPrintDictTuple( dt ) :


------

pyphy_ext_spakr.py ::

modifications:

 71 def getParentByTaxidRank( taxid, rank, tH ) :$
106 def getRankByAccVer( accV, tH ) : $
116 def getRankNameByAccVer(gi,rank, tH):$
128 def getRankNameByTaxid( taxid, rank, tH ):$



should be fixed... 
dbug now :)
code works.
but very slow.
the sql queries one entry at a time is not good.

moving on to taxo-dataframe and use join on dataframe to see if it speed things up.


----

2017.0216
the code works in c3po/hadoop.
but upon review of above, this code is very slow.
bitbucket/spark has  DataFrame approach, created wide trace table and saved as parquet
continue dev from there...
(note that this dir was going to be placed in git but never actually did)
