
## #!/bin/env python
##    /usr/prog/python/2.7.9-goolf-1.5.14-NX/bin/python   (2016.0304 converting back to this for v.6) 
## #!/usr/prog/python/3.4.2-goolf-1.5.14-NX/bin/python3   (code now requires python3).
##  when deploy as  python module, don't need the magic line 



## python module to provide taxonomic lookup 
## by using ncbi db, cached as sqlite3 db
## source: http://dgg32.blogspot.com/2013/07/pyphy-wrapper-program-for-ncbi-sqlite.html?view=sidebar
## there were slight fixes to above, and adaption to python3.
## pyphy_ext is an extension of pyphy, to provide more fn that peter needed but not avail from original source

## version history
## v 0.1  can produce species + genus, family frequency.  but query DB too many times, looking for species, then next loop for genus, etc.  super slow.  took forever.
## v 0.2  changed approach to lookup traceTree for each GI.  took 2 hrs to resolve 47k GI.   2016.0122
## v 0.3  Peter recommend convert from GI to taxid, and work in this space instead.  also use a nested hash to keep track of taxo pyramid for kragen, also reduces num of DB lookup.
## v 0.4  Hash contain gi to taxid then up from species to superkingdom.  reduced lookup from DB, process 5.4M records in 10-15 min.  get 4 lineage ranks.  prettyPrint_v4 tba
## v 0.5  first gi to taxid lookup is likely at subspecies, so keep this level, and then process species to superkingdom.  sorted version of prettyPrint. code ran in 16 min for above sample intput
## v 0.5.1  a few changes regarding file I/O, debug printing
## v 0.5.2  lineagePrint (for Yipin)
## v 0.6    - converting to use python 2.7 to be compatible with id bioinfo codebase.    (2016.0304)
##          - add param to specify db file/path 
##          - add -html support 
##          - debug message framed in <!-- --> so capturable to html via > redirect
## v 0.7.5  - converted to use Accession.Version (from GI)   (2016.0420)
##                - needed new DB, 
##                - pyphy.py was changed to use Accession.V instead of GI in a couple of places, so that code no longer vanilla open source
##                - performance of DB using Accession.Version instead of GI maybe a concern.  
##                  Seems like using limited Indexes and avoid declaring Primary Key provided good enough result.
## v 1.0     declaring this as version 1.0.
##           next dev will leverage spark/hdfs


from __future__ import print_function    # need python3 style print to support print ( x, out = file )
from pyphy import *
#from pyphy_spark import *
import re           # regular expressions
import operator	
import enum
from html import HTML

# v 0.5.1 addition:
# caller can change this when this module is imported
# dbgLevel 3 (ie -ddd) is expected by user troubleshooting problem parsing input file
# currently most detailed output is at level 5 (ie -ddddd) and it is eye blurry even for programmer
dbgLevel = 0  
def dbg( level, strg ):
    if( dbgLevel >= level ) : 
        print( "<!--dbg%s: %s-->" % (level, strg) )


# no longer needed even in places that might have raised exception
#--NO_MORE_PARENT = "exception for getting parent taxid when it is at top of the lineage tree already"



## given GI, get specie
##  getTaxidByGi(gi)
##  getRankByTaxid(...), compare if at desired rank (ie taxodepth)
##      check to see if it is specie... may need to go get parent...
##      repeat... exit if get -1 as return code...
##  getNameByTaxid(...) once at desired rank


## allow finding a taxid=taxid in any low level (eg species level),
## and what it's taxid at rank (eg superkingom) is
## returned taxid is at the specified rank level
def getParentByTaxidRank( taxid, rank ) :
        rankOfInputTaxid = getRankByTaxid( taxid ) 
        if rankOfInputTaxid == rank :
            # this is a special case, where caller is at the species level (instead of sub-species)
            # and ask for taxid of parent, but at the species level, so return same taxid
            # phylosophically bad to do this, caller should know not to call get parent taxid if no need to
            # but it leaves code at caller much cleaner 
            return taxid
        t = getParentByTaxid( taxid )
        r = "n/a"
        if t == -1 :
                ## FIXME dbglevel was 5, consider changing to 3 when taxid -1 issue is figured out
                #dbg( 5, "    --in t=%8s,r=%8s--out (found)t=%8s r=%8s (-1 case)" % (taxid, rank, t, r) )
                dbg( 3, "(getParent -1 check) Current taxid=[%10s],Current rank=[%10s]. Parent taxid=[%10s], rank=[%10s]" % (taxid, rank, t, r) )
                return t
        if t == "1" :   # taxid is really returned as string!! 
                dbg( 4, "(getParent '1'check) Current taxid=[%10s],Current rank=[%10s]. Parent taxid=[%10s], rank=[%10s]" % (taxid, rank, t, r) )
                ## dbg case only.  thus continue.  cuz taxid 1 isn't too much a problem, just wondering why mapped to top of taxo tree
                ## example of cases where parent taxid = "1" ::
                ## 131567 = cellular organism
                ## 28384  = other sequences
        if t == taxid :                       # this maybe strange case ??
                dbg( 5, "    --in t=%8s,r=%8s--out (found)t=%8s r=%8s (t=taxid case)" % (taxid, rank, t, r) )
                return taxid                    # assume no more parent case

        r = getRankByTaxid( t )
        if r.lower() == rank.lower() :        # found what was asked for
                dbg( 5, "    --in t=%8s,r=%8s--out (found)t=%8s r=%8s (r=rank case, done!)" % (taxid, rank, t, r) )
                return t
        else :
                #print( "** ** recursion getParentByTaxidRank %s %s " % (t, rank) )
                dbg( 5, "    --in t=%8s,r=%8s-- recursing(t=%8s r=%8s) (prev found r=%8s)" % (taxid, rank, t, rank, r) )
                return getParentByTaxidRank( t, rank )

##def getRankByGi( gi ) : 
def getRankByAccVer( accV ) : 
    ##taxid = getTaxidByGi( gi )
    taxid = getTaxidByAccVer( accV )
    return getRankByTaxid( taxid )


#getRankByGi("357542294","species")  ==> Rhinovirus A
#getRankByGi("357542294","genus")   ==> Enterovirus
## maybe call it getLineageByGi() ??
##def getRankNameByGi(gi,rank):
def getRankNameByAccVer(gi,rank):
    ##taxid = getTaxidByGi( gi )
    taxid = getTaxidByAccVer( gi )
    #dbg( "taxid=>%s" % taxid )
    #return getLineageByTaxid( taxid, rank )
    return getRankNameByTaxid( taxid, rank )
#getLineageByGi = getRankNameByGi    # alias, but has to come after the fn is defined 
# stop definiing the above alias.  Change Fn to say  getRankNameByAccVer going forward.

#getLineageByTaxid("147711","genus")
#getLineageByTaxid("147711","superkingdom")
# consider adding code that check rank is supported string (correct spell, etc)
def getRankNameByTaxid( taxid, rank ):
    currentRank = getRankByTaxid( taxid )
    name = getNameByTaxid( taxid )
    if( currentRank.lower() == rank.lower() ) :         # .lower is convert case to non capital letter
        dbg( 4, "%s (%s) \t [taxid=>%s]" % (name, currentRank, taxid) )
        return str(name)                                             # using "name" will get a strange u'' prefix/wrapper, peter said related to use of DB
    parentTaxid = getParentByTaxid( taxid )
    dbg( 4, "%s (%s) \t [taxid=>%s parentTaxid=>%s]" % (name, currentRank, taxid, parentTaxid) )
    if( parentTaxid == "1" ) :
       return "Taxonomy_Tree_Root"
    if( parentTaxid == taxid ) :
       #print( "reached top of lineage tree without finding a matching rank (%s).  aborting" % rank )
       #raise NameError( "No matching rank, no more parent" )
       ## FIXME this was dbglevel 4
       dbg( 4, "Reached top of lineage tree without finding a matching rank [%12s] for input taxid '%s'.  returning Invalid_TaxID" % (rank,taxid) )
       return "Invalid_TaxID"
       #return "NoRankData"
       #return "NoLineageData"
       #raise NameError( "No matching rank, no more parent" )
       #return 007
    return getLineageByTaxid( parentTaxid, rank )
getLineageByTaxid = getRankNameByTaxid
    
 
   
# return a array(list) of tuples, list is the full taxonomic trace from a given taxid .  
# each tuple is ( rank, name )  eg ( "species", "Rhinovirus A" )
# eg [ ('Species', 'Human Rhinovirus A'), ('genus', 'Enterovirus'), ... ]
# alt name getTree or getLineageTree ## ++
def getTraceByTaxid( taxid ):
    tree = [ ]
    return getTraceByTaxid2( taxid, tree )

##def getTraceByGi( gi ):
def getTraceByAccVer( gi ):
    tree = [ ]
    return getTraceByTaxid2( getTaxidByAccVer( gi ), tree )
    ##return getTraceByTaxid2( getTaxidByGi( gi ), tree )

# Python does not do overloading as part of the language
# https://bytes.com/topic/python/answers/602485-method-overloading
# lame lame lame!!  there are hacks to do the interpolation myself, but not worth the headache here.
# this new versio return a list of triples, with the extra element of taxid
# [... ('order', 'Herpesvirales', '548681'), ('no rank', 'dsDNA viruses, no RNA stage', '35237'), ('superkingdom', 'Viruses', '10239') ...]

def getTraceByTaxid2( taxid, tree ):
    currentRank = getRankByTaxid( taxid )
    name = getNameByTaxid( taxid )
    tree.append( (currentRank, name, taxid) )
    parentTaxid = getParentByTaxid( taxid )
    dbg( 4, "%s (%s) \t [taxid=>%s parentTaxid=>%s]" % (name, currentRank, taxid, parentTaxid) )
    # alt, can check if name = root, taxid = 1 , seems to be common top of tree.
    if( parentTaxid == taxid ) :
        return tree
    else:
        return getTraceByTaxid2( parentTaxid, tree )

# old version of this function 
# fn return a list of tuple, eg: 
# [('species', 'Human rhinovirus sp.'), ('no rank', 'unclassified Rhinovirus'), ('genus', 'Enterovirus'), ('family', 'Picornaviridae'), ('order', 'Picornavirales'), ('no rank', 'ssRNA positive-strand viruses, no DNA stage'), ('no rank', 'ssRNA viruses'), ('superkingdom', 'Viruses'), ('no rank', 'root')]
def getTraceByTaxid2_old( taxid, tree ):
    currentRank = getRankByTaxid( taxid )
    name = getNameByTaxid( taxid )
    tree.append( (currentRank, name) )
    parentTaxid = getParentByTaxid( taxid )
    dbg( 4, "%s (%s) \t [taxid=>%s parentTaxid=>%s]" % (name, currentRank, taxid, parentTaxid) )
    # alt, can check if name = root, taxid = 1 , seems to be common top of tree.
    if( parentTaxid == taxid ) :
        return tree
    else:
        return getTraceByTaxid2( parentTaxid, tree )







##def getGiFromBlastLine( line, colidx, ifs ) :
def getAccVerFromBlastLine( line, colidx, ifs ) :
    # checking can be done inline where the line is split
    # but cleaner to have the error checking code here
    # example expected format.  
    # input really has tab delimited columns, but blast line also has | that can act as delimiter.  eg YP_007003722.1 is the acc.v being extracted
    # PHUSEM-WDL30046:198:H760GADXX:1:1211:3745:72353 gi|422933597|ref|YP_007003722.1|        37.50      32      20      0       97      2       310     341     3.0     28.1
    # PHUSEM-WDL30046:198:H760GADXX:1:2108:16321:30071        gi|20026759|ref|NP_612801.1|    55.56      27      9       2       93      19      323     348     3.9     27.7
    # line is one input line from the Blast File.  
    # return element is the Accession.V 
    # for above example input line, split by |, ignore the tabs, and acc.v is in col id 3 (0-based index)
    #lineList = line.split( '|' )
    #gi = lineList[1]
    #lineList = line.rstrip("\n\r").split( ifs )
    line = line.rstrip("\n\r") 
    lineList = line.split( ifs )
    if( len(lineList) < colidx ) :
        dbg( 1, "Not enough columns.  No Accession.V found at col index '%s' for input line of '%s'" % (colidx, line) )
        dbg( 3, "Line split into %s words" % len (lineList) )
        #dbg4( "col idx %s not found in this line, returning empty string." % colidx )
        return ""                   # return empty string if line did not have required number of columns
    dbg( 3, "col idx: %s has val: %s"  % (colidx, lineList[colidx]) )
    accV = lineList[colidx].strip()       # strip() removes white space on left and right ends only, not middle
    #if( re.search( '^[0-9]+$', gi ) ) :       
    if( re.search( '^[A-Z][A-Z][_]{0,1}[0-9]+\.[0-9]+$', accV ) ) :     
        # re is the regular expression match.  some eg of Accession.Version: CP011538.1  NR_103137.1  XM_009432957.1  HQ634565.1
        dbg( 2, "Extract ok for acc.V [%14s] from input line '%s'" % (accV, line) )
        return accV
    else :
        dbg( 1, "Fail - Accession.V pattern not found at col index '%s' for input line '%s'" % (colidx, line) )
        return ""
# end getAccVerFromBlastLine()


# Process an input file with Accession.Version (first expected to be output of blast file, but can really be anything with accession.v (formerly gi))
# one acc.v per line, indicated in column number at colidx (0-based) and separated by ifs.
# v.7.5 list returns a hash , key = acc.v, elements = hash with freq, taxid  + number of records processed, ie a tuple:
# return (accVerList, lineCount, rejectRowCount )   
##def blastFil2giList( filename, colidx, ifs ):
##   file2giList( filename, colidx, ifs )
def file2accVerList( filename, colidx, ifs ):
    #checkBlastFile( filename )  # that fn will raise an exception if format is not as expected
    #giList = { }                # in v4 using hash, plan to place namedtuples in it
    accVerList = { }             # in v4 using hash, plan to place namedtuples in it
    #f = open( filename, 'r' )
    f = filename  # taxorpt use argparse to open file...
    #print f
    # example expected format.  
    # input really has tab delimited columns, but blast line also has | that can act as delimiter.  eg YP_007003722.1 is the acc.v being extracted
    # PHUSEM-WDL30046:198:H760GADXX:1:1211:3745:72353 gi|422933597|ref|YP_007003722.1|        37.50      32      20      0       97      2       310     341     3.0     28.1
    # PHUSEM-WDL30046:198:H760GADXX:1:2108:16321:30071        gi|20026759|ref|NP_612801.1|    55.56      27      9       2       93      19      323     348     3.9     27.7
    lineCount = 0
    rejectRowCount = 0
    for line in f:
        g = getAccVerFromBlastLine( line, colidx, ifs )   # formerly getGiFromBlastLine 
        #--GiNode = namedtuple( 'GiNode', ['Freq', 'Taxid'] )
        if( g == "" ) :
            rejectRowCount += 1
            dbg( 4, "empty accession.v, breaking away" )
            ##break    # this will terminate all for loop and stop processing the file!!  don't use!!
            continue #pass
        if g not in accVerList :
                taxid = getTaxidByAccVer( g )               # formerly taxid = getTaxidByGi( g )
                rank = getRankByTaxid( taxid )              # "species" or "no_rank" in subspecies ??
                #giList[g] = GiNode( Freq=1, Taxid=taxid)
                #giList[g] = { "Freq": 1, "Taxid": taxid }                  ## see if really want to store rank here  (nah)
                accVerList[g] = { "Freq": 1, "Taxid": taxid, "Rank": rank }
                ##dbg( 4, "added gi: %s with freq: %s taxid: %s rank: %s" % ( g, 1, taxid, rank) ) 
                ## dbg for taxid -1 and 1 use.  may want to change to dbglevel 3 once understand problem.  
                if( taxid == -1 or taxid == 1 ) :
                    dbg( 3, "Check TaxID '%s': with rank '%s' from gi: '%s'.  input:[%s]" % (taxid, rank, g, line.rstrip("\n\r")) ) 
                ## giList.update( {g: { "Freq": 1, "Taxid": taxid, "Rank": rank } } )
        else :
                gin = accVerList[g]
                #giList[g] = GiNode( gin.Freq+1, gin.Taxid )
                accVerList[g]['Freq'] += 1
        lineCount += 1
    #f.close()    # since caller opened file, let it close it now (v 0.5.1)
    #print( "    GiList uniq count: %s, total line processed: %s" % ( len(giList), lineCount ) )
    dbg( 4, accVerList )
    #return giList          # v4 list returns a hash , key = gi, elements = hash with freq, taxid
    return (accVerList, lineCount, rejectRowCount )   # v.6 list returns a hash , key = acc.v, elements = hash with freq, taxid  + number of records processed
    # each entry essentially looks like 
    # giList[g] = { "Freq": 1, "Taxid": taxid }
#blastFile2giList()-end



# import enum
#RankSet = Enum( 'Rank', 'species genus family order superkingdom' )
# RankSet no longer Used in v2.  but prettyPrint output not ordered as per enum.  if want to use this, need to add all possible ranks found in DB. eg subfamily
#RankSet = Enum( 'Rank',  ['species', 'genus', 'family', 'order', 'class', 'phylum', 'superkingdom', 'no rank', 'NoLineageData']  )       
#RankSet = Enum( 'Rank',  ['species', 'genus', 'subtribe', 'tribe', 'sub-family', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'parvorder',  'subclass', 'class', 'subphylum', 'phylum', 'kingdom', 'superkingdom', 'no rank']  )    # dropped 'NoLineageData', as that is returned as lineage only, not as rank   
#RankSet = Enum( 'Rank',  ['species', 'genus', 'family', 'superkingdom', 'no rank']  )    # dropped 'NoLineageData', as that is returned as lineage only, not as rank   
# https://en.wikipedia.org/wiki/Taxonomic_rank   there is sub-species, sub-genus, and botany use division instead of phyla (what does ncbi use?)
# For metagenomics, may not care about the plant stuff and maybe omit many of the above?  just ignore any such data that may appear in a 2D hash table?
#RankSet = Enum( 'Rank', ['species', 'genus', 'family', 'no rank'] )       # didn't need the extra complication of a dict till space in "no rank"
#RankSet = Enum( 'Rank', 'species' )

# enum functional style, need python3
# https://docs.python.org/3/library/enum.html#functional-api
# also, need to define getParent(), getChild() so can't use fn style enum even if not staying in python 2.7
# https://docs.python.org/3/library/enum.html#autonumber
# python 3 can use AutoNumber, but not python 2 (cuz backport is not 100%), so suck it up and put the ranking numbers there manually
# note that python2 depends on enum34  (regular enum don't work with the code below)
"""
class AutoNumber( enum.Enum ) :
        def __new__( cls ) : 
                value = len( cls.__members__ ) + 1
                obj = object.__new__( cls ) 
                obj._value_ = value
                return obj
class RankSet( AutoNumber ) :
        # first object in Enum is 1, not 0.  cuz want it to eval to True 
        subspecies      = ()    # subspecies is tricky, NCBI taxo leave it as "no rank"
        species         = ()    # order in this list matter!!
        genus           = ()    # RankSet.__x__.name 
        family          = ()    # can add other ranks in middle if desired
        superkingdom    = ()    # code expects sk to be highest
        #'no rank'       = ()   # can't do this, but new class-way of RankSet does not need this anyway
"""
class RankSet( enum.Enum ) :
        subspecies      =  1    # subspecies is tricky, NCBI taxo leave it as "no rank"
        species         =  2    # order in this list matter!!
        genus           =  3    # RankSet.__x__.name
        family          =  4    # can add other ranks in middle if desired
        superkingdom    =  5    # code expects sk to be highest
        # there are only enough ranks that fit the desired result table
        # if need full sequencing , create a RankTrace or FullRankSet class
        # but most likely don't need


        @classmethod 
        def getLowest( cls ) : 
                #(name, member) = cls.species      
                #return cls.species             # return RankSet.species  (what is needed programatically for getParent() etc )
                return cls( 1 )         # know that lowest rank in Enum class starts with 1
        # http://www.tech-thoughts-blog.com/2013/09/first-look-at-python-enums-part-1.html
                #return cls.species.name        # return species  # http://stackoverflow.com/questions/24487405/python-enum-getting-value-of-enum-on-string-conversion
                # below will do the equivalent, but much slower
                for (name, member) in cls.__members__.items() : 
                        if member.value == 1 : 
                                return name 
                #return cls.__members__ 
        @classmethod 
        def getHighest( cls ) : 
                return cls.superkingdom      # how to use value=max ??
        @classmethod 
        def getParent( cls, rank ) : 
                # eg call: RankSet.getParent(RankSet['species'])
                # may want to to have RankSet.getParentByName('species')
                if( rank == cls['superkingdom']  ) :
                        return None         # return None, as no parent for sk
                return cls( rank.value + 1) 
        @classmethod 
        def getChild( cls, rank ) : 
                if( rank.value == 1 ) :
                        return          # return None, as no child for species
                return cls( rank.value - 1) 
                # below will do the equivalent, but much slower
                for (name, member) in cls.__members__.items() : 
                        if member.value == rank.value - 1 : 
                                return member 
# RankSet class end
# self notes for enumeration
# RankSet is meant to be a static class, not to be instantiated.  
# support calls like these:
#        RankSet.getLowest()                            # RankSet.species
#        RankSet.getLowest().name                       # species 
#        RankSet.getParent( RankSet.getLowest() )       # RankSet.genus
#        r = RankSet.getChild( RankSet.getLowest() )    # get None when "out of range"
#        if r == None :
#                print( "got None from RankSet fn call..." )
#        Valid attributes after from pyphy_ext import * :
#        RankSet(3).value  # 3
#        RankSet(1).name   # 'subspecies'
#        RankSet(3).name   # 'genus'
#        RankSet['genus']  # <RankSet.genus: 3>
#        RankSet['genus'].value # 3
#########################


# This fn take a List of GI and frequencies, and produce a summary "tree"
# The input used to be a GI list, but now really using Accession.Version.  
# Overall, only changed fn name, as this whole fn works in tax_id space, 
# so gi is really just an key for entry lookup. 
# Wherever it says GI, just know that it really means Accession.Version 
# input: giList, a hash of tuples, eg
# giList[g] = { "Freq": 1, "Taxid": taxid }
# the giList is no longer expected to be uniq (v.4 counts GI freq)
# and at v 0.7.5, it isn't really list of GI, but list of Accession.Version 
# output: a convoluted data structure with frequency of taxid, binned so a tree can be produced.   names are also stored to reduce db lookup.
# first tried to use more FP or OOP, but for performance reason, procedural and getting deep into the guts of the data structure is used throughout this fn
# NOTE. subspecies and species result are almost the same.  
# some have variant with number replaced with letter at higher level.  
# may be don't print sub-species, even though code can handle it now...
##def summarizeGiList( giList ):     
def summarizeAccVerList( giList ):
        #TaxoNode = namedtuple( 'taxoNode', ['ParentTaxid', 'RankName', 'Tally', 'Children'])
        # going to have to change to use hash for TaxoNode instead of namedTuple
        lowestRank      = RankSet.getLowest()
        highestRank     = RankSet.getHighest()
        nextUpRank      = RankSet.getParent( lowestRank )
        lowestRankName  = RankSet.getLowest().name
        resultTable4 = { lowestRankName: {} }
        currentRankName = lowestRankName
        # first, parse the GI List to create the lowest level count based on Taxid
        for g in giList :
                giTaxid = giList[g]['Taxid']
                #print( giList )
                #print( "looking at gi:%12s \t with freq: %5d \t and taxid:%10s " % (g, giList[g]['Freq'], giList[g]['Taxid']) )
                ## below is for debbug only, don't affect code
                ##--  may want to use getParentByTaxid( giTaxid, rank)  # rank = lowestRank eg sub-species   (nah, works now)
                #tmpParentTaxid = getParentByTaxidRank( giTaxid, "species" )       # dont know if can go with sub-species, cuz that is no-rank?? 
                tmpParentTaxid = getParentByTaxid( giTaxid )                       # during this first pass, likely no-rank, which is sub-species
                dbg( 4, "looking at gi:%12s with freq: %5d and taxid:%10s (parentTaxid:%10s)" % (g, giList[g]['Freq'], giList[g]['Taxid'], tmpParentTaxid) )
                #if parentTaxid not in resultTable4[currentRank] :
                if giTaxid not in resultTable4[currentRankName] :
                        parentTaxid = getParentByTaxidRank( giTaxid, nextUpRank.name )  # eg looking for "species" taxid
                        rankName = getNameByTaxid(   giTaxid )
                        #rankName = getLineageByTaxid( giList[g]['Taxid'], currentRank )
                        runningCount = giList[g]['Freq']
                        resultTable4[currentRankName][giTaxid] = { 'ParentTaxid': parentTaxid, 'RankName': rankName, 'Tally':runningCount, 'Children': [g] }
                        #resultTable4[lowestRank][giList[g]['Taxid']] = { 'ParentTaxid': parentTaxid, 'RankName': rankName, 'Tally':giList[g]['Freq'], 'Children': giList[g]['Taxid']}
                else :
                        resultTable4[currentRankName][giTaxid]['Tally'] += giList[g]['Freq']
                        resultTable4[currentRankName][giTaxid]['Children'].append(g)              # children is a list
        # by here, all gi info should be populated into the lowest-rank level
        # just need to move up the taxo tree to the next rank up...
        dbg( 5, "resultTable4 after counting all GIs at lowest taxo rank.  so far have:" )
        dbg( 5, resultTable4 )
        dbg( 5, "=======================" )
        workingRankSet = [ ]
        #for rank_item in RankSet.__members__ :
        for rank_item in RankSet :
                #print( rank_item )
                if rank_item != highestRank and rank_item != lowestRank :
                        workingRankSet.append( rank_item )
        #workingRankSet.remove( RankSet.getLowest() )    # this could be subspecies, but NCBI has "no rank", careful with usage
        #workingRankSet.remove( highestRank )   # may need to do summary specially at the top

        for rank_item in workingRankSet :                       # this need to be in order, going up taxo tree.
                dbg( 4, "** ** rank_item: %s ** **" % rank_item )
                currentRank = rank_item
                childRank   = RankSet.getChild(  currentRank )
                parentRank  = RankSet.getParent( currentRank )          ## if not topRank
                if currentRank.name not in resultTable4 :
                        resultTable4.update( {currentRank.name: {}})
                for childTid in resultTable4[childRank.name] :
                        tid = resultTable4[childRank.name][childTid]['ParentTaxid']
                        dbgMsg = "childtid=%8s : %s  (child'sParent=%s)" % (childTid, resultTable4[childRank.name][childTid], tid)
                        dbg( 4, dbgMsg )
                        #print( tid ); print( resultTable4[childRank.name][tid]  ) #'Taxid']['RankName'] )
                        if tid not in resultTable4[currentRank.name] :
                                #parentTaxid = getParentByTaxid( tid )     
                                parentTaxid = getParentByTaxidRank( tid, parentRank.name )    
                                dbg( 4, "for tid: %8s got parentTaxid %s" % (tid, parentTaxid) )
                                rankName = getLineageByTaxid(   tid, currentRank.name )
                                runningCount = resultTable4[childRank.name][childTid]['Tally']
                                resultTable4[currentRank.name][tid] = { 'ParentTaxid': parentTaxid, 'RankName': rankName, 'Tally': runningCount, 'Children': [childTid] }     #.update( {tid: {}} )
                                #print( "added %s into " %  tid )
                                #print( "added %s into %s " % (tid, resultTable4[currentRank.name]) )
                                #pass
                                #print( "bla")
                        else :
                                resultTable4[currentRank.name][tid]['Tally'] += resultTable4[childRank.name][childTid]['Tally']
                                resultTable4[currentRank.name][tid]['Children'].append( childTid )
                                dbg( 4, 'in else section, updating %s with child %s' % (tid, childTid) )

        ## at this point, need to process the top level domain (superkingdom)
        ## can probably merge into above with a few if conditions
        ## but this is easier to read, too many if hides flow of algorithm
        topRank = RankSet.getHighest() 
        belowTopRank = RankSet.getChild( topRank )
        dbg( 4, "** ** rank_item: %s (lower found to be %s) ** **" % (topRank, belowTopRank) )
        if topRank.name not in resultTable4 : 
                resultTable4.update( {topRank.name: {} } )
        for childTid in resultTable4[belowTopRank.name] :
                tid = resultTable4[belowTopRank.name][childTid]['ParentTaxid']
                #tid = resultTable4[childRank.name][childTid]['ParentTaxid']
                dbgMsg = "childtid=%8s : %s  (child'sParent=%s)" % (childTid, resultTable4[belowTopRank.name][childTid], tid)
                dbg( 4, dbgMsg )
                if tid not in resultTable4[topRank.name] :
                        runningCount = resultTable4[belowTopRank.name][childTid]['Tally']
                        rankName = getLineageByTaxid( tid, topRank.name )
                        # omitting ParentTaxid as no more parent here
                        # this is really the only place it differ from the above block inside the for rank_item loop
                        resultTable4[topRank.name][tid] = { 'RankName': rankName, 'Tally': runningCount, 'Children': [ childTid] }
                else :
                        resultTable4[topRank.name][tid]['Tally'] += resultTable4[belowTopRank.name][childTid]['Tally']
                        resultTable4[topRank.name][tid]['Children'].append( childTid )
        ## should be done summarizing top level domain as well by here
        dbg( 5, "....sumGiListv4 about to return resultTable4..." )
        dbg( 5, resultTable4 )
        return resultTable4
#summarizeGiList()-end




# take a gi/taxid frequency data structure and provide a human readable print out
# this version will sort before print 
# somewhere in this fn need to have "import operator"
def prettyPrint( table, outfile ) :
        #outFH = open( outfile, 'w' )
        outFH = outfile  # v0.5.1 argparser opens file already
        #print( table, file = outFH )
        # things are from v2 right now, need fixing... for resultTeable4 structure
        for r in RankSet.__members__ :         # this give order as set in enum
                #print( "                         ===================  %s  ==================" % r, file = outFH )
                if r == "subspecies" :
                    print( "           =================== Taxid at Accession.V ==================", file = outFH )
                else : 
                    print( "           =================== Rank Level: %s  ==================" % r, file = outFH )
                print( "   count  percentage  Rank name                                                         [  taxid  ] (running tally)", file = outFH )
                ## loop to find out total count at the current rank, so can calculate percentage
                rankTotal = 0
                #SortEntry = namedtuple( 'SortEntry', ['Id', 'Freq'] ) 
                sortTmp1Table = {} 
                for ind in table[r]:
                        rankTotal += table[r][ind]['Tally'] 
                        sortTmp1Table[ind] = (table[r][ind]['Tally'], table[r][ind]['RankName']) 
                # create a sorted list of tuples (nested tuples cuz want to sort by species name as well) 
                # http://stackoverflow.com/questions/613183/sort-a-python-dictionary-by-value 
                # eg of sortedTable [('548682', (5, 'Alloherpesviridae')), (-1, (4, 'NoLineageData')), ('9604', (2, 'Hominidae')), ('11632', (1, 'Retroviridae')), ('10240', (1, 'Poxviridae')), ('4479', (1, 'Poaceae')), ('1', (1, 'NoLineageData')), ('10292', (1, 'Herpesviridae')), ('11266', (1, 'Filoviridae')), ('10508', (1, 'Adenoviridae'))]
                sortTmp2Table  = sorted( sortTmp1Table.items(), key=lambda x: x[1][1], reverse=False )   # first sort by 2nd key (rank name)  don't know how to do case insensitve sort.  str.lower don't work :( 
                sortedTable    = sorted( sortTmp2Table        , key=lambda x: x[1][0], reverse=True  )   # finally sort by 1st key, freq 
                #print( sortTmp1Table, file = outFH )      # tmp debbug
                #print( sortTmp2Table, file = outFH )      # tmp debbug
                #print( sortedTable,   file = outFH )      # tmp debbug
                entryCount = 0
                for (ind, (freq, rankname)) in sortedTable :
                        #(id,freq) = sortedTable[k] 
                        percentage  = ( float(table[r][ind]['Tally']) / rankTotal ) * 100
                        entryCount +=         table[r][ind]['Tally']
                        if r == "subspecies" :
                            rank = getRankByTaxid(ind)
                            displayText = '{0: <60}'.format( table[r][ind]['RankName'] + " (" + rank + ")"  )
                        else :
                            displayText = '{0: <60}'.format( table[r][ind]['RankName']  )
                        # if keeping [ taxid ] column, no need for the next if-block
                        #if table[r][ind]['RankName'] == "NoLineageData" :
                        #    displayText = '{0: <50}'.format( table[r][ind]['RankName'] + " (taxid=" + str(ind) + ")" )
                        #else :    
                        #    displayText = '{0: <50}'.format( table[r][ind]['RankName'] )
                        taxidText = '{0: >7}'.format( str(ind) )
                        print( "%8d  %8.4f %%    %s    [ %s ] ( %11d )" % (table[r][ind]['Tally'], percentage, displayText, taxidText, entryCount), file = outFH )
                print( "", file = outFH )       # add empty line at end of block
        #outFH.close() # v0.5.1 argparser opens file already, so let it close it
        print( "result stored in %s" % outfile )
        return
#prettyPrint()-end

# from html import HTML
# prepare output file with  HTML headers, so debbug info can be written to it as input is processed
def prepHtmlHeader( outFH ) :
        htmHead = """
        <HEAD>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <script type="text/javascript" language="javascript" src="http://code.jquery.com/jquery-1.12.0.min.js">
        </script>
        <script type="text/javascript" language="javascript" src="https://cdn.datatables.net/1.10.11/js/jquery.dataTables.min.js">
        </script>
        <script type="text/javascript" class="init">
         $(document).ready(function() {
          $('table.display').DataTable( {       // multi table example use table.display as selector
            lengthMenu: [ [ 10, 25, 50, 100, -1 ], [ 10, 25, 50, 100, "All" ] ],        // all is coded as -1, thus the double array
            pageLength: 25,                     // YES, can use -1 to display all rows by default
            order: [ 0, "desc" ],               // descending order // https://datatables.net/reference/option/order  // def *should* be no ordering, show data as they are loaded.  
          } );
         } );
        </script>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.11/css/jquery.dataTables.min.css">  
        <!-- overwrite some css to my liking -->
        <STYLE>
                H1, H2, H3, H4 {
                        color: green;
                        padding:        0.1em;
                        margin-top:     0.2em;
                        margin-bottom:  0.4em;
                        font-family: helvetica, arial, lucida, sans-serif;
                        text-align:     center;
                }
                H1 {
                        background-color: #BBF0BB;
                        font-size:      170%;
                        font-weight:    bolder;
                        text-transform: uppercase;
                        ; list-style-type: decimal
                }
                H2 {
                        background-color:       #D0F0D0;
                        font-size:              150%;
                        font-variant:           small-caps;
                        text-transform:         uppercase;
                        border-style:           solid;
                        /* text-transform:      capitalize; */
                        ; list-style-type: decimal
                }
                a:link{     color: green;  text-decoration: underline;  }
                a:visited{  color: gray;   text-decoration: underline;    font-weight: bold;  }
                a:hover{    color: Red;    text-decoration: underline;    background-color: #BBF0BB }  
                /* a:active{   color: Olive;  text-decoration: overline;     background-color: #334433 } */
                table.dataTable tbody td{padding:1px 1px 1px 1px;}
                /* table.dataTable.hover tbody tr:hover { background: cyan; }  don't work */
                /* table.dataTable tbody td:hover { background: magenta; }  this one works but don't like it */
        </STYLE>
        </HEAD>
        """

        h = HTML()
        # need to add HTML headers
        h += "<HTML>" 
        h += htmHead
        h += "<BODY>"
        print( h, file = outFH )


# take a gi/taxid frequency data structure and provide a human readable print out
# this version will sort before print
# ref for html module: https://pypi.python.org/pypi/html 
# to produce a table in interactive sort feature, need java script.  eg 
# https://www.datatables.net/examples/data_sources/js_array.html 
def prettyPrintHtml( table, jobName, jobDesc, uniqAccVerCount, recProcessed, rejectRowCount, outFH ) :

        ### this block of html will be used repeatedly in for loop for GI, Species, etc
        htmTableHead = """
        <thead>
            <tr> <th>Count</th>  <th>Percentage</th>     <th>Rank name</th>  <th>TaxID</th> </tr>
        </thead>
        <!--<tfoot>
            <tr> <th>Count</th>  <th>Percentage</th>     <th>Rank name</th>  <th>TaxID</th> </tr>
        </tfoot>-->
        """
        
        ### 
        h = HTML()
        h += "<title>Taxonomy report - %s </title>" % jobName
        h.h1("Taxonomy Report")
        print( h, file = outFH )
        
        ### print a horizontal navigation header
        h = HTML(newlines=False)
        h += "<!-- ############ description and navigation header table ########## -->\n" 
        h += "<TABLE id='job_description'><TBODY>\n"
        h += "<TR><TD>Job Name:             </TD><TD>%s</TD></TR>\n" % jobName
        h += "<TR><TD>Job Desc:             </TD><TD>%s</TD></TR>\n" % jobDesc
        h += "<TR><TD>Unique Acc.V Count:   </TD><TD>%s</TD></TR>\n" % uniqAccVerCount
        h += "<TR><TD>Total rows processed: </TD><TD>{:,}</TD></TR>\n".format( recProcessed )   # {:,} provides thousand separators
        h += "<TR><TD>Rejected rows:        </TD><TD>%s</TD></TR>\n" % rejectRowCount
        h += "</TBODY></TABLE>\n"
        h += "<TABLE id='navigation_header'>"
        h += "<TBODY>\n"
        h += "<TR><TD>Jump to: </TD>"
        print( h, file = outFH )
        h = HTML(newlines=False)
        for r in RankSet.__members__ :
                if r == "subspecies" :                  # do i want to query this from RankSet ?? FIXME
                    h += '<!-- ACCESSKEY is alt + key in chrome.  programmatic setting may not guarantee unique keys, last one win -->\n'
                    h += '<TD align="center"><A ACCESSKEY="a" HREF="#accV">Accession-level</A></TD>\n'    
                else :
                    h += '<TD align="center"><A ACCESSKEY="%s" HREF="#%s">%s</A></TD>\n' % (r[0], r, r)   
        h += "\n</TR></TBODY>\n"
        h += "</TABLE>\n"
        print( h, file = outFH )

        ## sort input and prep it for printing.  code is same from plaintext prettyPrint
        ## could skip the sorting and let the jquery DisplayTables do the sorting, but wanted this to be usable with eg lynx/elinks
        for r in RankSet.__members__ :
            h = HTML(newlines=False)
            h += "<!-- ############ new rank (%s) section ########## -->\n" % r
            if r == "subspecies" :                  # do i want to query this from RankSet ?? FIXME
                h += '<A NAME="accV"></A>'
                h.h2( "Accession.V to TaxID (Mapped Rank)" )    
            else :
                #h += ( "<HR>" )
                h += '<A NAME="%s"></A>' % r 
                h.h2( r )    
            # the table in https://pypi.python.org/pypi/html/1.16 sucks, can't handle code interspearsed into each table row!
            #t = h.table(id="taxo_table_1", klass="display", cellspaceing="0")
            #t += htmTableHead 
            #t += "<tbody>"
            #h += '<TABLE id="taxo_tables" class="display" cellspacing="0" width="100%">'
            h += "\n<TABLE id='taxo_table_%s' " % r 
            h += "class='display' cellspacing='0' width='100%'>"  
            h += htmTableHead
            h += "<TBODY>"
            print( h, file = outFH )

            rankTotal = 0
            sortTmp1Table = {}
            for ind in table[r] :
                rankTotal += table[r][ind]['Tally']
                sortTmp1Table[ind] = (table[r][ind]['Tally'], table[r][ind]['RankName'])
            sortTmp2Table   = sorted( sortTmp1Table.items(), key=lambda x: x[1][1], reverse=False )  # first sort by 2nd key (rank name) 
            sortedTable     = sorted( sortTmp2Table        , key=lambda x: x[1][0], reverse=True  )  # then sort by 1st key, freqo
            for (ind, (freq, rankname)) in sortedTable :
                percentage  = float(table[r][ind]['Tally']) / float(rankTotal) * 100
                if r == "subspecies" :                  # do i want to query this from RankSet ?? FIXME
                    rank = getRankByTaxid(ind)
                    displayText = '{0: <60}'.format( table[r][ind]['RankName'] + " (" + rank + ")"  )   # the space padding should not matter to DisplayTables
                else :
                    displayText = '{0: <60}'.format( table[r][ind]['RankName'] )
                taxidText = '{0: >7}'.format( str(ind) )
                ## here is where html printing differ from plaintext prettyPrint
                #h += "%8d  %8.4f %%    %s    [ %s ]" % (table[r][ind]['Tally'], percentage, displayText, taxidText)
                h = HTML(newlines=False)
                h += "<TR>"
                h += "<TD>%8d</TD>" % table[r][ind]['Tally']     # count
                h += "<TD>%8.4f %%</TD>" % percentage
                h += "<TD>%s</TD>" % displayText                # Rank name
                h += "<TD><A HREF='http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=%s'>\t%s</A></TD>" % (ind, taxidText)       # http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id= 
                h += "</TR>"
                print( h, file = outFH )
            #t += "</tbody>"
            h = HTML()
            h += "</TBODY></TABLE>"
            print( h, file = outFH )
        # end of loops for GI, species, etc tables
        #pass    # pass does nothing, just a no-op, so not needed here.
#prettyPrintHtml()-end


def prepHtmlEnding( outFH ) :
        h = HTML()
        h += "<BR>--<BR>\n"
        h += "For 'Accession.V to TaxID' table, some maps to species level.  Those showing 'no rank' are typically subspecies level <BR>"
        h += "Use shift click on header to sort on additional column(s)<BR>"
        h += "Use -d or -ddd to enable debug info.  Useful for troubleshooting input file parsing problem.  Look for dbg in html source.<BR>"
        h += "<!--taxorpt/pyphy_ext v 0.7.5 (2016.04.20)-->"
        h += "</BODY>"
        h += "</HTML>" 
        print( h, file = outFH )


# print a dictionary of tuples, eg one returned by getTraceByTaxid2, which has form of:
# [('species', 'Human rhinovirus sp.'), ('no rank', 'unclassified Rhinovirus'), ('genus', 'Enterovirus'), ('family', 'Picornaviridae'), ('order', 'Picornavirales'), ('no rank', 'ssRNA positive-strand viruses, no DNA stage'), ('no rank', 'ssRNA viruses'), ('superkingdom', 'Viruses'), ('no rank', 'root')]
# not much, really just print each tuple in a line so that it is easier to read 
def prettyPrintDictTuple( dt ) :
    #print( dt )
    for item in dt :
        print( item )
# end prettyPrintDictTuple()
lineageTracePrint = prettyPrintDictTuple


### main, if needed...
