#!/bin/env python

## #!/prog/python/3.4.2-goolf-1.5.14-NX/bin/python



##### pyphy_tester.py    #####
# this file is for unit test use 
# end user would not need to bother with this file



#import pyphy
#print( pyphy.getTaxidByName("Bacteria",1) )

#from __future__ import print_function
#http://stackoverflow.com/questions/12592544/typeerror-expected-a-character-buffer-object
from pyphy import *
from pyphy_ext import *   # some tester need phyphy_ext eg getTraceByTaxid()
#import pyphy        # needed so can set db path
#import pyphy_ext    # needed so can set dbglevel, 


# most of the function works up to v 0.6 when query used gi 
# new fn in v 0.7 use accession.version to convert to tax_id space


import pyphy_accession

def tester_v7():
        #pyphy.db = "/db/idinfo/taxo/ncbi-taxo-acc-2016-0408.db"
        #print( pyphy_accession.getTaxidByAccVer( "YP_007003691.1"))
        # can use two diff python file to open two diff sqlite3 db
        print( pyphy_accession.getAllByAccVer( "YP_007003691.1"))          # gi = 422933567  tax_id = 317858.   run time in sky-4-9 0.1s     (v.7.5)
        #print( pyphy_accession.getAllByAccVer( hash("YP_007003691.1")))   # gi = 422933567  tax_id = 317858.   run time in c3po = untested  (v.7.3) 
        #print( pyphy.getTaxidByGi(422933567))                          # use v 0.6 db (gi),                 run time in c3po =  1.04s; 0.21s
        # peter suggested running hash() on the accession.version so that it is numeric.  new new db.
 
# tester_v7()-end


 
#### fn below are pre v 0.6 ####

def tester1():
        print( getTaxidByName("Bacteria",1) )    # something about temp being integer and not iterable... 
        print( getTaxidByName("Bacteria",2) )           # [ 629395, 2]
        print( getTaxidByName("Flavobacteria",1) )      # get -1
        print( getTaxidByName("Rhinovirus A",1) )       # should get 147711
        ##getTaxidByName("Bacteria",1) 
        print( getRankByTaxid("2") )
        print( getRankByName("Viruses") )
        print( getRankByName("Flavobacteria") )
        print( getNameByTaxid("12085") )
        print( getParentByTaxid("31708") )
        print( getParentByName("Flavobacteria") )
        print( getPathByTaxid("46409") )
        print( getTaxidByGi("357542294") )      # Rhinovirus A => 147711
        print( getSonsByTaxid("35278") )
        print( getSonsByTaxid("147711") )
        print( "next one is get gi from taxid... it could return a long list if no limit used!!" )
        limit = 40;
        print( getGiByTaxid("147711")[:limit] )
        print( getAllSonsByTaxid("147711")[:limit] )
        print( getAllGiByTaxid("147711")[:limit] )
#tester1()-end

def tester2():
            # http://www.ncbi.nlm.nih.gov/protein/357542294 (Rhinovirus A)
            print( "==>> fn test ==>>" )
            print( getLineageByGi("357542294","species") ) 
            print( getLineageByGi("357542294","genus"  ) ) 
            print( getLineageByGi("21466137","order" ) )            # 21466137 from Zea mays
            print( getLineageByGi("21466137","family" ) ) 
            print( getLineageByGi("21466137","subfamily" ) ) 
            print( getLineageByGi("21466137","subclass" ) )       # some may not have sub-class...
            print( getLineageByGi("21466137","class" ) ) 
            print( getLineageByGi("21466137","phylum" ) ) 
            print( getLineageByGi("21466137","kingdom" ) ) 
            print( getLineageByGi("357542294","superkingdom" ) ) 
            print( getLineageByTaxid("4577","subtribe" ) )         # Zea mays
            print( getLineageByTaxid("4577","tribe" ) )         # Zea mays
            #print( getLineageByGi("357542294","phylum" ) )          ## error catching test: will raise exception NO matching rank, no more parent (string exception)
            #print( getLineageByGi("357542294","loopforever"  ) )   ## error catching test
#tester2()-end
            print( "<<==\n\n" )

def tester2_trace():
        try:  
            print( "==>> lineage trace test ==>>" )
            print( getTraceByTaxid( "9406" ) )
            return
            # tmp end
            print( getTraceByTaxid( "147711" ) )
            print( getTraceByTaxid( "12085" ) )
            print( getTraceByGi( "7769644" ) )        # gp120 HIV envelope
            print( getTraceByGi( "284794136" ) )      # ebola
            print( getTraceByGi( "30721694" ) )      # hemoglobin, synthetic
            print( getTraceByGi( "162462053" ) )      # hemoglobin, Zea mays
            print( getTraceByGi( "918400562i" ) )      # hemoglobin, Homo sapiens
            print( getTraceByGi( "524426976" ) )      # hemoglobin, Clostidium spp
            print( getTraceByGi( "316983257" ) )      # EHEC
            print( getTraceByGi( "48869529" ) )      # large antigen, bkv
            print( getTraceByGi( "48869528" ) )      # vp1 protein, bkv
            print( getTraceByGi( "474422278" ) )      # amoeba
            print( getTraceByGi( "951272730" ) )      # HSV-1, ds-dna virus 
            print( getTraceByGi( "145579197" ) )      # rotavirus (ds RNA)
            print( getTraceByTaxid( "1346816" ) )       # Circo-like virus-Brazil hs2 (ssDNA virus)
            print( "<<==\n\n" )
        #except NO_MORE_PARENT:
        except NameError:
            print( "something searched for bad rank, it didn't work." )
            raise       # re-raise the original exception to get a strack trace
#tester2_trace()-end

def simple_tester():
        try:  
            print( "==>> fn test ==>>" )
            print( "GI 357542294" )
            print( getLineageByGi("357542294","species") ) 
            print( getLineageByGi("357542294","genus"  ) ) 
            print( getLineageByGi("357542294","superkingdom" ) ) 
            print( "<<==\n\n" )
            print( "==>> lineage trace test ==>>" )
            print( "Taxid 147711" )
            print( getTraceByTaxid( "147711" ) )
            # [('species', 'Rhinovirus A'), ('genus', 'Enterovirus'), ('family', 'Picornaviridae'), ('order', 'Picornavirales'), ('no rank', 'ssRNA positive-strand viruses, no DNA stage'), ('no rank', 'ssRNA viruses'), ('superkingdom', 'Viruses'), ('no rank', 'root')]
            print( "\n" )
            print( "GI 162462053" )
            print( getTraceByGi( "162462053" ) )      # hemoglobin, Zea mays
            print( "<<==\n\n" )
            #print( getLineageByGi("357542294","phylum" ) )          ## error catching test: will raise exception NO matching rank, no more parent (string exception)
            #print( getLineageByGi("357542294","loopforever"  ) )   ## error catching test
        except NameError:
            print( "something searched for bad rank, it didn't work." )
            raise       # re-raise the original exception to get a strack trace
# simple_tester()-end

def test_and_study(gi="160700558") :
    try : 
            #print( getRankByGi( "422933597" ) ) # i coded this version, and called bi
            print( "==>> getRankByGi( %s) ==>>" % gi )
            print( getRankByGi( gi ) )                 # eg species
            print( getLineageByGi(gi,"species") )      # eg Cyprinid herpesvirus 1
            print( getLineageByGi(gi,"genus"  ) )      # eg Cyprinivirus 
            print( getLineageByGi(gi,"superkingdom" ) ) 
            print( getTaxidByGi( gi ) )                # eg 317858
            print( "==>> getSonsByTaxid(%s)" % gi )              #
            print( getSonsByTaxid(gi) )                   # [] list, could be empty
            print( "<<== \n" )
            print( "==>> getTraceByGi  %s ==>>" % gi )
            print( getTraceByGi( gi ) )      #  eg [('no rank', 'Snake adenovirus 1'), ('species', 'Snake adenovirus A'), ('genus', 'Atadenovirus'), ('family', 'Adenoviridae'), ('no rank', 'dsDNA viruses, no RNA stage'), ('superkingdom', 'Viruses'), ('no rank', 'root')
            print( "<<==\n" )
    except NameError:
            print( "something searched for bad rank, it didn't work." )
            raise       # re-raise the original exception to get a strack trace
# test_and_study()-end

        

def test_and_study_old() :
    try : 
            #print( getRankByGi( "422933597" ) ) # i coded this version, and called bi
            print( "==>> getRankByGi( 422933597 ) ==>>" )
            print( getRankByGi( "422933597" ) )                 # species
            print( getLineageByGi("422933597","species") )      # Cyprinid herpesvirus 1
            print( getLineageByGi("422933597","genus"  ) )      # Cyprinivirus 
            print( getLineageByGi("422933597","superkingdom" ) ) 
            print( getTaxidByGi( "422933597" ) )                # 317858
            print( "==>> getSonsByTaxid(317858)" )              #
            print( getSonsByTaxid("317858") )                   # [] ie empty!
            print( "<<== \n" )
            #print( "==>> lineage trace test ==>>" )
            #print( "Taxid 147711" )
            #print( getTraceByTaxid( "147711" ) )
            print( "==>> getTraceByGi  422933597 ==>>" )
            print( getTraceByGi( "422933597" ) )      # [('species', 'Cyprinid herpesvirus 1'), ('genus', 'Cyprinivirus'), ('family', 'Alloherpesviridae'), ('order', 'Herpesvirales'), ('no rank', 'dsDNA viruses, no RNA stage'), ('superkingdom', 'Viruses'), ('no rank', 'root')]
            print( "<<==\n\n" )
            #print( getLineageByGi("357542294","phylum" ) )          ## error catching test: will raise exception NO matching rank, no more parent (string exception)
            #print( getLineageByGi("357542294","loopforever"  ) )   ## error catching test
    except NameError:
            print( "something searched for bad rank, it didn't work." )
            raise       # re-raise the original exception to get a strack trace
# test_and_study()-end

        



def tester1a():
        print( "== * * * ==\n\n" )
        print( getNameByTaxid( getTaxidByGi("357542294") ) )      # Rhinovirus A => 147711
        print( getRankByTaxid( getTaxidByGi("357542294") ) + "\n" )  

        print( getNameByTaxid( getParentByTaxid( getTaxidByGi("357542294") ) ) ) 
        print( getRankByTaxid( getParentByTaxid( getTaxidByGi("357542294") ) )   + "\n" )  

        print( getNameByTaxid( getParentByTaxid( getParentByTaxid( getTaxidByGi("357542294") ) ) ) )
        print( getRankByTaxid( getParentByTaxid( getParentByTaxid( getTaxidByGi("357542294") ) ) )   + "\n" )  
#tester1a()-end

def blastfile_tester():
    # presets
    baseDir      = '/home/sys_usem_scriptr/taxo_browser/db/'
    testFileP5   = '/clscratch/skewepe1/blast_files_for_tin/protein_blast_hits.txt.head5'
    testFileP5d  = baseDir + 'blast_input/protein_blast_hits.txt.head5+dup'
    testFileP100 = '/clscratch/skewepe1/blast_files_for_tin/protein_blast_hits.txt.head100'    #   14sec /    92  (time -p data)   
    testFileP1k  = '/clscratch/skewepe1/blast_files_for_tin/protein_blast_hits.txt.head1k'     #   39sec /   720  (57s for faimily lookup)
    testFileP10k = '/clscratch/skewepe1/blast_files_for_tin/protein_blast_hits.txt.head10k'    #  178sec /  4278  (slt0009, 41sec for 1000 lookup)   v2: 741s (12m) full trace summary    
    testFileP = '/clscratch/skewepe1/blast_files_for_tin/protein_blast_hits.txt'            # 1387sec / 42916 uniq gi full.out sky-4-1 (23 min), [species? only single taxidFromGi() lookup, no 2D hash process yet] 
                                                                                            # 1076sec 3rd run w/ print 1076.66 sec
                                                                                            # 3644s   61 min on slt0009 for summary output for family lookup (summarizeGiList takes a long while!) 
                                                                                            # 6468s (107min) on sky4-1 for summary output for order
                                                                                            # 6005s (100min) repeat run on sky4-1 , no not much speed up, thoug sky41 idled for many hours
    testFileN = '/clscratch/skewepe1/blast_files_for_tin/nucleotide_blast_hits.txt.head5'
    #testFileN = baseDir + '/clscratch/skewepe1/blast_files_for_tin/protein+nucleotide_blast_hits.txt.head10'
    testFileN10 = baseDir + 'blast_input/protein+nucleotide_blast_hits.txt.head10'
    testFileN1k = 'blast_input/nucleotide_blast_hits.txt.head1k'            #  19 uniq GI
    testFileN2k = 'blast_input/protein+nucleotide_blast_hits.txt.head2k'    # 700-something uniq gi  
    testFileNPf = 'blast_input/protein+nucleotide_blast_hits.txt'           # full, 42944 uniq gi.  46547s (13 hr sky4-1), cuz of inefficient query.  will rewrite.  
                                                                            # 7591s (126min) v2.2 after rewrite query with lineage trace, still ~2x of looking up family only (61min)
                # v3 draft, really just reading gi and getting first taxid.  some 5+ million GI, and this looked up each into taxid first, thus took forever.  45,135 s = 12.5 h
    ##inputFile = testFileP1k
    inputFile = testFileP5d             # ** good example input to use, with dups and GIs that map to same taxid
    #inputFile = testFileP100
    #inputFile = testFileN1k
    #inputFile = testFileNPf         # ** final mega input file    FIXME ++
    outfile   = baseDir + 'blast_test/result_file_3.txt'
# FIXME ++ 
    ### define input/run params
    #rank2search = "species"
    #rank2search = "superkingdom"        # no kingdom data


    # execute test
    #uniqGiList = blastFile2giList( inputFile )              ## hmm... if multiple copy of GI, do i want to keep count of them??  ++
    giFreqList = blastFile2giList( inputFile )              ## hmm... if multiple copy of GI, do i want to keep count of them??  ++
    #table = summarizeGiList( uniqGiList, rank2search )     # for now only track one rank, but really should produce compleate lineage summary table ??
    #table = summarizeGiList( uniqGiList )
    taxidFreqTable = summarizeGiList( giFreqList )
    prettyPrint( taxidFreqTable, outfile ) 
    #taxoTree.prettyPrint( outfile ) 
    #table.


    # write test output to file
    #prettyPrint_v3( taxoTree, outfile )
    #prettyPrint( table, outfile )
    
    '''
    outFH = open( outfile, 'w' )
    # outFH.write( table )          # get type Error
    print( table, file = outFH )
    outFH.close()
    print( "result stored in %s" % outfile )
    '''

    return 0
# blastfile_tester()-end


#def prettyPrintDictionary( myHash ):
    #print( myHash )
    # ideally loop around, and get rid of funny u char.  but really need to write to file...
    #http://stackoverflow.com/questions/12592544/typeerror-expected-a-character-buffer-object
    #return


def rankSetAutoNumTest() :
        msg = "rankTest Low: %s " % RankSet.getLowest().name 
        print( msg )
        msg2 = "rankTest parent: %s " % RankSet.getParent( RankSet.getLowest() ) 
        print( msg2 )
        msg3 = "rankTest child:  %s " % RankSet.getChild( RankSet.getParent( RankSet.getLowest() ) )
        print( msg3 )
        r = RankSet.getChild( RankSet.getLowest() )
        if r is None :
                print( "got None from RankSet fn call..." )

        msg4 = "Enum test on RankSet: %s" % RankSet(1).value
        print( msg4 )
        msg4 = "Enum test on RankSet: %s" % RankSet(3).name
        print( msg4 )
        msg4 = "Enum test on RankSet: %s" % RankSet['genus']
        print( msg4 )
        msg4 = "Enum test on RankSet: %s" % RankSet.getParent(RankSet['genus'])
        print( msg4 )
# rankSet class autonum test end

def giListTest() :
        giLst = GiList()
        giLst.upCount( 123 )
        giLst.upCount( 123 )
        giLst.upCount( 123 )
        msg1 = "giListTest: %s" % giLst.getCount( 123 )
        print( msg1 )
        


def main_simple_test():
        taxidList = ( 9606, 10360, 11105, )
        # taxidList = [ 10360, 11105, 9606, 10306, 83332, 1280, 11676, 10759, 11709, 83333, 9913, 169066 ]
        # taxidList = [ 562, 333284, 11105, 10759, 103903, 1280, 227859, 12081, 10306, 10407, 10116, 224308, 518987, 11103, 277944, 268305, 11082, 41856, 39113, 10760, 31634, 11676, 928306, 10370, 9606, 11866, 11709, 10338, 506350, 10665, 11970, 83333, 11678, 169066, 9913, 11104, 31708, 10360, 272563, 211044, 83332, 11888, 10359, 271, 10299 ]

        for taxid in taxidList :
            print( "taxid: %s" % taxid )
            linList =  getTraceByTaxid( taxid )
            lineageTracePrint( linList )
            print( "" )
# main_simple_test()-end


def main():
                # v 0.6 tests:
        #tester1()
        #tester2()
        tester2_trace()
        #simple_tester()
        #test_and_study()
        #test_and_study("160700558") # this gi is at subspecies level (no rank)
        #blastfile_tester()
        #rankSetAutoNumTest()
        #giListTest()

        # v 0.7 tests:
        #tester_v7()
# main()-end


### end of all fn definition, begin of main program flow.
main()

