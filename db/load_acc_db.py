
### ****** abandoned... too slow ******* 

## load database, the accession.version to tax_id db
## the db need to be created.  db filename is passed as first argument.
## example run:
##  cd /home/script/taxo_browser/db/download 
##  python load_acc_db.py  ncbi-taxo-acc-2016-0413.db    nucl_est.accession2taxid       
##  python load_acc_db.py  ncbi-taxo-acc-2016-0413.db    prot.accession2taxid   
##  ie, call to load each input file iteratively.  


# code v 0.7.2    use hash(accession_version)  so that it is integer.
# DB expect to be created as:   
# CREATE TABLE acc_taxid( hash_acc INTEGER, hash_acc_ver INTEGER PRIMARY KEY, taxid integer);


import sys, sqlite3


# maybe able to make it loop, but let db.setup.cmd.sh call on multiple files... 
# but if decide to load multiple input file can do so with minimal coding
def loadfile( DBfile, INfile ):
    conn = sqlite3.connect(DBfile)
    cursor = conn.cursor()


    for line in open( INfile, 'r' ):
        line = line.rstrip("\n\r") 
        lineList = line.split( "\t" )
        (accession, accession_version, tax_id, gi) = lineList 
        #(accession, accession_version, tax_id, gi) = line.strip().split("\t")
        ## need to strip header row that reads "accession   accession.version   taxid    gi"     (tab separated)
        ## FIXME TODO header stripping not working
        if( accession == "accession" ):
            continue 
        ha = hash(accession)
        hav = hash(accession_version)
        #command = "INSERT INTO tree VALUES ('" + taxid + "', '" + tree[taxid][0].replace("'","''") + "', '" + tree[taxid][1] + "','" + tree[taxid][2] +"');"
        try:
                command = "INSERT INTO acc_taxid VALUES ('" + str(ha) + "', '" + str(hav) + "', '" + tax_id + "');"
                # gi is dropped
                #print( "acc: %s \t hash %s \t -- acc.v: %s \t hash %s" % (accession, ha, accession_version, hav))
                #print( command )
                cursor.execute(command)
               
        except sqlite3.IntegrityError :
                print( "Primary key violation (hash on accession.version crash) caused by input line: %s" % line )
                # since exception is catched, the db loading continues to the next line :)
    conn.commit()
    return




def main():
    DBfile = sys.argv[1]
    INfile = sys.argv[2]
    print( "DB is: %s" % DBfile )
    print( "input file is: %s" % INfile )
    loadfile( DBfile, INfile )
      

main()
