#!/bin/bash 

## commands to download NCBI taxonomy database
## some prep commands for setup db
## but setup of sqlite likely separate file...

## this script maybe run by a cronjob on scriptr prod server nrusem-slp0019 under sys_usem_scriptr

download_db()
{
        mkdir download
        cd    download
        # note that there maybe incremental updates from NCBI instead of doing
        # full dump re-load db each time...

        #ref: http://dgg32.blogspot.com/2013/07/map-ncbi-taxonomy-and-gi-into-sqlite.html
        #elinks  ftp://ftp.ncbi.nih.gov/pub/taxonomy/
        # gi_taxid_prot.dmp.gz and taxdmp.zip
        wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump_readme.txt 
        wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip
        #wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip.md5

        # not usng the gi to taxid anymore, as that is being phased out.
        #wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/gi_taxid_prot.dmp.gz 
        #wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/gi_taxid_prot.dmp.gz.md5 
        ## adding nucleotide db after talking to Peter on 2015.1221
        #wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/gi_taxid_nucl.dmp.gz
        #wget  ftp://ftp.ncbi.nih.gov/pub/taxonomy/gi_taxid_nucl.dmp.gz.md5 

        ## new method using accession.version rather than gi
        wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/README --output-document=README.accession2taxid 

        ## all these should go into a single table.  
        ## if optimizing for space/size is helpful, can drop 2 columns from these tables.
        wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/prot.accession2taxid.gz
        wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_wgs.accession2taxid.gz
        # rest of these are smaller, especially est and gss, being 10% of nucl wgs  downloading for now
        wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz
        wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_est.accession2taxid.gz
        wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_gss.accession2taxid.gz
        ## skipping the dead records (unless pete wants them)
}



extract_db()
{
        cd download
        #extract...
        for F in $( ls *gz ); do
                gunzip $F
        done
        unzip taxdmp.zip 
        rm citations.dmp delnodes.dmp division.dmp gc.prt gencode.dmp merged.dmp

}

massage_db()
{
        # cd download

        # strip header line and put all data into a single file
        # accession       accession.version       taxid   gi
        cat nucl_est.accession2taxid nucl_gb.accession2taxid nucl_gss.accession2taxid nucl_wgs.accession2taxid prot.accession2taxid | grep -v ^accession > prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid  

}


create_name_db()
{
        # run this fn in a machine with sqlite installed (not just the python
        # embeded sqlite lib, but the cli rpm) eg scriptr nrusem-slt0009, all nodes in c3po should have it too.
        # this fn takes about 40 sec to run. 

        # common stuff for tax_id space for names, parents, etc.  
        # same code for v 0.7.2, 0.7.1, 0.6 and prior
        # create the tree table using the taxo dump from NCBI:  some 1.4M records
        python create_name_node_db.py   download/names.dmp   download/nodes.dmp $DB   # create_name_node_db.py formerly pre_pyphy.py
        # index is created thru normal cli
        echo 'CREATE UNIQUE INDEX taxid_on_tree ON tree(taxid);' | sqlite3 $DB 
        echo 'CREATE INDEX name_on_tree ON tree(name);'          | sqlite3 $DB 
        echo 'CREATE INDEX parent_on_tree ON tree(parent);'      | sqlite3 $DB 
}



create_acc2taxid_db()
{
        # create and load db.  just use plain accession.version, not using hash.  really what was doing in v 0.7.2.  
        sqlite3 $DB < create_acc_db_cmd.sql   
        returnCode = $?
        return returnCode

}


clean_up()
{
        cd download
        rm accession2taxid.README 
        rm nucl_est.accession2taxid nucl_gb.accession2taxid nucl_gss.accession2taxid nucl_wgs.accession2taxid prot.accession2taxid
        rm prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid
        rm taxdmp.zip
        rm names.dmp nodes.dmp readme.txt taxdump_readme.txt

}


# "main"

#extract_db
VER=2016-04dd
DB=ncbi-taxo-acc-${VER}.db  
#DBPATH=/db/idinfo/taxo  


download_db
extract_db
# download and extract_db takes 4888.59 sec.

#create_name_db
#create_acc2taxid_db  
## add code that check if db creation was good, then move to /db/idinfo/taxo/ and create proper link
#clean_up


