

The database to power the taxonomy reporter depends on NCBI taxonomy and accession to taxid mappings.
they are stored in a sqlite3 db.  created as:

bash db.setup.cmd.sh


the above call a couple of helper scripts:
- create_name_node_db.py  # script from web, maps taxid to human readable names
- create_acc_db_cmd.sql   # commands for sqlite3 to create the accession to taxid mapping portion of the db, loading data as well.

There is a cleanup_db_cmd.sql that was used during development when db creation was bad and need to be redone.  
Included into svn just to facilitate future dev effort.

example machines to use for sqlite3: emv-nx, c3po, skywalker-4-10

------

example interaction with sqlite3 database.  This is just for sanity check that
db was created and loaded correctly.
the python scripts do not use the sqlite3 cli to access the db, but python
sqlite library API.  

sqlite3 ncbi-taxo.db

## Enter ".help" for usage hints.
## exmple queries:


SELECT taxid FROM tree WHERE name = "Proteobacteria";

SELECT name FROM tree WHERE taxid = '2';

SELECT parent FROM tree WHERE taxid = '976';


## 2015.1221 adding nucleotide dump

SELECT taxid FROM tree WHERE name = "Piconaviridae";
SELECT name FROM tree WHERE taxid = 12058;

SELECT taxid FROM tree WHERE name = "Enteroviruses"
SELECT name FROM tree WHERE taxid = 12059;

SELECT taxid FROM tree WHERE name = "ssRNA positive-strand viruses, no DNA
stage";
SELECT name FROM tree WHERE taxid = 35278;
SELECT name FROM tree WHERE taxid = 464095;
SELECT name FROM tree WHERE taxid = 31708;

PS.  A new version of the database, 
sqlite3 ncbi-taxo-2016.0408.db, is being converted to use accession.version
rather than gi
