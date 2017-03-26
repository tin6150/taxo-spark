-- create_acc_db_cmd.sql 
-- sqlite3 db creation and loading script
-- run as
-- sqlite3 ncbi-taxo-${VER}.db < create_acc_db_cmd.sql
-- or
-- .read create_acc_db_cmd.sql
-- https://www.sqlite.org/lang_comment.html

-- modeled after old gi version use import_taxo.sql, which is no longer needed 
-- this version intended for search using accession.version instead of gi

-- v 0.7.3 use hash for accession and acc.ver, hoping to get better search perf, but no longer needed
--create table acc_taxid(acc_hash integer, acc_ver_hash integer PRIMARY KEY, taxid integer);
-- v 0.7.4 use text for acc.v, now takes ~1s to complete (similar to gi, which  used ~1s)
-- largely the same as v 0.7.1.  
-- Not declaring PRIMARY KEY on acc_ver, as that just make DB file larger, and takes longer to load/import
--create table acc_taxid(acc text, acc_ver text, taxid integer, gi integer);
create table acc_taxid(acc text, acc_ver text PRIMARY KEY, taxid integer, gi integer);

.mode list
.separator \t
--pragma temp_store_directory;
pragma temp_store = 2;            -- use RAM

-- v 0.7.1:
--CREATE UNIQUE INDEX  acc_ver_idx_on_acc_taxid ON acc_taxid(acc_ver);  -- UNIQUE would be like PK, just adds overhead
CREATE INDEX  acc_ver_idx_on_acc_taxid ON acc_taxid(acc_ver);    
CREATE INDEX    taxid_idx_on_acc_taxid ON acc_taxid(taxid); 
--CREATE INDEX      acc_idx_on_acc_taxid ON acc_taxid(acc);				-- skipping for now, may never need to search on this column


.schema
.ind
-- .import below is same as v0.7.1 (stripped header line)
.import download/prot+nucl_wgs+nucl_gb+nucl_est+nucl_gss.accession2taxid  acc_taxid   
-- below has header line, will violate PK and error out by 2nd file starts to load
-- but really okay to use for this v0.7.4 with no PK declaration.  save time on stripping header.
--.import download/nucl_est.accession2taxid    acc_taxid
--.import download/nucl_gb.accession2taxid     acc_taxid
--.import download/nucl_gss.accession2taxid    acc_taxid
--.import download/nucl_wgs.accession2taxid    acc_taxid
--.import download/prot.accession2taxid        acc_taxid    
-- to speed loading, can declare PRIMARY KEY constrain after loading DB
-- but for our purpose, the PK really just increase file size and adds no benefits, so not doing.
--
-- In case need to undo this schema, run cleanup_acc_db_cmd.sql


