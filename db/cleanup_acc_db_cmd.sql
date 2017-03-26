-- cleanup_acc_db_cmd.sql 
-- undo create_acc_db_cmd.sql, 
-- used during dev/troubleshooting
-- run as :
-- sqlite3 ncbi-taxo-${VER}.db < cleanup_acc_db_cmd.sql
-- or (in sqlite command session) :
-- .read cleanup_db_cmd.sql

--CREATE UNIQUE INDEX     acc_idx_on_acc_taxid ON acc_taxid(acc);
--CREATE INDEX        acc_ver_idx_on_acc_taxid ON acc_taxid(acc);
--CREATE INDEX          taxid_idx_on_acc_taxid ON acc_taxid(taxid); 


-- in case need cleanup, undo:
drop table acc_taxid;
drop index       acc_idx_on_acc_taxid;
drop index   acc_ver_idx_on_acc_taxid;
drop index     taxid_idx_on_acc_taxid;

.mode list
-- list tables, the acc_taxid table should no longer be there
.table  
--list indexes, the 3 indexes should be gone.
.ind    
