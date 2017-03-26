#!/bin/bash 

## export the tree table from sqlite to csv
## just run as bash ./export.sh

## i think this was used cuz there were some tricks 
## (need to run a script found on the web) to create the tree table
## and i wanted the data in Apache Spark, 
## so just took a dump of what was already done in python/sqlite and imported there

## at this point, not svn ci 
## as it is used only by the spark workstream that i was working on, 
## maybe check into taxo-spark "fork" later on.

## maybe want to see how that import was done.  
## but that process uses create_name_node_db.py 
## which does some complex-ish way to load the db
## and i don't want to deal with that at this point

sqlite3 /prj/idinfo/taxo/ncbi-taxo-acc-2016-0424.db <<!
.headers on
.mode csv
.output tree.table.csv
select * from tree;
!
