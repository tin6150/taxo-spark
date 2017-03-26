
To run Taxo reporter, do:


module load python/2.7.9-goolf-1.5.14-NX               
source /prj/idinfo/prog/local_python_2.7.9/bin/activate    # needed for enum34 and html 1.16

./taxorpt.py -h         # provide usage help 

# example run 
./taxorpt.py -c1 -s'|' --html -d  /home/script/taxo_browser/db/blast_input/protein_blast_hits.txt.head5 >   example_run.html


PS. use --db or perform this one time setup:
ln -s /prj/idinfo/taxo/ncbi-taxo-gi.db .
