
taxo-spark
==========

SparkSQL version of 
taxonomy reporter tool (https://github.com/tin6150/taxonomy_reporter)

Needed to use UDF to create extra column for taxonomy trace table.
But UDF I wrote utilized sqlite db (leverating old pyphy taxorpt code),
somehow that was very slow 
(sqlite db was being locked and await lock release?  shouldn't be, but it was horribly slow,
took 3+ day to run and hasn't finished.  took longer than serial version used by taxorpt,
thus a rewrite is needed for it to be practical.)


code as is runs, but result not verified yet as never got it to finish.
small input test produced ok table.  able to save result in parquet file (dir)
but never got it to produce data table or .html that taxorpt generated.

would need more time to work on this.  
maybe UDF can do same query using text file instead of sqlite db and that may avoid the lock wait?
but alternate approach would likely be better off.

other historical code for other approach would be added retroactively for potential reconsideration.
use git log to see them.


db
--

The db/ folder contain code that download NCBI taxonomy database, and massage it into a sqlite db
It can be run from cron for automatic db creation and update 



-sn5050

~~~~

.rst code block test :)


.. code:: bash

        # use 
        #
        # .. code:: bash 
        #     echo "Hello world"
        #
        # to create pre-formatted code block with syntax highlight.
        # be careful though, as (typo?) or (some system?) may treat it as execution directive
        # below should show command and not execution of 'date' 'hostname' and 'uptime' ?
        date 
        hostname
        uptime
        # okay, github seems to render this as code block with highlight... more test:
        FECHA=`date +%Y%m%d-%H%M`
        LIST=$( seq 1 10 )
        for ITEM in $LIST; do
                echo $ITEM > /dev/null 
        done

back to normal text here
