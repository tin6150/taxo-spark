

#trying to submit the job, so that it use more than one node.

export PYTHONPATH=/home/bofh1/code/svn/taxo-spark/:/home/bofh1/local_python_2.7.9/lib/python2.7/site-packages/


# http://spark.apache.org/docs/latest/submitting-applications.html

# ./bin/spark-submit \
#  --class <main-class>
#    --master <master-url> \
#      --deploy-mode <deploy-mode> \
#        --conf <key>=<value> \
#          ... # other options
#            <application-jar> \
#              [application-arguments]


# need master, try:

# http://clp24001:8088/cluster

   #--deploy-mode local[4] \
   #--deploy-mode client \              # this work, but app id clearly says local.
#spark-submit --master spark://clp24001:7077 \
#spark-submit --master yarn://clp24001 \

# submit from nrusca-clp24900 and there is no need to specify hostname or port for yarn
date
   # this somehow make job not in yarn, not visible in history server...
   # but the log get created... eg shows can combine conf multiple times, not sure what's on...  maybe eg is wrong!
   # http://stackoverflow.com/questions/31233830/apache-spark-setting-spark-eventlog-enabled-and-spark-eventlog-dir-at-submit-or
   #--conf "spark.eventLog.enabled=true" --conf "spark.eventLog.dir=file:///home/bofh1/pub" \
spark-submit \
 --master yarn-client \
 --driver-memory=16G \
 --conf spark.yarn.am.memory=16G \
 --conf spark.executor.instances=2 \
 --conf spark.executor.cores=8 \
 --conf spark.executor.memory=8G \
 --conf spark.cores.max=80 \
        taxorpt_spark.py \
        --db /home/bofh1/code/svn/taxo-spark/db/ncbi-taxo-acc-2016-0424.db -c3 -s'|'  -ddd \
             --text \
             /home/bofh1/code/svn/taxo-spark/db/protein_blast_hits.txt.head100  \
             /home/bofh1/pub/taxorpt.txt 

 #--conf spark.executor.cores=40 \ #number of cores per executor
 #--conf spark.cores.max=80 \  #total number of cores (corse per executor x executor instances)
 #--conf spark.cores.max=80 \  #total number of cores (corse per executor x executor instances)

#   these options worked, but may have requested too much resources and j.calvert suggested above
#   --master yarn \
#   --deploy-mode cluster \
#   --driver-memory 16G \
#   --executor-memory 8G \
#   --total-executor-cores 80 \
## --num-executors 4
## defaults to 2?  then i should not be chewing up too many?

             # /home/bofh1/code/svn/taxo-spark/db/protein+nucleotide_blast_hits.txt.head10 \
  # spark_acc2taxid_3.py   
echo $?
echo '<-- exit code from spark-submit job' 

date; 
ls -l ~/pub/taxorpt-spark.out; 
cat ~/pub/taxorpt-spark.out;



#spark-submit --master yarn --deploy-mode cluster "hdfs:///user/sys_hadoop/spark_acc2taxid_2_cleaned.py"

# changed code to have SparcContext( appname = ... ) instead of local, now time -p say takes about 78sec, while spark reports 58s.
# but output isn't obvious, at least not on console.  should write to fie... :x

