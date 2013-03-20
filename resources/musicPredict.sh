#!/bin/bash

#PBS -q small
#PBS -N Music
#PBS -l nodes=1:ppn=1
#PBS -o oana.out
#PBS -e oana.err
#PBS -l walltime=06:10:00
#PBS -A cs240a-ucsb-34
#PBS -V
#PBS -M oana.catu@gmail.com
#PBS -m abe

# Shell program to execute WordCount using Hadoop.
# To run this program, you need to make sure ~/log/templog1 file exists as the input file.
# no other files need to be created first. All output files are created under ~/log
# Then you can do "qsub log.sh" to execute this file in a queue
# or you can do "qsub -I", and then "sh log.sh" as an interactive mode

# The above instructions set up 1 node and allocates at most 10 mins. 
# the hadoop file system will be namted cs240a-ucsb-34  
# under /state/partition1/hadoop-$USER/data  
# the trace log files are tyang.out and tyang.err



# Set this to location of myHadoop on Triton. That is the system code/libry etc.
# used to run Hadoop/Mapreduce under a supercomputer environment
# Notice the mapreduce Hadoop directory is available in the cluster allocated
#   for exeucting your job (but not necessarily in the triton login node).
export MY_HADOOP_HOME="/opt/hadoop/hadoop-0.20.2/contrib/myHadoop"
export HADOOP_LOG_DIR="/state/partition1/hadoop-cs240a-ucsb-34/log"

# Set this to the location of Hadoop on Triton
export HADOOP_HOME="/opt/hadoop/hadoop-0.20.2"

#### Set this to the directory where Hadoop configs should be generated
# under "/home/cs240a-ucsb-34/wc1/ConfDir"
# that is done automatically based on the above configuration setup.
# Don't change the name of this variable (HADOOP_CONF_DIR) as it is
# required by Hadoop - all config files will be picked up from here
#
# Make sure that this is accessible to all nodes
export HADOOP_CONF_DIR="/home/cs240a-ucsb-34/music/ConfDir"


#### Set up the configuration
# Make sure number of nodes is the same as what you have requested from PBS
# usage: $MY_HADOOP_HOME/bin/configure.sh -h
echo "Set up the configurations for myHadoop please..."
$MY_HADOOP_HOME/bin/configure.sh -n 1 -c $HADOOP_CONF_DIR 

# -p -d "/home/cs240a-ucsb-34/mapreduce/result"

#Format the Hadoop file system initially. It is possible that you donot need to call when executing multiple times.
# But after a while, this file system will be removed anyway, thus we may have to recreate again.
echo "Format HDFS"
#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR namenode -format
echo

#### Start the Hadoop cluster
echo "Start all Hadoop daemons"
$HADOOP_HOME/bin/start-all.sh
$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave

#### Clear Result folder
#rm /home/cs240a-ucsb-34/result/*

#### Run your jobs here


#echo "Creating input directory in HDFS "
#The input data is created under the hadoop system with name input/a. 
# The data is fetched from the local system ~/wc1/a.
#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -mkdir in1 
echo "Copy data to HDFS "

#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -D dfs.block.size=134217728 -copyFromLocal ~/log/templog1 input/a
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal ~/music/trainpreprocessed_test.txt input/a

#Now we really start to run this job. The intput and output directories are under the hadoop file system.
echo "Run log analysis job .."
time $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ~/music/musicanalyzer.jar MusicPredictor input output

echo "Check output files after PC.. but i remove the old output data first"
rm -r ~/music/predict
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal output ~/music/predict
cp -r  $HADOOP_HOME/logs ~/music
rm -r ~/music/log
cp -r $HADOOP_LOG_DIR ~/music


#### Stop the Hadoop cluster
echo "Stop all Hadoop daemons"
$HADOOP_HOME/bin/stop-all.sh
echo

#### Clean up the working directories after job completion
## that may remove the filesystem created.
echo "Clean up .."
$MY_HADOOP_HOME/bin/cleanup.sh -n 1
echo
