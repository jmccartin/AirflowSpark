#!/bin/bash
# To be run on the master node of a Spark EMR cluster
declare USER="airflow"
echo "Setting up directories on HDFS for user: ${USER}"
set -o xtrace
hdfs dfs -mkdir /user/$USER
hdfs dfs -chown -R $USER /user/$USER
hdfs dfs -chmod -R 770 /user/$USER