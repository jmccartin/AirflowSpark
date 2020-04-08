#!/bin/bash

# Exports the configuration directories so that
# nodes can be tested on an EMR cluster
export HADOOP_CONF_DIR=/tmp/airflow/test/
export SPARK_CONF_DIR=/tmp/airflow/test/
export YARN_CONF_DIR=/tmp/airflow/test/
