#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES="typing"

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171001" #"20171103"

# end date
END_DATE="20180530" #"20171111"

# list of usersids separated by comma. Leave blank to process all users.

SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"

CC_EGG="/cerebralcortex/code/eggs/MD2K_Cerebral_Cortex-2.2.2-py3.6.egg"

PY_FILES=${CC_EGG} #",dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.1-py3.6.egg"


SPARK_UI_PORT=4066

MAX_CORES=24

# set to True to make use of spark parallel execution
SPARK_JOB="True"

# build before executing

# python3.6 setup.py bdist_egg

if [ $SPARK_JOB == 'True' ]
    then
        echo 'Executing Spark job'
        spark-submit --master $SPARK_MASTER \
                     --conf spark.ui.port=$SPARK_UI_PORT \
                     --conf spark.cores.max=$MAX_CORES \
                     --conf spark.app.name='data_quality' \
                     --py-files $PY_FILES \
                     analyze_calculated_data.py
    else
        echo 'Executing single threaded'
        export PYTHONPATH=.:${CC_EGG}:$PYTHONPATH
        echo $PYTHONPATH
        python3.6 phone_stream_analyzer -c $CC_CONFIG_FILEPATH \
                       -s $STUDY_NAME -sd $START_DATE \
                       -ed $END_DATE -u $USERIDS -f $FEATURES 

fi

