#!/usr/bin/env bash

DIR=`pwd`
NAME="csv2d4m"
cd ${NAME}
java -jar ${NAME}-0.0.1.jar -i /csv -t 500cities -fs hdfs://haz00:9000 -l accumulo -zk haz00:2181 -ow -r -c file:///home/haz/accumulo-creds.yml
cd $DIR