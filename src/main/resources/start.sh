#!/bin/bash

HOME=`pwd`
PID_FILE="/tmp/rkr-kariz2.pid"

HEAP="-Xmx15g"

CONF_FILE="-Dconfig.file=${HOME}/application.hocon"
LOG4J_FILE="-Dlog4j2.configurationFile=${HOME}/log4j2.xml"

JAR_FILE=`ls ${HOME}/kariz2*dependencies.jar`

if [[ ! -f ${JAR_FILE} ]];then
   echo ${JAR_FILE}
   echo "Jar file does not exist"
   exit
fi

#/opt/kariz/redis-server &

java ${CONF_FILE} ${LOG4J_FILE} ${HEAP} -XX:+UseConcMarkSweepGC -jar ${JAR_FILE}

echo $! > $PID_FILE
