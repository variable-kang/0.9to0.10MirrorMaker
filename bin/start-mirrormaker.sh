#!/usr/bin/env bash

BASE_DIR=$(cd `dirname ${BASH_SOURCE[0]}`/.. && pwd)
CONF_DIR=${BASE_DIR}/conf
BIN_DIR=${BASE_DIR}/bin
LIB_DIR=${BASE_DIR}/lib

if [ x"" == x${JAVA_HOME} ]; then
    JAVA=java
else
    JAVA=${JAVA_HOME}/bin/java
fi

for l in `ls -d ${LIB_DIR}/*`
do
    CLASSPATH=${CLASSPATH}:$l
done

exec $JAVA -cp $CLASSPATH $JAVA_OPTS -Xms1g -Xmx1g kafka.mirrormaker.MirrorMaker $@