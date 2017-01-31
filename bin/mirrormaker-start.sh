#!/usr/bin/env bash

BASE_DIR=$(cd `dirname ${BASH_SOURCE[0]}`/.. && pwd)
CONF_DIR=${BASE_DIR}/conf
BIN_DIR=${BASE_DIR}/bin
LIB_DIR=${BASE_DIR}/lib
LOG_DIR=$BASE_DIR/logs

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

if [ x"" == x${JAVA_HOME} ]; then
    JAVA=java
else
    JAVA=${JAVA_HOME}/bin/java
fi

# JMX settings
if [ -z "$KAFKA_JMX_OPTS" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log4j settings
if [ -z "$LOG4J_OPTS" ]; then
  # Log to console. This is a tool.
  LOG4J_OPTS="-Dlog4j.configuration=file:$BASE_DIR/conf/tools-log4j.properties"
else
  # create logs directory
  if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
  fi
fi

# Memory options
if [ -z "$KAFKA_HEAP_OPTS" ]; then
  KAFKA_HEAP_OPTS="-Xmx256M"
fi
# JVM performance options
if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
  KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

for l in `ls -d ${LIB_DIR}/*`
do
    CLASSPATH=${CLASSPATH}:$l
done

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done


# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA -cp $CLASSPATH $JAVA_OPTS $LOG4J_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_HEAP_OPTS $KAFKA_JMX_OPTS kafka.skplanet.mirrormaker.MirrorMaker "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  $JAVA -cp $CLASSPATH $JAVA_OPTS $LOG4J_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_HEAP_OPTS $KAFKA_JMX_OPTS kafka.skplanet.mirrormaker.MirrorMaker "$@"
fi


