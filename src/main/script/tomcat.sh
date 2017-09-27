#!/bin/sh

PRGDIR="/home/yshi/Applications/tomcat"
EXECUTABLE="catalina.sh"

if [ "$#" -ne 2 ]
then
  echo "Usage: $0 DIRECTORY"
  exit 1
fi

if [ "$2" = "knowledge" ]
then
    export CATALINA_BASE="$PRGDIR/catalina_base/knowledge"
    DPORT="5006"
fi

if [ "$1" = "start" ]
then
    ARG="start"
    export JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007"
elif [ "$1" = "stop" ]
then
    ARG="stop"
fi

CMD="$PRGDIR/bin/$EXECUTABLE $ARG"
echo "CATALINA_BASE=$CATALINA_BASE"
echo "JAVA_OPTS=$JAVA_OPTS"
eval "$CMD"
