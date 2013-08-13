#!/bin/sh

. $ACCISMUS_HOME/conf/accismus-env.sh

$ACCUMULO_HOME/bin/tool.sh ../target/accismus-benchmark-0.0.1-SNAPSHOT.jar org.apache.accumulo.accismus.benchmark.Generator -libjars "$ACCISMUS_HOME/lib/accismus-0.0.1-SNAPSHOT.jar" "$@"
