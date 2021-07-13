#!/bin/bash
exec java ${JVM_OPTS:-} -jar /opt/freqel-server.jar "$@"