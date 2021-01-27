#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
MVN=${MVN:-./mvnw}
JAVA=${JAVA:-java}
JAVA_MEM=${JAVA_MEM:--Xmx1G -Xms1G}
JAR_PATH=target/freqel-1.0-SNAPSHOT.jar

if [ ! -e "$JAR_PATH" ]; then
  "$MVN" package -DskipTests=false
else
  LOGFILE=$(mktemp)
  echo -n "Rebuilding jar (will skip tests)..."
  if ! ("$MVN" package -DskipTests=true &> "$LOGFILE"); then
    echo ""
    cat "$LOGFILE" 1>&2
  else
    echo -e " OK\n"
  fi
  rm -f "$LOGFILE"
fi

"$JAVA" --add-opens java.base/java.lang=ALL-UNNAMED $JAVA_MEM -jar "$JAR_PATH" "$@"
