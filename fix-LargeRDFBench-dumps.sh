#!/bin/bash
ACTION=$1
DUMPS_DIR=${DUMPS_DIR:-$2}
OUT_DIR=${OUT_DIR:-$3}

function fix_nt() {
  echo "Fixing $1 ..."
  # Replace " "
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(<https?://[^ >]*) (.*>)@\1%20\2@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done

  # Replace "|"
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(<https?://[^ >]*)\|(.*>)@\1%7C\2@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done

  # Replace "`"
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(<https?://[^ >]*)`(.*>)@\1%60\2@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done

  # Replace '"'
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(<https?://[^ >]*)"(.*>)@\1%60\2@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done

  # Wrap plain numbers as xsd:int
  sed -E 's@(<.*> *<.*> *)([0-9]+)( *\. *)@\1"\2"^^<http://www.w3.org/2001/XMLSchema#int>\2@' \
         "$1" > "$1.tmp"
  mv "$1.tmp" "$1"
}

function fix_rdf() {
  echo "Fixing $1 ..."

  # Replace " "
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(resource|about)=("https?://[^ ]*) (.*")@\1=\2%20\3@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done
  
  # Replace "|"
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(resource|about)=("https?://[^ ]*)\|(.*")@\1=\2%7C\3@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done

  # Replace "`"
  CHG=1
  while [ "$CHG" -ne 0 ]; do
    sed -E 's@(resource|about)=("https?://[^ ]*)`(.*")@\1=\2%60\3@g' "$1" > "$1.tmp"
    diff "$1" "$1.tmp"
    CHG=$?
    test "$CHG" -ne 0 && mv "$1.tmp" "$1" || rm "$1.tmp"
  done
}

function fix_file() {
  EXT="$(echo $1 | sed -E 's/^.*\.([^.]+)$/\1/')"
  if ( echo "$1" | grep -iE '^.*\.(n3|nt|ntriples|ttl|turtle)$' &>/dev/null ); then
    fix_nt "$1"
  fi
  if ( echo "$1" | grep -iE '^.*\.(rdf)$' &>/dev/null ); then
    fix_rdf "$1"
  fi
}

OK=1
if [ "$ACTION" != "all" -a "$ACTION" != "files" ]; then OK=0; fi
if [ "$ACTION" = "all" ]; then
  if [ -z "$DUMPS_DIR" -a -z "$OUT_DIR" ]; then
    echo "Arguments dumps_dir and out_dir required for action all."
    OK=0
  fi
fi

if [ "$OK" -ne 1 ]; then
  echo "Usage: $0 all|files [dumps_dir out_dir]"
  echo ""
  echo "Actions:"
  echo "    all: extracts and fixes all archives in dumps_dir, "
  echo "         saves new compressed archives in out_dir"
  echo "  files: fixes the files given as arguments. "
  echo "         Example: $0 files file1.nt file2.nt file3.nt ..."
  exit 1
fi


###########################
# $ACTION=files           #
###########################
if [ "$ACTION" = "files" ]; then
  FIRST=1
  for i in "$@"; do
    if [ "$FIRST" = "1" ]; then
      FIRST=0
    else
      fix_file "$i"
    fi
  done
  exit 0
fi

###########################
# $ACTION=all             #
###########################

DUMPS_DIR=$(realpath "$DUMPS_DIR")
OUT_DIR=$(realpath "$OUT_DIR")
mkdir -p "$OUT_DIR"

cd "$DUMPS_DIR"
for file in *.zip *.7z; do
  # Extract to a contained directory
  EXTR_DIR="$OUT_DIR/$(echo "$file" | sed -E 's/\.(zip|7z)$//')"
  rm -fr "$EXTR_DIR"
  mkdir -p "$EXTR_DIR"
  cd "$EXTR_DIR"
  EXT="$(echo $file | sed -E 's/^.*\.([^.]+)$/\1/')"
  echo "Extracting $DUMPS_DIR/$file to $EXTR_DIR ..."
  if [ "$EXT" = "zip" ]; then
    unzip "$DUMPS_DIR/$file"
  elif [ "$EXT" = "7z" ]; then
    7z x "$DUMPS_DIR/$file"
  else
    echo "Bad extension $EXT on $DUMPS_DIR/$file"
    exit 1
  fi

  # Do the magic
  OLD_IFS="$IFS"
  export IFS=$'\n'
  for i in $(find . -type f -iregex '.*\.\(n3\|nt\|ntriples\|ttl\|turtle\)' ); do fix_nt "$i"; done
  for i in $(find . -type f -iname '*.rdf' ); do fix_rdf "$i"; done
  export IFS="$OLD_IFS"

  # Compress
  echo "Compressing to $OUT_DIR/$file"
  rm -f "$OUT_DIR/$file"
  if [ "$EXT" = "zip" ]; then
    zip -9r "$OUT_DIR/$file" *
  elif [ "$EXT" = "7z" ]; then
    7z a "$OUT_DIR/$file" *
  fi

  #Cleanup
  cd "$DUMPS_DIR"
  rm -fr "$EXTR_DIR"
done

