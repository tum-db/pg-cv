#!/bin/bash
# Remove the trailing separator from the data for postgres compability
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

for i in `ls $DIR/*.tbl`; do
  table=${i/.tbl/}
  echo "Loading $table..."
  sed 's/|$//' $i > $i.fixed
  mv $i.fixed $i 
done
