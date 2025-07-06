#!/bin/bash

set -e
BINDIR=`dirname $0`

source $BINDIR/../dev.env
export targetAlias="baci"
if [ -z "$2" ]; then
    echo "you must declare at least one index to remain in the alias"
fi

echo "this is not ready to use"
exit


#curl -i  'http://localhost:9200/_cat/aliases?v'
indexes=`curl   "http://localhost:${ES_PORT}/_cat/aliases?v" | grep -e '^gdc' | (read a b c d ; 
    while [ "$a" == "$targetAlias" ]; do echo "$b"; read a b c d; done )`


for s in  ] ; then
            curl -XPOST "http://localhost:${ES_PORT}/_aliases" -d '
{
    "actions" : [
        { "remove" : { "index" : "'$ES_Index'", "alias" : "gdc" } },
    ]
}'




fi


