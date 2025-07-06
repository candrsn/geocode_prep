#!/bin/bash

if [ -z "$ES_Index" ]; then
    echo "usage:   batch_apply.sh"
    echo "    set the ENV var ES_Index before running this script"
    echo "    see dev.env for one way to do this"
    exit
fi

BINDIR=`dirname $0`

echo "Starting to apply ElasticSearch Index $ES_Index"
echo ""

# we may have unpackaged a dump into the data dir
if [ -d "data" ]; then
    cd data
else
    echo ""
fi

#scan for idxname
idxname=`head -n 1 batch_pre.sh | cut -d'=' -f2`

if [ "$ES_Index" != "$idxname" ]; then
    echo "Fatal Error"
    echo "batch index files were built for $idxname, but we are attempting to apply them to $ES_Index"
    exit
fi

bash ${BINDIR}/batch_pre.sh

if [ ! -r tmp ]; then
  mkdir tmp
else
  rm tmp/bq??
fi

split -l 10000 batch.json tmp/bq

# using the split files as smaller batch commits
# apply this update to the ElasticSerach server
# to apply to a remote host
# use:
#   ssh -C -L9200:localhost:9200 ubuntu@<host> 
# the -c flag enables compression

for i in tmp/bq??; do
    curl -XPOST localhost:${ES_LOAD_PORT}/_bulk --data-binary  @${i}
    echo ""
done 


# The actions below are atomic, in that both occur in the same instant
curl -XPOST localhost:${ES_LOAD_PORT}/_aliases -d '
{
    "actions" : [
        { "remove" : { "index" : "'$OLD_ES_Index'", "alias" :  "'$ES_MKT_Index'" } },
        { "add" : { "index" : "'$ES_Index'", "alias" :  "'$ES_MKT_Index'" } }
    ]
}'

curl -XGET localhost:${ES_LOAD_PORT}/_aliases | json_pp


