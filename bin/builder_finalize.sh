#!/bin/bash

# package the build up and save it to AWS
set -e

usage() {
  echo "builder_finalize.sh <pfx>"
}

pfx="$1"
if [ -z "$pfx" ];then
  usage
  exit
fi

dt=`date +%Y%m%d`
file="g${pfx}_${dt}.zip"

# if either of data/batch.json or data/batch_pre.sh are missing, we need to re-run builder.py
if [ ! -r data/batch_pre.sh -o -r data/batch_pre.json ]; then
    echo "please rerun builder.py to create data/batch.json and data/batch_pre.sh"
    exit
fi

if [ -r data/tmp ]; then
    echo "removing working data before packaging the build"
    rm -rf data/tmp
fi
# the file batch_pre.sh is created by the builder.py process, so do not delete it here
if [ -r data/batch_apply.sh ]; then
    echo "removing scripts from the data dir before packaging the build"
    rm -f data/batch_apply.sh
fi


# do not save directory paths and compress the file as much as possible
cp -p bin/batch_apply.sh data
zip -j -9 dumps/${file}  data/* 

# save the build into AWS S3
# you may need to set the AWS_KEY and AWS_SECRET first
echo "uploading build to AWS S3"
aws s3 cp dumps/${file} s3://t-rex-dumps/geocoder/${file}

echo "now unpackage the dump into the data dir "
echo "  unzip -d data dumps/$file"
echo "then run data/batch_apply.sh to apply the data"

