#!/bin/bash

curl -XPUT 'http://localhost:9200/_snapshot/my_backup' -d '{
    "type": "fs",
    "settings": {
        "location": "/tmp/my_backup",
        "compress": true
    }
}'


curl -XPUT 'http://localhost:9200/_snapshot/my_backup/snapshot_1?wait_for_completion=true'

curl -XPOST 'http://localhost:9200/_snapshot/my_backup/snapshot_1/_restore'

curl -XGET 'http://localhost:9200/_snapshot/_status'

curl -XDELETE 'http://localhost:9200/_snapshot/my_backup/snapshot_1'



