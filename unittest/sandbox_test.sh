#!/bin/bash

geoHost="http://127.0.0.1:9200"
# Geocoder sandbox tests

curl -XPOST -d '{
   "query": {
      "multi_match": {
         "type": "most_fields",
         "query": "1111 19 NW",
         "fields": [
            "complete_address", "core_address", "addr_number"
            
         ]
      }
   }
}' ${geoHost}/geodc/address/_search


curl -XPOST -d '{
   "query": {
      "multi_match": {
         "type": "most_fields",
         "query": "WHITE HOUSE",
         "fields": [
            "complete_address", "core_address", "addr_number"
            
         ]
      }
   }
}' ${geoHost}/geodc/address/_search


curl -XPOST -d '{
   "query": {
      "multi_match": {
         "type": "most_fields",
         "query": "WHITEHOUSE",
         "fields": [
            "complete_address", "core_address", "addr_number"
            
         ]
      }
   }
}' ${geoHost}/geodc/address/_search
