#ES_Index=${idxName}

idxName="${idxName}"

curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/address"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/address" -d '{"mappings": {"address": {"properties": {"city": {"type": "string"}, "neighborhood": {"type": "string"}, "address_number": {"type": "integer"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "zipcode": {"type": "string"}, "name_type": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}, "addr_use": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/nbhd"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/nbhd" -d '{"mappings": {"nbhd": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/landmark"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/landmark" -d '{"mappings": {"landmark": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "zipcode": {"type": "string"}, "name_type": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}, "addr_use": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/SMC"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/SMC" -d '{"mappings": {"SMC": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/SMR"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/SMR" -d '{"mappings": {"SMR": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/market"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/market" -d '{"mappings": {"market": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/postalcode"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/postalcode" -d '{"mappings": {"postalcode": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE "http://localhost:${ES_LOAD_PORT}/${idxName}/quadrant"


curl -XPOST "http://localhost:${ES_LOAD_PORT}/${idxName}/quadrant" -d '{"mappings": {"quadrant": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'

