#!/bin/bash

# review the lexical analyzer

curl localhost:${ES_PORT}/geodc_b/_analyze?complete_address -d "5864 C St NW"


