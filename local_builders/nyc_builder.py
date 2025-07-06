#!/usr/bin/python

import contextlib
import simplejson as json
import logging
import os
import sys
import psycopg2
import pycurl
import cStringIO
import time
import re
#from data.common.tools import add_index,  db_roles

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', level=logging.DEBUG)

## elasticsearch uses lowercased names
# set the index name to build
# we use an alias to connect the index to the webserver
IDXNAME = os.environ.get('ES_Index','geony_qta')
OutputDir="./data/"

RUNLIVE = False
BATCH =  cStringIO.StringIO()
BATCH_PRE = cStringIO.StringIO()

# this is the operational port for ElasticSearch
PORT=int(os.environ.get('ES_PORT', 9200))
# this is the port to use when loading data to the geocoder ElasticSearch Server
LOAD_PORT=int(os.environ.get('ES_LOAD_PORT', 9200))

DB_USER = os.environ.get('CREATE_DB_USER')
DB_PASS = os.environ.get('CREATE_DB_PASS')
DB_SCHEMA = os.environ.get('CREATE_DB_SCHEMA')

DB_NAME = os.environ.get('DB_NAME')
DB_INSTANCE = os.environ.get('DB_INSTANCE')
DBHOST = os.environ.get('DBHOST')
DB_PORT = os.environ.get('DB_PORT')

if not DB_USER:
    sys.stderr.write('User not found: please set environment variable CREATE_DB_USER\n')
    sys.exit(-1)

if not DB_PASS:
    sys.stderr.write('Password not found: please set environment variable CREATE_DB_PASS\n')
    sys.exit(-1)


if not DBHOST:
    DBHOST = 'localhost'

if not DB_PORT:
    DB_PORT = '5432'

if DB_NAME:
    logger.info("using DB_NAME from environment vars")

if DB_SCHEMA.index('_') > 1:
    TEMP_SCHEMA = "temp_" + DB_SCHEMA.split('_')[1]
else:
    TEMP_SCHEMA = "temp"

#Set DB connection info
logger.info('writing to database "%s" in instance "%s"', DB_NAME, DB_INSTANCE)
DB_CONNECTION_STRING = 'host=%s dbname=%s user=%s password=%s port=%s' % (DBHOST, DB_NAME, DB_USER, DB_PASS, DB_PORT)

# print the connection string we will use to connect
logger.info('''Connecting to database\n ->%s''' % (DB_CONNECTION_STRING))

#curl -XPOST 'http://localhost:9200/_aliases' -d '
#{
#    "actions" : [
#        { "add" : { "index" : "test1", "alias" : "alias1" } },
#        { "add" : { "index" : "test2", "alias" : "alias1" } }
#    ]
#}'


#curl -XPOST 'http://localhost:9200/_aliases' -d '
#{
#    "actions" : [
#        { "remove" : { "index" : "test1", "alias" : "alias1" } },
#        { "add" : { "index" : "test1", "alias" : "alias2" } }
#    ]
#}'

@contextlib.contextmanager
def db_cursor(encoding=None):
    ''' Connect to psycoPg2 synchronously '''
    with psycopg2.connect(DB_CONNECTION_STRING, async=0) as conn:
        # Don't even think about running this stuff without turning on autocommit. You'll be waiting a loooooong time.
        conn.set_session(autocommit=True)
        if encoding:
            conn.set_client_encoding(encoding)
        with conn.cursor() as cursor:
            ''' don't wait on the WAL commit before moving on '''
            '''  this is a setting for performance, not for data security '''
            cursor.execute('set synchronous_commit=off')
            yield cursor

def send_command_live(cmd,  idx,  typ):
    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:%d/%s/%s' % (PORT, idx,  typ)

    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    if (cmd != None):
        c.setopt(pycurl.CUSTOMREQUEST, cmd)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
        pass
    except:
        e = sys.exc_info()[0]
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()

    pass

def send_command_batch(cmd,  idx,  typ):
    BATCH_PRE.write("""
curl -X%s 'http://localhost:'${ES_LOAD_PORT}'/%s/%s'\n
""" % (cmd, idx, typ))
    pass

def send_command(cmd,  idx,  typ):
    if (RUNLIVE == True):
        send_command_live(cmd,  idx,  typ)
    else:
        send_command_batch(cmd,  idx,  typ)

    pass

def send_action_live(idx,  typ,  data):
    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:%d/%s/%s' % (PORT, idx, typ)
    data = json.dumps(data)
    logger.debug(data)

    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, data)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
        pass
    except:
        e = sys.exc_info()[0]
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()
    pass

def send_action_batch(idx,  typ,  data):
    BATCH_PRE.write("""
curl -XPOST 'http://localhost:'${ES_LOAD_PORT}'/%s/%s' -d '%s'\n
""" % (idx, typ,  json.dumps(data) ))
    pass

def send_action(idx, typ, data):
    if (RUNLIVE == True):
        send_action_live
    else:
        send_action_batch(idx, typ, data)
    pass

def drop_index(idx,  index):

    if index == None or index == '':
        return "Failed"

    send_command("DELETE", idx,  index)
    pass

def send_mapping(address,  typemap):

    send_action(IDXNAME, typemap,  address)
    pass

def send_address_live(address, indtyp):

    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:%d/%s/%s/' % (PORT, IDXNAME,  indtyp) + str(address["id"])
    data = json.dumps(address)
    logger.debug(address)

    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, data)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
        pass
    except:
        e = c.errstr()
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()
    pass

def send_address_batch(address,  indtyp):
    activity = {"index": { "_index": IDXNAME,  "_type": indtyp, "_id": address['id']}}
    BATCH.write(json.dumps(activity) + "\n");
    BATCH.write(json.dumps(address) + "\n")
    pass

def send_address(address,  indtyp):
    if RUNLIVE == True:
        send_address_live(address, indtyp)
    else:
        send_address_batch(address,  indtyp)

    pass

def set_address_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "address_number": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" },
                "zipcode": { "type": "string" },
                "neighborhood": { "type": "string" },
                "addr_use": { "type": "string" },
                "name_type": {"type": "string" }
                }
            }
        }
    }
    ''' % (mapname)
    mapping = json.loads(map_str)

    send_mapping(mapping,  mapname)
    return mapping

def set_landmark_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" },
                "zipcode": { "type": "string" },
                "addr_use": { "type": "string" },
                "name_type": {"type": "string" }
                }
            }
        }
    } ''' % (mapname)

    mapping = json.loads(map_str)

    send_mapping(mapping,  mapname)
    return mapping

def set_neighborhood_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_submarket_C_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_submarket_R_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_postalcode_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_quadrant_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_market_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def number_cardinal(address):
    # convert from first to 1st and visa versa
    address = re.sub(r' FIRST ',  ' 1ST ',  address, flags=re.I)
    address = re.sub(r' SECOND ',  ' 2ND ',  address, flags=re.I)
    address = re.sub(r' THIRD ',  ' 3RD ',  address, flags=re.I)
    address = re.sub(r' FOURTH ',  ' 4TH ',  address, flags=re.I)
    address = re.sub(r' FIFTH ',  ' 5TH ',  address, flags=re.I)
    address = re.sub(r' SIXTH ',  ' 6TH ',  address, flags=re.I)
    address = re.sub(r' SEVENTH ',  ' 7TH ',  address, flags=re.I)
    address = re.sub(r' EIGHTH ',  ' 8TH ',  address, flags=re.I)
    address = re.sub(r' NINTH ',  ' 9TH ',  address, flags=re.I)
    address = re.sub(r' TENTH ',  ' 10TH ',  address, flags=re.I)
    address = re.sub(r' ELEVENTH ',  ' 11TH ',  address, flags=re.I)
    address = re.sub(r' TWELFTH ',  ' 12TH ',  address, flags=re.I)
    address = re.sub(r' THIRTEENTH ',  ' 13TH ',  address, flags=re.I)
    address = re.sub(r' FOURTEENTH ',  ' 14TH ',  address, flags=re.I)
    address = re.sub(r' FIFTEENTH ',  ' 15TH ',  address, flags=re.I)
    address = re.sub(r' SIXTEENTH ',  ' 16TH ',  address, flags=re.I)
    address = re.sub(r' SEVENEENTH ',  ' 17TH ',  address, flags=re.I)
    address = re.sub(r' EIGTHTEENTH ',  ' 18TH ',  address, flags=re.I)
    address = re.sub(r' NINTEENTH ',  ' 19TH ',  address, flags=re.I)
    address = re.sub(r' TWENTIETH ',  ' 20TH ',  address, flags=re.I)
    address = re.sub(r' TWENTY FIRST ',  ' 21ST ',  address, flags=re.I)
    address = re.sub(r' TWENTY FOURTH ',  ' 24TH ',  address, flags=re.I)

    return address

def ordinal(numstr):
    num = int(numstr)
    SUFFIXES = {1: 'st', 2: 'nd', 3: 'rd'}
    if num > 10  and num < 14:
        suffix = 'th'
    else:
        suffix = SUFFIXES.get(num % 10, 'th')
    return numstr + suffix

def naked_cardinal(address):
    # convert from 1 to 1st
    newaddress = address
    
    # splits the address into space delimited tokens and review the second and third ones
    parts = address.split(' ')
    if len(parts) > 1 and parts[1].isdigit():
        parts[1] = ordinal(parts[1])
    elif len(parts) > 2 and parts[2].isdigit():
        parts[2] = ordinal(parts[2])

    newaddress = ' '.join(parts)
    return newaddress

def cardinal_number(address):
    # convert from first to 1st and visa versa
    address = re.sub(r' 1ST ', ' FIRST ', address, flags=re.I)
    address = re.sub(r' 2ND ',  ' SECOND ',  address, flags=re.I)
    address = re.sub(r' 3RD ',  ' THIRD ',  address, flags=re.I)
    address = re.sub(r' 4TH ',  ' FOURTH ',  address, flags=re.I)
    address = re.sub(r' 5TH ',  ' FIFTH ',  address, flags=re.I)
    address = re.sub(r' 6TH ',  ' SIXTH ',  address, flags=re.I)
    address = re.sub(r' 7TH ',  ' SEVENTH ',  address, flags=re.I)
    address = re.sub(r' 8TH ',  ' EIGHTH ',  address, flags=re.I)
    address = re.sub(r' 9TH ',  ' NINTH ',  address, flags=re.I)
    address = re.sub(r' 10TH ', ' TENTH ',  address, flags=re.I)
    address = re.sub(r' 11TH ', ' ELEVENTH ',  address, flags=re.I)
    address = re.sub(r' 12TH ', ' TWELFTH ',  address, flags=re.I)
    address = re.sub(r' 13TH ', ' THIRTEENTH ',  address, flags=re.I)
    address = re.sub(r' 14TH ', ' FOURTEENTH ',  address, flags=re.I)
    address = re.sub(r' 15TH ', ' FIFTEENTH ',  address, flags=re.I)
    address = re.sub(r' 16TH ',  ' SIXTEENTH ',  address, flags=re.I)
    address = re.sub(r' 17TH ',  ' SEVENEENTH ',  address, flags=re.I)
    address = re.sub(r' 18TH ',  ' EIGTHTEENTH ',  address, flags=re.I)
    address = re.sub(r' 19TH ',  ' NINTEENTH ',  address, flags=re.I)
    address = re.sub(r' 20TH ',  ' TWENTIETH ',  address, flags=re.I)
    address = re.sub(r' 24TH ',  ' TWENTY FOURTH ',  address, flags=re.I)

    return address

def strip_type(address):

    if address == None:
        return ''

    # these words are all occur in more than 5% of the addresses for the jurisdiction
    address = re.sub(r' STREET',  ' ',  address)
    address = re.sub(r' COURT',  ' ',  address)
    address = re.sub(r' CIRCLE',  ' ',  address)

    address = re.sub(r' AVENUE',  ' ',  address)
    address = re.sub(r' ROAD',  ' ',  address)
    address = re.sub(r' DRIVE',  ' ',  address)
    address = re.sub(r' PLACE',  ' ',  address)
    address = re.sub(r' PLAZA',  ' ',  address)
    address = re.sub(r' TERRACE',  ' ',  address)
    address = re.sub(r' TRAIL',  ' ',  address)
    address = re.sub(r' GREEN',  ' ',  address)
    address = re.sub(r' WAY',  ' ',  address)
    address = re.sub(r' WALK',  ' ',  address)
    address = re.sub(r' ALLEY',  ' ',  address)
    address = re.sub(r' BOULEVARD',  ' ',  address)
    address = re.sub(r' CRESCENT',  ' ',  address)
    address = re.sub(r' MEWS',  ' ',  address)
    address = re.sub(r' LANE',  ' ',  address)
    address = re.sub(r' PARKWAY',  ' ',  address)
    address = re.sub(r' PARK',  ' ',  address)
    address = re.sub(r' EXPRESSWAY',  ' ',  address)
    address = re.sub(r' ROW',  ' ',  address)
    address = re.sub(r' FREEWAY',  ' ',  address)
    address = re.sub(r' BRIDGE',  ' ',  address)
    address = re.sub(r' SQUARE',  ' ',  address)

    return address

def expand_abbr(address):

    # un abbreviate things before indexing, some will get re-abbreviated later
    address = re.sub(r' ST ', ' STREET ', address)
    address = re.sub(r' CT ', ' COURT ', address)
    address = re.sub(r' CIR ', ' CIRCLE ', address)
    address = re.sub(r' AVE ', ' AVENUE ', address)
    address = re.sub(r' RD ', ' ROAD ', address)
    address = re.sub(r' SQ ',' SQUARE ', address)
    address = re.sub(r' QTR ', ' QUARTER ', address)

    return address

def abbr_Type(address):

    # this step  creates a version of the address to support index matching

    # abbreviate address street types with the USPS preferred abbreviations
    address = re.sub(r' STREET',  ' ST',  address)
    address = re.sub(r' COURT',  ' CT',  address)
    address = re.sub(r' CIRCLE',  ' CIR CR',  address)
    address = re.sub(r' AVENUE',  ' AVE AV',  address)
    address = re.sub(r' ROAD',  ' RD',  address)
    address = re.sub(r' DRIVE',  ' DR',  address)
    address = re.sub(r' PLACE',  ' PL PLC',  address)
    address = re.sub(r' PLAZA',  ' PLZ PL',  address)
    address = re.sub(r' TERRACE',  ' TERR TR',  address)
    address = re.sub(r' TRAIL',  ' TRL TR',  address)
    address = re.sub(r' GREEN',  ' GRN GR',  address)
    address = re.sub(r' WAY',  ' WY',  address)
    address = re.sub(r' WALK',  ' WLK',  address)
    address = re.sub(r' ALLEY',  ' ALY',  address)
    address = re.sub(r' BOULEVARD',  ' BLVD BL BLV',  address)
    address = re.sub(r' CRESCENT',  ' CRES',  address)
    address = re.sub(r' MEWS',  ' MWS',  address)
    address = re.sub(r' LANE',  ' LA LN',  address)
    address = re.sub(r' PARKWAY',  ' PKWY',  address)
    address = re.sub(r' PARK',  ' PK',  address)
    address = re.sub(r' EXPRESSWAY',  ' EXPY EY',  address)
    address = re.sub(r' ROW',  ' RW',  address)
    address = re.sub(r' FREEWAY',  ' FWY FY',  address)
    address = re.sub(r' BRIDGE',  ' BR',  address)
    address = re.sub(r' SQUARE',  ' SQ',  address)
    address = re.sub(r' QUARTER',  ' QTR',  address)

    return address

def abbr_lead(address):

    address = re.sub(r'^ST.',  'SAINT',  address)
    address = re.sub(r'^ST ',  'SAINT',  address)

    return address

def super_core_address(address):
    # this is an attempt to remove all words with a greater frequency than 15%
    address = strip_type(address)

    # remove a tailing quadrant ot directional
    address = re.sub(r' (NE|NW|SE|SW)$',  ' ',  address)
    address = re.sub(r'  ',  ' ',  address)

    return address

def strip_grammar(address):
    if address == None:
        return ''

    address = re.sub(r'&',  ' and ',  address)
    address = re.sub(r'/',  ' and ',  address)
    
    address = re.sub(r'-',  ' and ',  address)
    address = re.sub(r"'",  '',  address)
    address = re.sub(r"\.",  '',  address)

    return address

def pad_grammar(address):

    address = re.sub(r'&',  ' & ',  address)
    address = re.sub(r'/',  ' / ',  address)

    address = re.sub(r'-',  ' - ',  address)
    address = re.sub(r"'",  '',  address)
    address = re.sub(r"\.",  ' ',  address)

    return address

def core_address(address):
    # strip leading  Directionality
    # IGNORE FOR NYC at this time
    #address = re.sub(r'^NORTH ',  '',  address)

    # strip tailing Quadrant data
    if address == None:
        return ''

    address = re.sub(r' (NE|NW|SE|SW)$',  ' ',  address)

    #convert Streets to Types
    address = abbr_Type(address)

    address = strip_grammar(address)
    address = abbr_lead(address)

    return address

def alt_addresses(address):

    alts = [address]

    address = strip_grammar(address)
    address = abbr_lead(address)

    # custom NYC rules go here
    new_address = re.sub(r' EYE ',  ' I ',  address)
    if (new_address != address):
        alts.append(new_address)
    else:
        # only convert one way for I <=> EYE
        new_address = re.sub(r' I ',  ' EYE ',  address)
        if (new_address != address):
            alts.append(new_address)

    new_address = re.sub(r' MASSACHUSETTS ',  ' MASS ',  address)
    if (new_address != address):
        alts.append(new_address)
    new_address = re.sub(r' CONNECTICUT ',  ' CONN ',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r' PENNSYLVANIA ',  ' PENN ',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r' MASSACHUSETTS ',  ' MASS ',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r' NEW YORK ',  ' NY ',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r'WHITE HOUSE',  'WHITEHOUSE',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r'1707 7TH STREET NW',  'PARCEL 42',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r'PENN QTR',  'PENN QUARTER',  address)
    if (new_address != address):
        alts.append(new_address)

    new_address = re.sub(r'CENTRAL BUSINESS DISTRICT',  'CBD',  address)
    if (new_address != address):
        alts.append(new_address)

    # attempt to convert 11th => eleventh
    new_address = number_cardinal(address)
    if (new_address != address):
        alts.append(new_address)

    # attempt to convert 11 => 11th
    new_address = naked_cardinal(address)
    if (new_address != address):
        alts.append(new_address)

    new_address = cardinal_number(address)
    if (new_address != address):
        alts.append(new_address)

    return alts

def alt_address(address, force):
    # custom NYC rules go here
    new_address = re.sub(r' EYE ',  ' I ',  address)
    if ( new_address == address):
        new_address = re.sub(r' I ',  ' EYE ',  address)
    if ( new_address == address):
        new_address = re.sub(r' MASSACHUSETTS ',  ' MASS ',  address)
    if ( new_address == address):
        new_address = re.sub(r' CONNECTICUT ',  ' CONN ',  address)
    if ( new_address == address):
        new_address = re.sub(r' PENNSYLVANIA ',  ' PENN ',  address)
    if ( new_address == address):
        new_address = re.sub(r' MASSACHEUSETTS ',  ' MASS ',  address)
    if ( new_address == address):
        new_address = re.sub(r' NEW YORK ',  ' NY ',  address)

    # attempt to convert 11th => eleventh
    if (new_address == address):
        new_address = number_cardinal(new_address)

    # unless we force it - only return a changed address
    if (force or new_address != address):
        return new_address
    else:
        return ""

def alt_core_address(address):
    # strip leading  Directionality
    #address = re.sub(r'^NORTH ',  '',  address)
    address = re.sub(r' EYE ',  ' I ',  address)

    address = strip_grammar(address)

    # strip tailing Quadrant data
    address = re.sub(r' (NE|NW|SE|SW)$',  '',  address)

    return address

def submit_address(data,  typeName):
    # if the address does not have an extent then ignore it
    # this protects against building geocoder indexes for things that lack a geometry
    if data[9] == None:
        return

    # expand the address to all alternatives (non - alias)
    alts = alt_addresses(data[1])
    alt_ctr = 0
    for address_entry in alts:
        address = {"id": data[0].strip(),
            "proper_address": data[14],
            "complete_address": address_entry,
            "core_address": core_address(address_entry),
            "super_core_address": super_core_address(address_entry),
            "alt_core_address": alt_address(super_core_address(address_entry), True),
            "city": data[2],
            "state": data[3],
            "zipcode": data[4],
            "local_id": data[6],
            "local_desc": data[7],
            "addr_use": data[8],
            "extentBBOX": json.loads(data[9]),
            "location": json.loads(data[10]),
            "neighborhood": data[11],
            "camera": json.loads(data[12]),
            "front_vect": json.loads(data[13])
        }
        #if data[5] > '':
        #   address['address_number'] = int(data[5])
        if alt_ctr > 0:
            address['id'] = data[0].strip() + '_%s' % (alt_ctr)
        send_address(address,  typeName)

        alt_ctr += 1

def index_landmarks(prm):
    landmark_poly_tbl = "temp_us3651000" + ".nris_landmark_polygon"
    landmark_point_tbl = "temp_us3651000" + ".nris_landmark_point"
    place_poly_tbl = "temp_us3651000" + ".nris_historic_place_polygon"
    place_point_tbl = "temp_us3651000" + ".nris_historic_place_point"

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_landmark_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT a.nris_refnum::TEXT,
            a.resname as name,
            a.city,
            a.state,
            '',
            'NRIS_Landmark'::TEXT as domain,
            0 as normative,
            a.bnd_type,
            a.restype,
            '',
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            '{}' as location,
            a.address as proper_address,
            '{}' as camera
            FROM
                %s a
            WHERE city = 'New York'
            UNION ALL
            SELECT a.nris_refnum::TEXT,
            a.resname as name,
            a.city,
            a.state,
            '',
            'NRIS_Landmark'::TEXT as domain,
            0 as normative,
            a.bnd_type,
            a.restype,
            '',
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(a.geometry) as location,
            a.address as proper_address,
            '{}'
            FROM
                %s a
            WHERE city = 'New York'
            UNION ALL
            SELECT a.nris_refnum::TEXT,
            a.resname as name,
            a.city,
            a.state,
            '',
            'NRIS_Landmark'::TEXT as domain,
            0 as normative,
            a.bnd_type,
            a.restype,
            '',
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            '{}' as location,
            a.address as proper_address,
            '{}' as camera
            FROM
                %s a
            WHERE city = 'New York'
            UNION ALL
            SELECT a.nris_refnum::TEXT,
            a.resname as name,
            a.city,
            a.state,
            '',
            'NRIS_Landmark'::TEXT as domain,
            0 as normative,
            a.bnd_type,
            a.restype,
            '',
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(a.geometry) as location,
            a.address as proper_address,
            '{}'
            FROM
                %s a
            WHERE city = 'New York'
                """ %(landmark_poly_tbl,  landmark_point_tbl, place_poly_tbl, place_point_tbl))
        result = cursor.fetchall()

        for data in result:
            alts = alt_addresses(data[1].upper())
            alt_ctr = 0
            for address_entry in alts:
                # place padding around ,/-&. to make the combined bit indexable
                address_entry = pad_grammar(address_entry)

                address = {"id": 'lm_' + data[0].strip(),
                    "proper_address": address_entry,
                    "complete_address": address_entry,
                    "core_address": core_address(address_entry),
                    "super_core_address": super_core_address(address_entry),
                    "alt_core_address": alt_address(super_core_address(address_entry), True),
                    "city": data[2],
                    "state": data[3],
                    "zipcode": data[4],
                    "domain": data[5],
                    "normative": int(data[6]),
                    "status": data[7],
                    "aliastype": data[8],
                    "local_id": data[9],
                    "extentBBOX": json.loads(data[10]),
                    "location": json.loads(data[10]),
                    "front_vect": json.loads(data[13]),
                    "camera": {}
                }
                if alt_ctr > 0:
                    address['id'] = "lm_" + data[0].strip() + '_%s' % (alt_ctr)
                if (data[12] > ''):
                    address['proper_address'] = data[12] + ', ' + address['proper_address']
                if ( "front_vect" in address and not ("coordinates" in  address['front_vect'])):
                    del address['front_vect'];

                send_address(address,  typeName)
                if (cntr % 5000) == 0:
                    time.sleep(0)
                cntr += 1

                alt_ctr += 1

    pass

def add_index(idxfield, table, using=None):
    if len(table.split('.')) > 1:
        schema = table.split('.')[0]
        table = table.split('.')[1]

    assert table > ''
    assert schema > ''

    idx = (table + "_" + idxfield + "_ind").lower()

    with db_cursor() as cursor:
        cursor.execute("SELECT count(*) as cnt FROM pg_indexes WHERE schemaname = '" + schema + "' and indexname = '" + idx + "'")
        result = cursor.fetchone()
        if result[0] != 1:
            logger.debug('''create index on %s . %s ( %s ) '''%(schema, table, idxfield))
            # DO NOT Blindly drop and recreate indexes
            # INDEX may have modifiers that will get lost
            # cursor.execute('DROP INDEX IF EXISTS ' + schema + '.' + idx)
            if using:
                cursor.execute('CREATE INDEX ' + idx + ' ON ' + schema + '.' + table + ' USING ' + using + '("' + idxfield + '")')
            else:
                cursor.execute('CREATE INDEX ' + idx + ' ON ' + schema + '.' + table + ' ("' + idxfield + '")')
        cursor.execute('SELECT now()')

def index_addresses(prm):

    addr_point_tbl = TEMP_SCHEMA + ".address_points"
    parcel_tbl = DB_SCHEMA + ".parcels"
    property_tbl = DB_SCHEMA + ".properties"

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_address_mapping(typeName)

    add_index('geometry', addr_point_tbl, 'GIST')
    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""DROP TABLE IF EXISTS address_list_temp""")
        cursor.execute("""CREATE TEMP TABLE address_list_temp AS (SELECT
            to_char(a.addressid, 'mar_000000000000000D')::TEXT as indexable_id,
            a.hs_num || ' ' || a.street_nm as fulladdress,
            'New York' as city,
            'NY' as state,
            a.zipcode::TEXT,
            a.hs_num::TEXT as addrnum,
            p.parcel_id,
            (p.local_ids[1])::TEXT as local_id,
            'BBL' as local_desc,
            p.current_use_category as addr_use,
            st_asgeojson(st_expand(a.geometry, 0.0000001)) as extent,
            st_asgeojson(a.geometry) as location,
            (CASE
                WHEN ((p.zones)->'jurisdiction') = 'MN' THEN 'Manhattan'
                WHEN ((p.zones)->'jurisdiction') = 'BX' THEN 'Bronx'
                WHEN ((p.zones)->'jurisdiction') = 'BK' THEN 'Brooklyn'
                WHEN ((p.zones)->'jurisdiction') = 'QN' THEN 'Queens'
                WHEN ((p.zones)->'jurisdiction') = 'SI' THEN 'Staten Island'
                ELSE ''
                END) ::TEXT as neighborhood,
            '{}'::TEXT as camera,
            '{}'::TEXT as front_vect,
            a.hs_num || ' ' || a.street_nm  as proper_address
            FROM %s a LEFT OUTER JOIN
                %s p ON (st_intersects(p.geometry, a.geometry))
            WHERE a.geometry IS NOT NULL)""" % (addr_point_tbl, parcel_tbl))
        logger.debug('''  inserted %d from address_points''' %(cursor.rowcount))

        cursor.execute('''UPDATE address_list_temp a SET
                camera = (CASE WHEN (p.camera_vect)::TEXT > '{}' THEN p.camera_vect ELSE '{}' END)::TEXT,
                front_vect = (CASE WHEN (p.front_vect)::TEXT > '{}' THEN p.front_vect ELSE '{}' END)::TEXT,
                local_id = coalesce(a.local_id, p.local_id, '')
            FROM
                %s p
            WHERE
                p.parcel_id = a.parcel_id''' %(property_tbl))
        logger.debug('''  updated %d address_points from properties''' %(cursor.rowcount))

        cursor.execute("""CREATE INDEX address_list_temp__ind on address_list_temp(local_id)""")
        # add place descriptors from the OTR owner_point file
        # filter by those not already in the address file
        cursor.execute("""CREATE TEMP TABLE owner_point_temp as (
            SELECT p.local_id,
                trim(CASE
                    WHEN p.property_address like '% ST %' THEN replace (p.property_address, ' ST ', ' STREET ')
                    WHEN p.property_address like '% AV %' THEN replace (p.property_address, ' AV ', ' AVENUE ')
                    WHEN p.property_address like '% RD %' THEN replace (p.property_address, ' RD ', ' ROAD ')
                    WHEN p.property_address like '% CT %' THEN replace (p.property_address, ' CT ', ' COURT ')
                    WHEN p.property_address like '% CIR %' THEN replace (p.property_address, ' CIR ', ' CIRCLE ')
                    WHEN p.property_address like '% BLVD %' THEN replace (p.property_address, ' BLVD ', ' BOULEVARD ')
                    WHEN p.property_address like '% TR %' THEN replace (p.property_address, ' TR ', ' TERRACE ')
                    WHEN p.property_address like '% PLZ %' THEN replace (p.property_address, ' PLZ ', ' PLAZA ')
                    WHEN p.property_address like '% PL %' THEN replace (p.property_address, ' PL ', ' PLACE ')
                    WHEN p.property_address like '% PL %' THEN replace (p.property_address, ' SQ ', ' SQUARE ')
                    WHEN p.property_address like '% PL %' THEN replace (p.property_address, 'WLK ', ' WALK ')
                    WHEN p.property_address like '% PL %' THEN replace (p.property_address, ' PLZ ', ' PLAZA ')
                    ELSE p.property_address
                    END) as property_address,
                p.geometry, p.property_id, p.property_city, p.property_state, p.property_zip, p.front_vect, p.property_quadrant
                FROM (SELECT p.local_id, p.property_address || ' ' as property_address, p.geometry, p.property_id,
                        p.property_city, p.property_state, p.property_zip, p.front_vect, p.core->'quadrant' as property_quadrant
                    FROM """ + """%s p LEFT OUTER JOIN address_list_temp a ON (p.local_id = a.local_id)
                        WHERE
                            a.local_id is NULL and
                            trim(property_address) > '' and split_part(property_address,' ',1) !~ '[A-Z]+' ) as p
                WHERE p.geometry IS NOT NULL
            )""" %(property_tbl))
        logger.debug('''  inserted %d new address_points from addresses in properties''' %(cursor.rowcount))

        # strip apt and suite and unit info from addresses
        cursor.execute("""UPDATE owner_point_temp SET property_address = regexp_replace(property_address, ' Unit: .*', '')
            WHERE  (property_address ~* ' UNIT: ')
            """)
        cursor.execute("""UPDATE owner_point_temp SET property_address = regexp_replace(property_address, ' (NE|NW|SE|SW) .*', '')
            WHERE  (property_address ~ ' (NE|NW|SE|SW) ')
            """)
        cursor.execute("""UPDATE owner_point_temp SET property_address = property_address || ' ' || property_quadrant
            WHERE not (property_address ~ ' (NE|NW|SE|SW)$' or property_address ~ ' (NE|NW|SE|SW) ')
            """)
        cursor.execute("""DELETE FROM owner_point_temp  o
            WHERE exists (SELECT 1 FROM %s a WHERE o.property_address = (a.hs_num || ' ' || a.street_nm))
            """ %(addr_point_tbl))

        cursor.execute("""CREATE TEMP TABLE address_list_words as (
            SELECT local_id, (regexp_matches(fulladdress, '[^ ]* [^ ]*'))[1] as words FROM address_list_temp)
            """)
        cursor.execute("""CREATE INDEX address_list_words__ind on address_list_words(words)""")
        cursor.execute("""CREATE TEMP TABLE del_op as (
            SELECT p.property_id, p.local_id, (regexp_matches(property_address, '[^ ]* [^ ]*'))[1] as words
            FROM owner_point_temp p
        )""")
        cursor.execute("""DELETE FROM del_op d WHERE EXISTS (SELECT 1 FROM address_list_words w WHERE w.words = d.words )""")

        cursor.execute("""CREATE INDEX del_op__ind on del_op(property_id)""")
        cursor.execute("""DELETE FROM owner_point_temp o WHERE
             NOT EXISTS (SELECT 1 FROM del_op d WHERE d.property_id = o.property_id) """)

        cursor.execute("""INSERT INTO address_list_temp (indexable_id, fulladdress, city, state, zipcode, local_id, local_desc, addrnum,
            extent, location, front_vect, neighborhood, camera, proper_address)  (
            SELECT 'OTR_' || property_id::TEXT, property_address,
                'NEW YORK',
                'NY',
                '',
                local_id,
                'BBL',
                coalesce(ax[1], '') as addrnum,
                st_asgeojson(st_expand(geometry, 0.0000001)) as extent,
                st_asgeojson(geometry) as location,
                coalesce(front_vect,'{}')::TEXT as front_vect,
                ''::TEXT as neighborhood,
                '{}'::TEXT as camera,
                property_address
            FROM  owner_point_temp o,
                regexp_matches(property_address, '^[0-9]*') as ax
                )""")
        cursor.execute("""SELECT
                indexable_id,
                fulladdress,
                city,
                state,
                zipcode,
                addrnum::text,
                coalesce(local_id,''),
                local_desc,
                coalesce(addr_use,''),
                coalesce(extent,'{}'),
                location,
                neighborhood,
                coalesce(camera, '{}'),
                coalesce(front_vect,'{}'),
                proper_address
            FROM address_list_temp""")
        result = cursor.fetchall()
        for data in result:
            submit_address(data,  typeName)

            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1
    pass

def neighbodhood_pts(nbhd_tbl, typeDesc, typeName):
    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT 'NBHD:' || objectid::TEXT, a.name as name,
            'NEW YORK' as city,
            'NY' as state,
            'CREATE_NYZ'::TEXT as domain, 0 as normative,
            'G' as class,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(st_cleangeometry(a.geometry))) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT name, objectid, st_simplifypreservetopology(geometry,0.00001) as geometry
                    FROM %s t
                    WHERE st_npoints(geometry) > 3)  a""" %(nbhd_tbl))
        logger.debug('  collected %d names from %s' %(cursor.rowcount, nbhd_tbl))
        result = cursor.fetchall()
        for data in result:
            alts = alt_addresses(data[1].upper())
            for address_entry in alts:
                # place padding around ,/-&. to make the combined bit indexable
                address_entry = pad_grammar(address_entry)

                address = {"id": data[0].strip(),
                    "proper_address": data[1],
                    "complete_address": address_entry,
                    "core_address": core_address(address_entry),
                    "super_core_address": super_core_address(address_entry),
                    "alt_core_address": alt_address(super_core_address(address_entry), True),
                    "city": data[2],
                    "state": data[3],
                    "zipcode": "(" + typeDesc + ")",
                    "domain": data[4],
                    "normative": int(data[5]),
                    "class": data[6],
                    "extentBBOX": json.loads(data[7]),
                    "location": json.loads(data[8]),
                    "neighborhood": data[1],
                    "camera": {},
                    "geometry": json.loads(data[9])
                }
                send_address(address, typeName)
                if (cntr % 5000) == 0:
                    time.sleep(0)
                cntr += 1

def neighbodhood_polys(nbhd_tbl, typeDesc, typeName):
    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT 'NYNTA:' || objectid::TEXT, a.ntaname as name,
            'NEW YORK' as city,
            'NY' as state,
            'NYNTA'::TEXT as domain, 0 as normative,
            'G' as class,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(st_cleangeometry(a.geometry))) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT ntaname, objectid, st_simplifypreservetopology(geometry,0.00001) as geometry
                    FROM %s t
                    WHERE st_npoints(geometry) > 3)  a""" %(nbhd_tbl))
        logger.debug('  collected %d names from %s' %(cursor.rowcount, nbhd_tbl))

        result = cursor.fetchall()
        for data in result:
            alts = alt_addresses(data[1].upper())
            for address_entry in alts:
                # place padding around ,/-&. to make the combined bit indexable
                address_entry = pad_grammar(address_entry)

                address = {"id": data[0].strip(),
                    "proper_address": data[1],
                    "complete_address": address_entry,
                    "core_address": core_address(address_entry),
                    "super_core_address": super_core_address(address_entry),
                    "alt_core_address": alt_address(super_core_address(address_entry), True),
                    "city": data[2],
                    "state": data[3],
                    "zipcode": "(" + typeDesc + ")",
                    "domain": data[4],
                    "normative": int(data[5]),
                    "class": data[6],
                    "extentBBOX": json.loads(data[7]),
                    "location": json.loads(data[8]),
                    "neighborhood": data[1],
                    "camera": {},
                    "geometry": json.loads(data[9])
                }
                send_address(address, typeName)
                if (cntr % 5000) == 0:
                    time.sleep(0)
                cntr += 1

def index_neighborhoods(prm):

    nbhd_point_tbl = "temp_us3651000" + ".neighborhood_names"
    nbhd_tbl = "temp_us3651000" + ".nynta"

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_neighborhood_mapping(typeName)

    neighbodhood_pts(nbhd_point_tbl, typeDesc, typeName)
    neighbodhood_polys(nbhd_tbl, typeDesc, typeName)

    pass

def index_create_neighborhoods(prm):

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_neighborhood_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT 
                'NBHD:' || objectid::TEXT, 
                a.name as name,
                'NEW YORK' as city,
                'NY' as state,
                'NYC'::TEXT as domain, 
                0 as normative,
                'G' as class,
                st_asgeojson(st_expand(a.geometry, 0.001)) as extent,
                '{}',
                st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT name, objectid, st_simplifypreservetopology(geometry,0.00001) as geometry
                    FROM temp.create_nbhd
                    WHERE st_npoints(geometry) > 3)  a""")
        result = cursor.fetchall()
        for data in result:
            alts = alt_addresses(data[1].upper())
            for address_entry in alts:
                # place padding around ,/-&. to make the combined bit indexable
                address_entry = pad_grammar(address_entry)

                address = {"id": data[0].strip(),
                    "proper_address": data[1],
                    "complete_address": address_entry,
                    "core_address": core_address(address_entry),
                    "super_core_address": super_core_address(address_entry),
                    "alt_core_address": alt_address(super_core_address(address_entry), True),
                    "city": data[2],
                    "state": data[3],
                    "zipcode": "(" + typeDesc + ")",
                    "domain": data[4],
                    "normative": int(data[5]),
                    "class": data[6],
                    "extentBBOX": json.loads(data[7]),
                    "location": json.loads(data[8]),
                    "neighborhood": data[1],
                    "camera": {},
                    "geometry": json.loads(data[9])
                }
                send_address(address, typeName)
                if (cntr % 5000) == 0:
                    time.sleep(0)
                cntr += 1

    pass

def index_submarket_commercial(prm):

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME,typeName)
        set_submarket_C_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT objectid::TEXT, replace(a.name, '_', ' ') as name,
            'NEW YORK' as city,
            'NY' as state,
            'create.io'::TEXT as domain, 0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(st_cleangeometry(a.geometry))) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT submarket as name, id as objectid, st_simplifypreservetopology(st_cleangeometry(geometry),0.0001) as geometry
                    FROM temp.submarket_commercial_nbhd
                    WHERE st_npoints(geometry) > 3)  a""")
        result = cursor.fetchall()
        for data in result:
            # place padding around ,/-&. to make the combined bit indexable
            # expand some but not all abbreviations in the data
            address_entry = expand_abbr(pad_grammar(data[1].upper()))

            address = {"id": data[0].strip(),
                "proper_address": data[1],
                "complete_address": address_entry,
                "core_address": core_address(address_entry),
                "super_core_address": super_core_address(address_entry),
                "alt_core_address": alt_address(super_core_address(address_entry), True),
                "city": data[2],
                "state": data[3],
                "zipcode": "(" + typeDesc + ")",
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]),
                "location": json.loads(data[7]),
                "camera": {},
                "geometry": json.loads(data[8])
            }
            send_address(address,  typeName)
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def index_submarket_residential(prm):

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_submarket_R_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT objectid::TEXT, name,
            'NEW YORK' as city,
            'NY' as state,
            'create.io'::TEXT as domain, 0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT name, objectid, st_simplifypreservetopology(st_cleangeometry(geometry),0.00001) as geometry
                    FROM temp.submarket_residential_nbhd
                    WHERE st_npoints(geometry) > 3)  a""")
        result = cursor.fetchall()
        for data in result:
            # place padding around ,/-&. to make the combined bit indexable
            address_entry = data[1].upper()
            address_entry = pad_grammar(address_entry)

            address = {"id": data[0].strip(),
                "proper_address": data[1],
                "complete_address": address_entry,
                "core_address": core_address(address_entry),
                "super_core_address": super_core_address(address_entry),
                "alt_core_address": alt_address(super_core_address(address_entry), True),
                "city": data[2],
                "state": data[3],
                "zipcode": "(" + typeDesc + ")",
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]),
                "location": json.loads(data[7]),
                "camera": {},
                "geometry": json.loads(data[8])
            }
            send_address(address, typeName)
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def index_postalcode(prm):
    zip_tbl = 'temp_us3651000.census_face'

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_postalcode_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute('''CREATE TEMP TABLE zip_tmp AS ( SELECT
            statefp10 || countyfp10 || zcta5ce10 as geoid10,
            zcta5ce10, st_union(geometry) as geometry
            FROM
                %s a
            WHERE zcta5ce10 IS NULL
            GROUP BY 1,2
            )''' %(zip_tbl))
        cursor.execute("""SELECT 
                geoid10::TEXT as objectid, 
                a.name,
                'NEW YORK' as city,
                'NY' as state,
                'create.io'::TEXT as domain, 0 as normative,
                st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
                st_asgeojson(st_pointonsurface(a.geometry)) as location,
                st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT z.zcta5ce10 as name, z.geoid10, 
                        st_simplifypreservetopology(st_cleangeometry(z.geometry),0.00001) as geometry
                    FROM zip_tmp z
                    WHERE
                        z.zcta5ce10 IS NOT NULL
                    )  as a""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(),
                "proper_address": data[1],
                "complete_address": data[1],
                "core_address": core_address(data[1]),
                "super_core_address": super_core_address(data[1]),
                "alt_core_address": alt_address(super_core_address(data[1]), True),
                "city": data[2],
                "state": data[3],
                "zipcode": "(" + typeDesc + ")",
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]),
                "location": json.loads(data[7]),
                "camera": {},
                "geometry": json.loads(data[8])
            }
            send_address(address,  typeName)
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def index_market(prm):

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, typeName)
        set_market_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT objectid::TEXT, name,
            'NEW YORK' as city,
            'NY' as state,
            'create.io'::TEXT as domain, 0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT 'New York, NY' as name, objectid, st_simplifypreservetopology(st_cleangeometry(geometry),0.0001) as geometry FROM temp.dc_boundary)  a""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(),
                "proper_address": data[1],
                "complete_address": data[1],
                "core_address": core_address(data[1]),
                "super_core_address": super_core_address(data[1]),
                "alt_core_address": alt_address(super_core_address(data[1]), True),
                "city": data[2],
                "state": data[3],
                "zipcode": "(" + typeDesc + ")",
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]),
                "location": json.loads(data[7]),
                "camera": {},
                "geometry": json.loads(data[8])
            }
            send_address(address,  typeName)
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def index_borough(prm):
    boro_tbl = "temp_us3651000" + ".borough_boundaries"

    if 'type' in prm:
        typeName = prm['type']
        typeDesc = prm['descr']
    else:
        assert 1>2,  "the 'type' must be declared"

    logger.debug('''  Start ''' + typeName)
    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME,typeName)
        set_quadrant_mapping(typeName)

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT objectid::TEXT, 
            name,
            'NEW YORK' as city,
            'NY' as state,
            'NYC'::TEXT as domain,
            0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT boro_name as name, boro_code, boro_code as objectid, 
                    st_simplifypreservetopology(st_cleangeometry(geometry),0.0001) as geometry 
                FROM %s)  a""" %(boro_tbl))
        result = cursor.fetchall()
        for data in result:
            address_entry = data[1].upper()
            address = {"id": data[0].strip(),
                "proper_address": data[1],
                "complete_address": address_entry,
                "core_address": core_address(address_entry),
                "super_core_address": super_core_address(address_entry),
                "alt_core_address": alt_address(super_core_address(address_entry), True),
                "city": data[2],
                "state": data[3],
                "zipcode": "(" + typeDesc + ")",
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]),
                "location": json.loads(data[7]),
                "camera": {},
                "geometry": json.loads(data[8])
            }
            send_address(address,  typeName)
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def main_loop():

    if (not os.path.isdir(OutputDir) ):
        os.mkdir(OutputDir)

    index_addresses({"reset": True, "type": "address",  "descr": "Address"})
    index_landmarks({"reset": True, "type": "landmark",  "descr": "Landmarks"})

    index_neighborhoods({"reset": True, "type": "neighborhood",  "descr": "Neighborhood"})
    #index_create_neighborhoods({"reset": True, "type": "neighborhood",  "descr": "Neighborhood"})

    #index_submarket_commercial({"reset": True, "type": "SMC",  "descr": "Commercial Submarket"})
    #index_submarket_residential({"reset": True, "type": "SMR",  "descr": "Residential Submarket"})

    #index_market({"reset": True, "type": "market",  "descr": "Market"})
    index_postalcode({"reset": True, "type": "postalcode",  "descr": "ZIP Code"})
    index_borough({"reset": True, "type": "borough",  "descr": "Borough"})
    logger.debug('''finished''')

    if (RUNLIVE == False):
        BATCH_PRE.reset()
        with  open(OutputDir + "batch_pre.sh",  "wb+") as bfile:
            bfile.write("#ES_Index=%s\n"% (IDXNAME))
            bfile.write(BATCH_PRE.getvalue())
        BATCH_PRE.close()

        BATCH.reset()
        with  open( OutputDir + "batch.json",  "wb+") as bfile:
            bfile.write(BATCH.getvalue())
        BATCH.close()
    pass

if __name__ == "__main__":
    main_loop()
    pass
