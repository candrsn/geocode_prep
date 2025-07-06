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
IDXNAME = os.environ.get('ES_Index','geodc_qt')
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

TEMP_SCHEMA = "temp_" + DB_SCHEMA.split('_')[1]

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
    address = re.sub(r' FIRST ',  ' 1ST ',  address)
    address = re.sub(r' SECOND ',  ' 2ND ',  address)
    address = re.sub(r' THIRD ',  ' 3RD ',  address)
    address = re.sub(r' FOURTH ',  ' 4TH ',  address)
    address = re.sub(r' FIFTH ',  ' 5TH ',  address)
    address = re.sub(r' SIXTH ',  ' 6TH ',  address)
    address = re.sub(r' SEVENTH ',  ' 7TH ',  address)
    address = re.sub(r' EIGHTH ',  ' 8TH ',  address)
    address = re.sub(r' NINTH ',  ' 9TH ',  address)
    address = re.sub(r' TENTH ',  ' 10TH ',  address)
    address = re.sub(r' ELEVENTH ',  ' 11TH ',  address)
    address = re.sub(r' TWELFTH ',  ' 12TH ',  address)
    address = re.sub(r' THIRTEENTH ',  ' 13TH ',  address)
    address = re.sub(r' FOURTEENTH ',  ' 14TH ',  address)
    address = re.sub(r' FIFTEENTH ',  ' 15TH ',  address)
    address = re.sub(r' SIXTEENTH ',  ' 16TH ',  address)
    address = re.sub(r' SEVENEENTH ',  ' 17TH ',  address)
    address = re.sub(r' EIGTHTEENTH ',  ' 18TH ',  address)
    address = re.sub(r' NINTEENTH ',  ' 19TH ',  address)
    address = re.sub(r' TWENTIETH ',  ' 20TH ',  address)
    address = re.sub(r' TWENTY FIRST ',  ' 24TH ',  address)

    return address

def cardinal_number(address):
    # convert from first to 1st and visa versa
    address = re.sub(r' 1ST ', ' FIRST ', address)
    address = re.sub(r' 2ND ',  ' SECOND ',  address)
    address = re.sub(r' 3RD ',  ' THIRD ',  address)
    address = re.sub(r' 4TH ',  ' FOURTH ',  address)
    address = re.sub(r' 5TH ',  ' FIFTH ',  address)
    address = re.sub(r' 6TH ',  ' SIXTH ',  address)
    address = re.sub(r' 7TH ',  ' SEVENTH ',  address)
    address = re.sub(r' 8TH ',  ' EIGHTH ',  address)
    address = re.sub(r' 9TH ',  ' NINTH ',  address)
    address = re.sub(r' 10TH ', ' TENTH ',  address)
    address = re.sub(r' 11TH ', ' ELEVENTH ',  address)
    address = re.sub(r' 12TH ', ' TWELFTH ',  address)
    address = re.sub(r' 13TH ', ' THIRTEENTH ',  address)
    address = re.sub(r' 14TH ', ' FOURTEENTH ',  address)
    address = re.sub(r' 15TH ', ' FIFTEENTH ',  address)
    address = re.sub(r' 16TH ',  ' SIXTEENTH ',  address)
    address = re.sub(r' 17TH ',  ' SEVENEENTH ',  address)
    address = re.sub(r' 18TH ',  ' EIGTHTEENTH ',  address)
    address = re.sub(r' 19TH ',  ' NINTEENTH ',  address)
    address = re.sub(r' 20TH ',  ' TWENTIETH ',  address)
    address = re.sub(r' 24TH ',  ' TWENTY FIRST ',  address)

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
    # IGNORE FOR DC at this time
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

    # custom DC rules go here
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

    test_address = cardinal_number(address)
    if (new_address != address):
        alts.append(new_address)

    return alts

def alt_address(address, force):
    # custom DC rules go here
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

    if ( new_address == address):
        new_address = re.sub(r'1707 7TH STREET NW',  'PARCEL 42',  address)
    if ( new_address == address):
        new_address = re.sub(r'CENTRAL BUSINESS DISTRICT',  'CBD',  address)

    # attempt to convert 11th => eleventh
    test_address = number_cardinal(new_address)
    if (test_address == new_address):
        test_address = cardinal_number(new_address)

    # unless we force it - only return a changed address
    if (force or test_address != address):
        return test_address
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
        cursor.execute("""CREATE TEMP TABLE addr_alias_fronts as (
            SELECT a.address_id, a.fulladdress,
                coalesce(b.front_vect,'{}')::TEXT as front_vect,
                b.property_id, b.parcel_id,
                a.addralias_id,
                a.aliasname,
                a.city,
                a.state,
                a.zipcode,
                a.status,
                a.aliastype,
                a.ssl,
                a.geometry
                FROM
                    temp.location_alias a LEFT OUTER JOIN %s.properties b
                    ON (a.ssl = b.local_id)
        )""" % (DB_SCHEMA))
        cursor.execute("""SELECT a.addralias_id::TEXT,
            a.aliasname as name,
            a.city,
            a.state,
            a.zipcode,
            'DCMAR_Alias'::TEXT as domain,
            0 as normative,
            a.status,
            a.aliastype,
            a.ssl,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(a.geometry) as location,
            a.fulladdress as proper_address,
            a.front_vect
            FROM
                addr_alias_fronts a
                """)
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

def index_addresses(prm):

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

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""DROP TABLE IF EXISTS address_list_temp""")
        cursor.execute("""CREATE TEMP TABLE address_list_temp AS (SELECT
            to_char(a.address_id, 'mar_000000000000000D')::TEXT as indexable_id,
            a.fulladdress,
            a.city,
            'DC' as state,
            a.zipcode::TEXT,
            a.addrnum::TEXT,
            a.ssl::TEXT as local_id,
            'SSL' as local_desc,
            a.res_type as addr_use,
            st_asgeojson(st_expand(a.geometry, 0.0000001)) as extent,
            st_asgeojson(a.geometry) as location,
            n.descriptio as neighborhood,
            '{}'::TEXT as camera,
            coalesce(p.front_vect,'{}')::TEXT as front_vect,
            a.fulladdress as proper_address
            FROM (temp.address_points a LEFT OUTER JOIN
                temp.nbhd n ON (st_intersects(a.geometry, n.geometry)))  LEFT OUTER JOIN
                %s p ON (p.local_id = a.ssl))""" % (property_tbl))
        logger.debug('''  inserted %d from address_points''' %(cursor.rowcount))

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
            )""" % (property_tbl))
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
            WHERE exists (SELECT 1 FROM temp.address_points a WHERE o.property_address = a.fulladdress)
            """)

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
            SELECT 'OTR_' || property_id::TEXT,
            property_address,
            'WASHINGTON',
            'DC',
            '',
            local_id,
            'SSL',
            coalesce(ax[1], '0') as addrnum,
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

def index_neighborhoods(prm):

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
        cursor.execute("""SELECT 'NBHD:' || objectid::TEXT, a.name as name,
            'WASHINGTON' as city,
            'DC' as state,
            'CREATE_DCZ'::TEXT as domain, 0 as normative,
            'G' as class,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(st_cleangeometry(a.geometry))) as location,
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
            'WASHINGTON' as city,
            'DC' as state,
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
            'WASHINGTON' as city,
            'DC' as state,
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
        cursor.execute("""SELECT geoid10::TEXT as objectid, a.name,
            'WASHINGTON' as city,
            'DC' as state,
            'create.io'::TEXT as domain, 0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT z.zcta5ce10 as name, z.geoid10, st_simplifypreservetopology(st_cleangeometry(z.geometry),0.00001) as geometry
                    FROM temp.census_zcta5 z
                    WHERE z.zcta5ce10 in (SELECT f.zcta5ce10 FROM temp.dc_census_face f WHERE
                       f.statefp10 = '11') )  as a""")
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
            'WASHINGTON' as city,
            'DC' as state,
            'create.io'::TEXT as domain, 0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT 'Washington, DC' as name, objectid, st_simplifypreservetopology(st_cleangeometry(geometry),0.0001) as geometry FROM temp.dc_boundary)  a""")
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

def index_quadrant(prm):

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
        cursor.execute("""SELECT objectid::TEXT, name,
            'WASHINGTON' as city,
            'DC' as state,
            'create.io'::TEXT as domain, 0 as normative,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location,
            st_asgeojson(a.geometry) as geometry
            FROM
                (SELECT name, objectid, st_simplifypreservetopology(st_cleangeometry(geometry),0.0001) as geometry FROM temp.dc_quadrants)  a""")
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

    index_submarket_commercial({"reset": True, "type": "SMC",  "descr": "Commercial Submarket"})
    index_submarket_residential({"reset": True, "type": "SMR",  "descr": "Residential Submarket"})

    index_market({"reset": True, "type": "market",  "descr": "Market"})
    index_postalcode({"reset": True, "type": "postalcode",  "descr": "ZIP Code"})
    index_quadrant({"reset": True, "type": "quadrant",  "descr": "Quadrant of City"})
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
