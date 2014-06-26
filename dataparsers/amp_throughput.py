# This file is part of NNTSC
#
# Copyright (C) 2013 The University of Waikato, Hamilton, New Zealand
# Authors: Shane Alcock
#          Brendon Jones
#          Nathan Overall
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For more information, please see
# http://www.wand.net.nz/
#
# This source code is proprietary to the University of Waikato and may not be
# redistributed, published or disclosed without prior permission from the
# University of Waikato and the WAND Network Research Group.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#
# $Id$

from libnntsc.dberrorcodes import *
from libnntsc.parsers.common import create_new_stream, insert_data
import libnntscclient.logger as logger

STREAM_TABLE_NAME = "streams_amp_throughput"
DATA_TABLE_NAME = "data_amp_throughput"
COLNAME = "amp_throughput"

amp_tput_streams = {}

tput_streamcols = [ \
    {"name":"source", "type":"varchar", "null":False},
    {"name":"destination", "type":"varchar", "null":False},
    {"name":"direction", "type":"varchar", "null":False},
    {"name":"localaddress", "type":"inet", "null":False},
    {"name":"remoteaddress", "type":"inet", "null":False},
    {"name":"duration", "type":"integer", "null":False},
    {"name":"writesize", "type":"integer", "null":False}, 
    {"name":"tcpreused", "type":"boolean", "null":False},
]

tput_datacols = [ \
    {"name":"bytes", "type":"bigint", "null":True},    
    {"name":"packets", "type":"bigint", "null":True},
    {"name":"runtime", "type":"integer", "null":True}
]

def construct_key(stream_data):
    src = str(stream_data["source"])
    dest = str(stream_data["destination"])
    direction = str(stream_data["direction"])
    local = stream_data["localaddress"]
    remote = stream_data["remoteaddress"]
    duration = str(stream_data["duration"])
    writesize = str(stream_data["writesize"])
    reused = stream_data["tcpreused"]
    
    key = (src, dest, direction, local, remote, duration, writesize, reused)
    return key

def create_existing_stream(stream_data):
    key = construct_key(stream_data)
    streamid = stream_data["stream_id"]
    amp_tput_streams[key] = streamid

def insert_stream(db, exp, timestamp, result):
    return create_new_stream(db, exp, "amp", "throughput", 
            tput_streamcols, result, timestamp, STREAM_TABLE_NAME, 
            DATA_TABLE_NAME)

def stream_table(db):
    uniqcols = ['source', 'destination', 'direction', 'localaddress', \
            'remoteaddress', 'duration', 'writesize', 'tcpreused']

    err = db.create_streams_table(STREAM_TABLE_NAME, tput_streamcols, uniqcols)

    if err != DB_NO_ERROR:
        logger.log("Failed to create streams table for amp-throughput")
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['source'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create source index on %s" % (STREAM_TABLE_NAME))
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['destination'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create dest index on %s" % (STREAM_TABLE_NAME))
        return None

    return STREAM_TABLE_NAME

def data_table(db):

    # Do we need this? Or do we want one on packets as well?
    indexes = [{"columns":['bytes']}]

    err = db.create_data_table(DATA_TABLE_NAME, tput_datacols, indexes)
    if err != DB_NO_ERROR:
        return None
    return DATA_TABLE_NAME

def process_single_result(db, exp, timestamp, resdict):
    key = construct_key(resdict)
    
    if key in amp_tput_streams:
        stream_id = amp_tput_streams[key]
    else:
        stream_id = insert_stream(db, exp, timestamp, resdict)

        if stream_id < 0:
            logger.log("AMPModule: Cannot create new throughput stream")
            logger.log("AMPModule: %s:%s:%s:%s" % ( \
                    resdict['source'], resdict['destination'], 
                    resdict['duration'],
                    resdict['writesize']))
            return stream_id
        else:
            amp_tput_streams[key] = stream_id

    err = insert_data(db, exp, stream_id, timestamp, resdict, tput_datacols,
            COLNAME, DATA_TABLE_NAME)
    if err != DB_NO_ERROR:
        return err
    return stream_id


def process_data(db, exp, timestamp, data, source):
    done = {}
    for result in data['results']:
        resdict = {}
        resdict['source'] = source
        resdict['destination'] = data['target']
        resdict['localaddress'] = data['local_address']
        resdict['remoteaddress'] = data['address']
        resdict['direction']  = result['direction']
        resdict['duration'] = result['duration']
        resdict['runtime'] = result['runtime']
        resdict['writesize'] = result['write_size']
        resdict['bytes'] = result['bytes']
        resdict['packets'] = result['packets']
        resdict['tcpreused'] = result['tcpreused']

        streamid = process_single_result(db, exp, timestamp, resdict)
        if streamid < 0:
            return streamid
        done[streamid] = 0

    return db.update_timestamp(done.keys(), timestamp)

def register(db):
    st_name = stream_table(db)
    dt_name = data_table(db)

    if st_name == None or dt_name == None:
        logger.log("Error creating AMP throughput base tables")
        return DB_CODING_ERROR

    return db.register_collection("amp", "throughput", st_name, dt_name)
 

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

