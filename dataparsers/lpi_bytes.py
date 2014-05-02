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


import libnntscclient.logger as logger
import sys, string
from libnntsc.dberrorcodes import *
from libnntsc.parsers.common import *

STREAM_TABLE_NAME = "streams_lpi_bytes"
DATA_TABLE_NAME = "data_lpi_bytes"
COLNAME = "lpi_bytes"

lpi_bytes_streams = {}

streamcols = [ \
        {"name":"source", "type":"varchar", "null":False},
        {"name":"user", "type":"varchar", "null":False},
        {"name":"dir", "type":"varchar", "null":False},
        {"name":"freq", "type":"integer", "null":False},
        {"name":"protocol", "type":"varchar", "null":False},
    ]

datacols = [ \
        {"name":"bytes", "type":"bigint"}
]


def stream_table(db):


    uniqcols = ['source', 'user', 'dir', 'freq', 'protocol']

    err = db.create_streams_table(STREAM_TABLE_NAME, streamcols, uniqcols)

    if err != DB_NO_ERROR:
        return None
    return STREAM_TABLE_NAME

def data_table(db):

    err =  db.create_data_table(DATA_TABLE_NAME, datacols)
    if err != DB_NO_ERROR:
        return None
    return DATA_TABLE_NAME


def create_existing_stream(stream_data):
    key = (stream_data['source'], stream_data['user'], stream_data['dir'], \
            stream_data['freq'], stream_data['protocol'])

    lpi_bytes_streams[key] = stream_data['stream_id']


def find_stream(mon, user, dir, freq, proto):
    k = (mon, user, dir, freq, proto)
    if lpi_bytes_streams.has_key(k):
        return lpi_bytes_streams[k]
    return -1

def add_new_stream(db, exp, mon, user, dir, freq, proto, ts):
    k = (mon, user, dir, freq, proto)

    dirstr = ""
    if dir == "out":
        dirstr = "outgoing"
    if dir == "in":
        dirstr = "incoming"

    namestr = "%s %s bytes for user %s -- measured from %s every %s seconds" \
            % (proto, dirstr, user, mon, freq)

    props = {'source':mon, 'user':user, 'dir':dir, 'freq':freq, 
            'protocol':proto}
    
    return create_new_stream(db, exp, "lpi", "bytes", namestr, streamcols,
            props, ts, STREAM_TABLE_NAME, DATA_TABLE_NAME)


def process_data(db, exp, protomap, data):

    mon = data['id']
    user = data['user']
    dir = data['dir']
    freq = data['freq']
    done = []

    for p, val in data['results'].items():
        if p not in protomap.keys():
            logger.log("LPI Bytes: Unknown protocol id: %u" % (p))
            return DB_DATA_ERROR
        stream_id = find_stream(mon, user, dir, freq, protomap[p])
        if stream_id == -1:
            if val == 0:
                # Don't create a stream until we get a non-zero value
                continue

            stream_id = add_new_stream(db, exp, mon, user, dir, freq,
                    protomap[p], data['ts'])

            if stream_id < 0:
                logger.log("LPI Bytes: Cannot create new stream")
                logger.log("LPI Bytes: %s:%s %s %s %s\n" % (mon, user, dir, freq, protomap[p]))
                return stream_id
            else:
                lpi_bytes_streams[(mon, user, dir, freq, protomap[p])] = stream_id

        code = insert_data(db, exp, stream_id, data['ts'], {'bytes':val},
                datacols, COLNAME, DATA_TABLE_NAME)
        
        if code != DB_NO_ERROR:
            return code
        done.append(stream_id)
   
    return db.update_timestamp(done, data['ts'])
    

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
