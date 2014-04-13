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
from libnntsc.dberrorcodes import *
import sys, string

STREAM_TABLE_NAME = "streams_lpi_users"
DATA_TABLE_NAME = "data_lpi_users"

lpi_users_streams = {}

def stream_table(db):

    streamcols = [ \
        {"name":"source", "type":"varchar", "null":False},
        {"name":"metric", "type":"varchar", "null":False},
        {"name":"freq", "type":"integer", "null":False},
        {"name":"protocol", "type":"varchar", "null":False},
    ]

    uniqcols = ['source', 'metric', 'freq', 'protocol']

    err = db.create_streams_table(STREAM_TABLE_NAME, streamcols, uniqcols)

    if err != DB_NO_ERROR:
        return None
    return STREAM_TABLE_NAME

def data_table(db):

    datacols = [ \
        {"name":"users", "type":"integer"}
    ]

    err =  db.create_data_table(DATA_TABLE_NAME, datacols)
    if err != DB_NO_ERROR:
        return None
    return DATA_TABLE_NAME


def create_existing_stream(stream_data):
    key = (stream_data['source'], stream_data['freq'], \
            stream_data['protocol'], stream_data['metric'])

    lpi_users_streams[key] = stream_data['stream_id']


def find_stream(mon, freq, proto, metric):
    k = (mon, freq, proto, metric)
    if lpi_users_streams.has_key(k):
        return lpi_users_streams[k]
    return -1

def add_new_stream(db, exp, mon, freq, proto, metric, ts):
    k = (mon, freq, proto, metric)

    metstr = ""
    if metric == "active":
        metstr = "Active "
    if metric == "observed":
        metstr = "Observed "

    namestr = "%s%s users -- measured from %s every %s seconds" \
            % (metstr, proto, mon, freq)

    props = {'source':mon, 'freq':freq, 'protocol':proto, 'metric':metric}

    return db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME, 
            "lpi", "users", namestr, ts, props)


def insert_data(db, exp, stream_id, ts, value):
    result = {"users":value}

    return db.insert_data(exp, DATA_TABLE_NAME, "lpi_users", stream_id, ts, result)


def process_data(db, exp, protomap, data):

    mon = data['id']
    freq = data['freq']
    done = []

    if data['metric'] == "activeusers":
        metric='active'
    elif data['metric'] == "observedusers":
        metric='observed'
    else:
        logger.log("LPI Users: Unknown Metric: %s" % (data['metric']))
        return DB_DATA_ERROR

    for p, val in data['results'].items():
        if p not in protomap.keys():
            logger.log("LPI Users: Unknown protocol id: %u" % (p))
            return DB_DATA_ERROR
        stream_id = find_stream(mon, freq, protomap[p], metric)
        if stream_id == -1:
            stream_id = add_new_stream(db, exp, mon, freq, protomap[p], metric,
                    data['ts'])

            if stream_id < 0:
                logger.log("LPI Users: Cannot create new stream")
                logger.log("LPI Users: %s %s %s %s\n" % (mon, freq, protomap[p], metric))
                return stream_id
            else:
                lpi_users_streams[(mon, freq, protomap[p], metric)] = stream_id

        res = insert_data(db, exp, stream_id, data['ts'], val)
        if res != DB_NO_ERROR:
            return res
        done.append(stream_id)
    return db.update_timestamp(done, data['ts'])

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
