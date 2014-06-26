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

import time
import libnntscclient.logger as logger
from libnntsc.dberrorcodes import *
from libnntsc.parsers.common import create_new_stream, insert_data

STREAM_TABLE_NAME="streams_rrd_smokeping"
DATA_TABLE_NAME="data_rrd_smokeping"
COLNAME = "rrd_smokeping"

smoke_streamcols = [ \
        {"name":"filename", "type":"varchar", "null":False}, 
        {"name":"source", "type":"varchar", "null":False}, 
        {"name":"host", "type":"varchar", "null":False}, 
        {"name":"minres", "type":"integer", "null":False, "default":"300"}, 
        {"name":"highrows", "type":"integer", "null":False, "default":"1008"}
    ]

smoke_datacols = [ \
        {"name":"loss", "type":"smallint"},
        {"name":"median", "type":"double precision"},
        {"name":"pings", "type":"double precision[]"}
    ]


def stream_table(db):

    uniqcols = ['filename', 'source', 'host']

    err = db.create_streams_table(STREAM_TABLE_NAME, smoke_streamcols, uniqcols)

    if err != DB_NO_ERROR:
        return None
    return STREAM_TABLE_NAME

def data_table(db):

    err =  db.create_data_table(DATA_TABLE_NAME, smoke_datacols)
    if err != DB_NO_ERROR:
        return None
    return DATA_TABLE_NAME

def insert_stream(db, exp, name, fname, source, host, minres, rows):

    props = {"filename":fname, "source":source, "host":host,
            "minres":minres, "highrows":rows}

    return create_new_stream(db, exp, "rrd", "smokeping", name, 
            smoke_streamcols, props, 0, STREAM_TABLE_NAME, DATA_TABLE_NAME)


def get_last_timestamp(db, stream):
    err, lastts = db.get_last_timestamp(DATA_TABLE_NAME, stream)
    if err != DB_NO_ERROR:
        return time.time()
    return lastts

def process_data(db, exp, stream, ts, line):
    kwargs = {}

    if len(line) >= 1:
        if line[1] == None:
            kwargs['loss'] = None
        else:
            kwargs['loss'] = int(float(line[1]))

    if len(line) >= 2:
        if line[2] == None:
            kwargs['median'] = None
        else:
            kwargs['median'] = round(float(line[2]) * 1000.0, 6)
        
    kwargs['pings'] = []

    for i in range(3, len(line)):
        if line[i] == None:
            val = None
        else:
            val = round(float(line[i]) * 1000.0, 6)

        kwargs['pings'].append(val)

    return insert_data(db, exp, stream, ts, kwargs, smoke_datacols, COLNAME,
            DATA_TABLE_NAME, {"pings":"double precision[]"})


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

