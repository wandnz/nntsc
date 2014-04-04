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
import libnntscclient.logger as logger

STREAM_TABLE_NAME = "streams_amp_icmp"
DATA_TABLE_NAME = "data_amp_icmp"

amp_icmp_streams = {}
    
streamcols = [ \
    {"name":"source", "type":"varchar", "null":False},
    {"name":"destination", "type":"varchar", "null":False},
    {"name":"address", "type":"inet", "null":False},
    {"name":"packet_size", "type":"varchar", "null":False},
    {"name":"datastyle", "type":"varchar", "null":False}
]

datacols = [ \
    {"name":"rtt", "type":"integer", "null":True},
    {"name":"packet_size", "type":"smallint", "null":False},
    {"name":"ttl", "type":"smallint", "null":True},
    {"name":"loss", "type":"smallint", "null":False},
    {"name":"error_type", "type":"smallint", "null":True},
    {"name":"error_code", "type":"smallint", "null":True},
]

def stream_table(db):
    """ Specify the description of an icmp stream, used to create the table """

    uniqcols = ['source', 'destination', 'packet_size', 'address']

    err = db.create_streams_table(STREAM_TABLE_NAME, streamcols, uniqcols)
    if err != DB_NO_ERROR:
        logger.log("Failed to create streams table for amp-icmp")
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
    """ Specify the description of icmp data, used to create the table """


    indexes = [{"columns":['rtt']}]

    err = db.create_data_table(DATA_TABLE_NAME, datacols, indexes)
    if err != DB_NO_ERROR:
        return None

    return DATA_TABLE_NAME

def create_existing_stream(stream_data):
    """Extract the stream key from the stream data provided by NNTSC
when the AMP module is first instantiated"""

    src = str(stream_data["source"])
    dest = str(stream_data["destination"])
    addr = str(stream_data["address"])
    size = str(stream_data["packet_size"])
    streamid = stream_data["stream_id"]

    key = (src, dest, addr, size)

    amp_icmp_streams[key] = streamid

def insert_stream(db, exp, source, dest, size, address, timestamp):
    """ Insert a new stream into the database and export to listeners """

    name = "icmp %s:%s:%s:%s" % (source, dest, address, size)

    props = {"source":source, "destination":dest,
            "packet_size":size, "datastyle":"rtt_ms", "address":address}

    return db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME,
            "amp", "icmp", name, timestamp, props)

def insert_data(db, exp, stream, ts, result):
    """ Insert a new measurement into the database and export to listeners """

    filtered = {}
    for col in datacols:
        if col["name"] in result:
            filtered[col["name"]] = result[col["name"]]
        else:
            filtered[col["name"]] = None

    return db.insert_data(exp, DATA_TABLE_NAME, "amp_icmp", stream, ts, filtered)

def process_data(db, exp, timestamp, data, source):
    """ Process a data object, which can contain 1 or more sets of results """
    done = {}

    for d in data:
        if d["random"]:
            sizestr = "random"
        else:
            sizestr = str(d["packet_size"])

        d["source"] = source
        key = (source, d["target"], d["address"], sizestr)

        if key in amp_icmp_streams:
            stream_id = amp_icmp_streams[key]

            if stream_id in done:
                continue
        else:
            stream_id = insert_stream(db, exp, source, d["target"], sizestr,
                    d["address"], timestamp)

            if stream_id < 0:
                logger.log("AMPModule: Cannot create stream for:")
                logger.log("AMPModule: %s %s:%s:%s:%s\n" % (
                        "icmp", source, d["target"], d["address"], sizestr))
                return stream_id
            else:
                amp_icmp_streams[key] = stream_id

        res = insert_data(db, exp, stream_id, timestamp, d)
        if res != DB_NO_ERROR:
            return res
        done[stream_id] = 0
    # update the last timestamp for all streams we just got data for
    return db.update_timestamp(done.keys(), timestamp)

def register(db):
    """ Register the amp-icmp collection """
    st_name = stream_table(db)
    dt_name = data_table(db)

    if st_name == None or dt_name == None:
        logger.log("Error creating AMP ICMP base tables")
        return DB_CODING_ERROR

    return db.register_collection("amp", "icmp", st_name, dt_name)
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

