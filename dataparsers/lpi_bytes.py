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


from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean, BigInteger
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
import libnntsc.logger as logger

import sys, string

STREAM_TABLE_NAME="streams_lpi_bytes"
DATA_TABLE_NAME="data_lpi_bytes"

lpi_bytes_streams = {}

def stream_table(db):
    
    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('user', String, nullable=False),
        Column('dir', String, nullable=False),
        Column('freq', Integer, nullable=False),
        Column('protocol', String, nullable=False),
        UniqueConstraint('source', 'user', 'dir', 'freq', 'protocol'),
        useexisting=True
    )

    Index('index_lpi_bytes_source', st.c.source)
    Index('index_lpi_bytes_user', st.c.user)
    Index('index_lpi_bytes_dir', st.c.dir)
    Index('index_lpi_bytes_protocol', st.c.protocol)

    return STREAM_TABLE_NAME

def data_table(db):
    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME
    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('bytes', BigInteger),
        useexisting=True
    )

    Index('index_lpi_bytes_timestamp', dt.c.timestamp)
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

def add_new_stream(db, exp, mon, user, dir, freq, proto):
    k = (mon, user, dir, freq, proto)

    namestr = "%s bytes %s for user %s -- measured from %s every %s seconds" \
            % (proto, dir, user, mon, freq)

    colid, streamid = db.register_new_stream("lpi", "bytes", namestr)

    if colid == -1:
        return -1

    st = db.metadata.tables[STREAM_TABLE_NAME]
    try:
        result = db.conn.execute(st.insert(), stream_id=streamid,
                source=mon, user=user, dir=dir, freq=freq, protocol=proto)
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1

    if streamid >= 0 and exp != None:
        exp.send((1, (colid, "lpi_bytes", streamid, \
                {'source':mon, 'user':user, 'dir':dir, 'freq':freq, 'protocol':proto})))

    return streamid

def insert_data(db, exp, stream_id, ts, value):
    dt = db.metadata.tables[DATA_TABLE_NAME]

    try:
        db.conn.execute(dt.insert(), stream_id=stream_id, timestamp=ts, bytes=value)
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1
    
    exp.send((0, ("lpi_bytes", stream_id, ts, {"bytes":value})))
    return 0

def process_data(db, exp, protomap, data):

    mon = data['id']
    user = data['user']
    dir = data['dir']
    freq = data['freq']

    for n in data['results']:
        if n[0] not in protomap.keys():
            logger.log("LPI Bytes: Unknown protocol id: %u" % (n[0]))
            return -1
        stream_id = find_stream(mon, user, dir, freq, protomap[n[0]])
        if stream_id == -1:
            stream_id = add_new_stream(db, exp, mon, user, dir, freq, protomap[n[0]])

            if stream_id == -1:
                logger.log("LPI Bytes: Cannot create new stream")
                logger.log("LPI Bytes: %s:%s %s %s %s\n" % (mon, user, dir, freq, protomap[n[0]]))
                return -1
            else:
                lpi_bytes_streams[(mon, user, dir, freq, protomap[n[0]])] = stream_id

        insert_data(db, exp, stream_id, data['ts'], n[1])
        db.update_timestamp(stream_id, data['ts'])
    return 0
        

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
