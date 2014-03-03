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
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError,\
        DataError, SQLAlchemyError
from sqlalchemy.dialects import postgresql
import libnntscclient.logger as logger
from libnntsc.database import DB_NO_ERROR, DB_DATA_ERROR, DB_GENERIC_ERROR
import sys, string

STREAM_TABLE_NAME = "streams_lpi_packets"
DATA_TABLE_NAME = "data_lpi_packets"

lpi_packets_streams = {}

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

    Index('index_lpi_packets_source', st.c.source)
    Index('index_lpi_packets_user', st.c.user)
    Index('index_lpi_packets_dir', st.c.dir)
    Index('index_lpi_packets_protocol', st.c.protocol)

    return STREAM_TABLE_NAME

def data_table(db):
    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME
    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('packets', BigInteger),
        useexisting=True
    )

    return DATA_TABLE_NAME


def create_existing_stream(stream_data):
    key = (stream_data['source'], stream_data['user'], stream_data['dir'], \
            stream_data['freq'], stream_data['protocol'])

    lpi_packets_streams[key] = stream_data['stream_id']


def find_stream(mon, user, dir, freq, proto):
    k = (mon, user, dir, freq, proto)
    if lpi_packets_streams.has_key(k):
        return lpi_packets_streams[k]
    return -1

def add_new_stream(db, exp, mon, user, dir, freq, proto, ts):
    k = (mon, user, dir, freq, proto)

    dirstr = ""
    if dir == "out":
        dirstr = "outgoing"
    if dir == "in":
        dirstr = "incoming"

    namestr = "%s %s packets for user %s -- measured from %s every %s seconds" \
            % (proto, dirstr, user, mon, freq)
    props = {'source':mon, 'user':user, 'dir':dir, 'freq':freq, 
            'protocol':proto}

    return db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME, 
            "lpi", "packets", namestr, ts, props)


def insert_data(db, exp, stream_id, ts, value):
    result = {"packets":value}

    return db.insert_data(exp, DATA_TABLE_NAME, "lpi_packets", stream_id, ts,
            result)

def process_data(db, exp, protomap, data):

    mon = data['id']
    user = data['user']
    dir = data['dir']
    freq = data['freq']
    done = []

    for p, val in data['results'].items():
        if p not in protomap.keys():
            logger.log("LPI Packets: Unknown protocol id: %u" % (p))
            return DB_DATA_ERROR
        stream_id = find_stream(mon, user, dir, freq, protomap[p])
        if stream_id == -1:
            if val == 0:
                # Don't create a stream until we get a non-zero value
                continue

            stream_id = add_new_stream(db, exp, mon, user, dir, freq,
                    protomap[p], data['ts'])

            if stream_id < 0:
                logger.log("LPI Packets: Cannot create new stream")
                logger.log("LPI Packets: %s:%s %s %s %s\n" % (mon, user, dir, freq, protomap[p]))
                return stream_id
            else:
                lpi_packets_streams[(mon, user, dir, freq, protomap[p])] = stream_id

        res = insert_data(db, exp, stream_id, data['ts'], val)
        if res != DB_NO_ERROR:
            return res
        done.append(stream_id)
    return db.update_timestamp(done, data['ts'])

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
