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
from sqlalchemy.exc import IntegrityError, OperationalError, DataError, \
        ProgrammingError, SQLAlchemyError
from sqlalchemy.dialects import postgresql
import libnntscclient.logger as logger
from libnntsc.partition import PartitionedTable
from libnntsc.database import DB_NO_ERROR, DB_DATA_ERROR, DB_GENERIC_ERROR

import sys, string

STREAM_TABLE_NAME="streams_lpi_flows"
DATA_TABLE_NAME="data_lpi_flows"

lpi_flows_streams = {}
partitions = None

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
        Column('metric', String, nullable=False),
        UniqueConstraint('source', 'user', 'dir', 'freq', 'protocol', 'metric'),
        useexisting=True
    )

    Index('index_lpi_flows_source', st.c.source)
    Index('index_lpi_flows_user', st.c.user)
    Index('index_lpi_flows_dir', st.c.dir)
    Index('index_lpi_flows_protocol', st.c.protocol)
    Index('index_lpi_flows_metric', st.c.metric)

    return STREAM_TABLE_NAME

def data_table(db):
    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME
    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('flows', BigInteger),
        useexisting=True
    )

    return DATA_TABLE_NAME


def create_existing_stream(stream_data):

    key = (stream_data['source'], stream_data['user'], stream_data['dir'], \
            stream_data['freq'], stream_data['protocol'], stream_data['metric'])

    lpi_flows_streams[key] = stream_data['stream_id']


def find_stream(mon, user, dir, freq, proto, metric):
    k = (mon, user, dir, freq, proto, metric)
    if lpi_flows_streams.has_key(k):
        return lpi_flows_streams[k]
    return -1

def add_new_stream(db, exp, mon, user, dir, freq, proto, metric, ts):
    k = (mon, user, dir, freq, proto, metric)

    metstr = ""
    if metric == "peak":
        metstr = "Peak "
    if metric == "new":
        metstr = "New "

    dirstr = ""
    if dir == "out":
        dirstr = "outgoing"
    if dir == "in":
        dirstr = "incoming"

    namestr = "%s%s %s flows for user %s -- measured from %s every %s seconds" \
            % (metstr, proto, dirstr, user, mon, freq)

    colid, streamid = db.register_new_stream("lpi", "flows", namestr, ts)

    if colid < 0:
        return colid

    st = db.metadata.tables[STREAM_TABLE_NAME]
    try:
        result = db.conn.execute(st.insert(), stream_id=streamid,
                source=mon, user=user, dir=dir, freq=freq, protocol=proto,
                metric=metric)
    except (DataError, IntegrityError, ProgrammingError) as e:
        # These errors suggest that we have some bad data that we may be
        # able to just throw away and carry on
        db.rollback_transaction()
        logger.log(e)
        return DB_DATA_ERROR
    except SQLAlchemyError as e:
        # All other errors imply an issue with the database itself or the
        # way we have been using it. Restarting the database connection is
        # a better course of action in this case.
        db.rollback_transaction()
        logger.log(e)
        return DB_GENERIC_ERROR

    if streamid >= 0 and exp != None:
        exp.publishStream(colid, "lpi_flows", streamid, \
                {'source':mon, 'user':user, 'dir':dir, 'freq':freq, \
                'protocol':proto, 'metric':metric})

    return streamid

def insert_data(db, exp, stream_id, ts, value):
    global partitions
    dt = db.metadata.tables[DATA_TABLE_NAME]

    if partitions == None:
        partitions = PartitionedTable(db, DATA_TABLE_NAME, 60 * 60 * 24 * 7, ["timestamp", "stream_id"])

    partitions.update(ts)

    try:
        db.conn.execute(dt.insert(), stream_id=stream_id, timestamp=ts, flows=value)
    except (DataError, IntegrityError, ProgrammingError) as e:
        # These errors suggest that we have some bad data that we may be
        # able to just throw away and carry on
        db.rollback_transaction()
        logger.log(e)
        return DB_DATA_ERROR
    except SQLAlchemyError as e:
        # All other errors imply an issue with the database itself or the
        # way we have been using it. Restarting the database connection is
        # a better course of action in this case.
        db.rollback_transaction()
        logger.log(e)
        return DB_GENERIC_ERROR
    

    if exp != None:
        exp.publishLiveData("lpi_flows", stream_id, ts, {"flows":value})
    return 0

def process_data(db, exp, protomap, data):

    mon = data['id']
    user = data['user']
    dir = data['dir']
    freq = data['freq']
    done = []

    if data['metric'] == "newflows":
        metric='new'
    elif data['metric'] == "peakflows":
        metric='peak'
    else:
        logger.log("LPI Flows: Unknown Metric: %s" % (data['metric']))
        return DB_DATA_ERROR

    for p, val in data['results'].items():
        if p not in protomap.keys():
            logger.log("LPI Flows: Unknown protocol id: %u" % (p))
            return DB_DATA_ERROR
        stream_id = find_stream(mon, user, dir, freq, protomap[p], metric)
        if stream_id == -1:
            if val == 0:
                # Don't create a stream until we get a non-zero value
                continue

            stream_id = add_new_stream(db, exp, mon, user, dir, freq, 
                    protomap[p], metric, data['ts'])

            if stream_id < 0:
                logger.log("LPI Flows: Cannot create new stream")
                logger.log("LPI Flows: %s:%s %s %s %s %s\n" % (mon, user, dir, freq, protomap[p], metric))
                return stream_id
            else:
                lpi_flows_streams[(mon, user, dir, freq, protomap[p], metric)] = stream_id

        res = insert_data(db, exp, stream_id, data['ts'], val)
        if res != DB_NO_ERROR:
            return res
        done.append(stream_id)
    db.update_timestamp(done, data['ts'])
    return DB_NO_ERROR

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
