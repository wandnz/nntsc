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
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import select, join, outerjoin, func, label
from libnntsc.partition import PartitionedTable
import libnntscclient.logger as logger

STREAM_TABLE_NAME = "streams_amp_udpstream"
DATA_VIEW_NAME = "data_amp_udpstream"

def stream_table(db):

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packetsize', Integer, nullable=False),
        Column('packetspacing', Integer, nullable=False),
        UniqueConstraint('source', 'destination', 'packetsize',
                'packetspacing'),
        extend_existing=True,
    )

    Index('index_amp_udpstream_source', st.c.source)
    Index('index_amp_udpstream_destination', st.c.destination)
    Index('index_amp_udpstream_packetsize', st.c.packetsize)
    Index('index_amp_udpstream_packetspacing', st.c.packetspacing)

    return STREAM_TABLE_NAME

def data_tables(db):

    if DATA_VIEW_NAME in db.metadata.tables:
        return DATA_VIEW_NAME

    testtable = Table('internal_amp_udpstream_test', db.metadata,
        Column('test_id', Integer, primary_key=True),
        Column('stream_id', Integer, nullable=False),
        Column('test_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        UniqueConstraint('stream_id', 'test_starttime'),
        extend_existing=True,
    )

    Index('index_amp_udpstream_teststart', testtable.c.test_starttime)
    Index('index_amp_udpstream_teststream', testtable.c.stream_id)

    pkttable = Table('internal_amp_udpstream_packets', db.metadata,
        Column('test_id', Integer,
            ForeignKey('internal_amp_udpstream_test.test_id',
                    ondelete="CASCADE"),
            nullable=False),
        Column('packet_order', Integer, nullable=False),
        Column('arrivaltime', postgresql.DOUBLE_PRECISION, nullable=False),
        UniqueConstraint('test_id', 'packet_order'),
        extend_existing=True,
    )

    Index('index_amp_udpstream_packettestid', pkttable.c.test_id)

    viewquery = testtable.join(pkttable).select().reduce_columns()
    dataview = db.create_view(DATA_VIEW_NAME, viewquery)

    return DATA_VIEW_NAME



def register(db):
    st_name = stream_table(db)
    dt_name = data_tables(db)

    db.register_collection("amp", "udpstream", st_name, dt_name)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
