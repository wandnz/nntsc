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
from sqlalchemy.types import Integer, String, Float, SmallInteger
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError,\
        SQLAlchemyError, DataError
import libnntscclient.logger as logger
from libnntsc.database import DB_NO_ERROR, DB_GENERIC_ERROR, DB_DATA_ERROR

STREAM_TABLE_NAME="streams_rrd_smokeping"
DATA_TABLE_NAME="data_rrd_smokeping"

def stream_table(db):

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        # rrd filename
        Column('filename', String, nullable=False),
        # machine that is running smokeping
        Column('source', String, nullable=False),
        # host (fqdn or ip address)
        Column('host', String, nullable=False),
        # seconds between measurements at highest resolution
        Column('minres', Integer, nullable=False, default=300),
        # number of measurements stored at highest resolution
        Column('highrows', Integer, nullable=False, default=1008),

        UniqueConstraint('filename', 'source', 'host'),
        useexisting=True
    )

    return STREAM_TABLE_NAME

def data_table(db):

    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME

    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('loss', SmallInteger, nullable=True),
        Column('uptime', Float, nullable=True),
        Column('median', Float, nullable=True),
        Column('ping1', Float, nullable=True),
        Column('ping2', Float, nullable=True),
        Column('ping3', Float, nullable=True),
        Column('ping4', Float, nullable=True),
        Column('ping5', Float, nullable=True),
        Column('ping6', Float, nullable=True),
        Column('ping7', Float, nullable=True),
        Column('ping8', Float, nullable=True),
        Column('ping9', Float, nullable=True),
        Column('ping10', Float, nullable=True),
        Column('ping11', Float, nullable=True),
        Column('ping12', Float, nullable=True),
        Column('ping13', Float, nullable=True),
        Column('ping14', Float, nullable=True),
        Column('ping15', Float, nullable=True),
        Column('ping16', Float, nullable=True),
        Column('ping17', Float, nullable=True),
        Column('ping18', Float, nullable=True),
        Column('ping19', Float, nullable=True),
        Column('ping20', Float, nullable=True),
        useexisting=True
    )

    Index('index_rrd_smokeping_timestamp', dt.c.timestamp)

    return DATA_TABLE_NAME

def insert_stream(db, exp, name, fname, source, host, minres, rows):

    props = {"filename":fname, "source":source, "host":host,
            "minres":minres, "highrows":rows}

    return db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME, "rrd", 
            "smokeping", name, 0, props)


def insert_data(db, exp, stream, ts, line):
    # This is terrible :(

    kwargs = {}
    exportdict = {}
    line_map = {0:"uptime", 1:"loss", 2:"median", 3:"ping1", 4:"ping2",
        5:"ping3", 6:"ping4", 7:"ping5", 8:"ping6", 9:"ping7", 10:"ping8",
        11:"ping9", 12:"ping10", 13:"ping11", 14:"ping12", 15:"ping13",
        16:"ping14", 17:"ping15", 18:"ping16", 19:"ping17", 20:"ping18",
        21:"ping19", 22:"ping20"}

    for i in range(0, len(line)):
        if line[i] == None:
            val = None
        elif i == 1:
            val = int(float(line[i]))
        elif i > 1:
            val = round(float(line[i]) * 1000.0, 6)
        else:
            val = round(float(line[i]), 6)

        if val != None:
            kwargs[line_map[i]] = val

        exportdict[line_map[i]] = val

    return db.insert_data(exp, DATA_TABLE_NAME, "rrd_smokeping", stream, ts, 
            exportdict)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

