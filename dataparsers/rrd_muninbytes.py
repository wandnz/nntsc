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
from sqlalchemy.types import Integer, String, Float, BigInteger
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError,\
        ProgrammingError, DataError
import libnntscclient.logger as logger
from libnntsc.database import DB_NO_ERROR, DB_GENERIC_ERROR, DB_DATA_ERROR

STREAM_TABLE_NAME = "streams_rrd_muninbytes"
DATA_TABLE_NAME = "data_rrd_muninbytes"

def stream_table(db):

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        # rrd filename
        Column('filename', String, nullable=False),
        # switch name
        Column('switch', String, nullable=False),
        # name identifying the interface on the switch, usually a port number
        Column('interface', String, nullable=False),
        # textual description of the purpose of the interface, e.g. the host
        # or device connected to the port
        Column('interfacelabel', String, nullable=True),
        # direction, e.g. sent or received
        Column('direction', String, nullable=False),
        # seconds between measurements at highest resolution
        Column('minres', Integer, nullable=False, default=300),
        # number of measurements stored at highest resolution
        Column('highrows', Integer, nullable=False, default=1008),

        UniqueConstraint('filename', 'interface', 'switch', 'direction'),
        extend_existing=True,
    )

    return STREAM_TABLE_NAME

def data_table(db):

    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME

    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('bytes', BigInteger, nullable=True),
    )

    return DATA_TABLE_NAME

def insert_stream(db, exp, name, filename, switch, interface, dir, minres,
        rows, label):

    props = {"filename":filename, "switch":switch,
            "interface":interface, "direction":dir, "minres":minres,
            "highrows":rows, "interfacelabel":label}

    return db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME, "rrd", 
            "muninbytes", name, 0, props)


def insert_data(db, exp, stream, ts, line):
    assert(len(line) == 1)

    kwargs = {}
    exportdict = {}

    line_map = {0:"bytes"}

    for i in range(0, len(line)):
        if line[i] == None:
            val = None
        else:
            val = int(line[i])

        if val != None:
            kwargs[line_map[i]] = val
        exportdict[line_map[i]] = val

    return db.insert_data(exp, DATA_TABLE_NAME, "rrd_muninbytes", stream, ts,
            exportdict)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
