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


from sqlalchemy import Table, Column, Integer, \
    String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.sql import text
from sqlalchemy.types import Integer, String, SmallInteger
from sqlalchemy.exc import IntegrityError, OperationalError, DataError, \
        ProgrammingError, SQLAlchemyError
from sqlalchemy.dialects import postgresql
from libnntsc.database import DB_DATA_ERROR, DB_GENERIC_ERROR, DB_NO_ERROR
import libnntscclient.logger as logger

STREAM_TABLE_NAME = "streams_amp_traceroute"
DATA_TABLE_NAME = "data_amp_traceroute"

amp_trace_streams = {}

def stream_table(db):
    """ Specify the description of a traceroute stream, to create the table """

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packet_size', String, nullable=False),
        Column('address', postgresql.INET, nullable=False),
        UniqueConstraint('source', 'destination', 'packet_size', 'address'),
        useexisting=True,
    )

    Index('index_amp_traceroute_source', st.c.source)
    Index('index_amp_traceroute_destination', st.c.destination)

    return STREAM_TABLE_NAME

def data_table(db):
    """ Specify the description of traceroute data, used to create the table """

    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME

    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('timestamp', Integer, nullable=False),
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('path_id', Integer, nullable=False),
        Column('packet_size', SmallInteger, nullable=False),
        Column('length', SmallInteger, nullable=False),
        Column('error_type', SmallInteger, nullable=True),
        Column('error_code', SmallInteger, nullable=True),
        Column('hop_rtt', postgresql.ARRAY(Integer), nullable=False),
        useexisting=True,
    )

    paths = Table("data_amp_traceroute_paths", db.metadata,
        Column('path_id', Integer, primary_key=True),
        Column('path', postgresql.ARRAY(postgresql.INET), nullable=False, 
                unique=True),
        useexisting=True,
    )

    Index('index_amp_traceroute_timestamp', dt.c.timestamp)

    return DATA_TABLE_NAME


def register(db):
    st_name = stream_table(db)
    dt_name = data_table(db)

    db.register_collection("amp", "traceroute", st_name, dt_name)


def create_existing_stream(stream_data):
    """ Extract the stream key from the stream data provided by NNTSC
        when the AMP module is first instantiated.
    """

    key = (str(stream_data["source"]), str(stream_data["destination"]),
        str(stream_data["address"]), str(stream_data["packet_size"]))

    amp_trace_streams[key] = stream_data["stream_id"]


def insert_stream(db, exp, source, dest, size, address, timestamp):
    """ Insert a new traceroute stream into the streams table """

    name = "traceroute %s:%s:%s:%s" % (source, dest, address, size)

    props = {"source":source, "destination":dest,
            "packet_size":size, "datastyle":"traceroute",
            "address": address}

    streamid = db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME, 
            "amp", "traceroute", name, timestamp, props)

    if streamid <= 0:
        return streamid

    ret = db.clone_table("data_amp_traceroute_paths", streamid)
    if ret != DB_NO_ERROR:
        return ret

    # Ensure our custom foreign key gets perpetuated
    newtable = "%s_%d" % (DATA_TABLE_NAME, streamid)
    pathtable = "data_amp_traceroute_paths_%d" % (streamid)
    err = db.add_foreign_key(newtable, "path_id", pathtable, "path_id")

    if err == DB_NO_ERROR:
        return streamid
    return err

def insert_data(db, exp, stream, ts, result):
    """ Insert data for a single traceroute test into the database """

    # sqlalchemy is again totally useless and makes it impossible to cast
    # types on insert, so lets do it ourselves.

    pathtable = "data_amp_traceroute_paths_%d" % (stream)

    pathinsert = text("WITH s AS (SELECT path_id, CAST(:path AS inet[]) "
            "as path FROM %s "
            "WHERE path = CAST(:path AS inet[])), "
            "i AS (INSERT INTO %s (path) "
            "SELECT CAST(:path AS inet[]) WHERE "
            "NOT EXISTS (SELECT path FROM %s "
            "WHERE path = CAST(:path AS inet[])) "
            "RETURNING path_id, path) "
            "SELECT path_id, path FROM i UNION ALL "
            "SELECT path_id, path FROM s" % (pathtable, pathtable, pathtable))
        
   
    err, queryret = db.custom_insert(pathinsert, result)
     
    if err != DB_NO_ERROR:
        return err

    if queryret == None:
        return DB_DATA_ERROR
            
    result['path_id'] = queryret.fetchone()[0]
    queryret.close()
    
    insertfunc = text("INSERT INTO %s ("
                    "stream_id, timestamp, path_id, packet_size, length, "
                    "error_type, error_code, hop_rtt) VALUES ("
                    ":stream_id, :timestamp, :path_id, "
                    ":packet_size, :length, "
                    ":error_type, :error_code, CAST(:hop_rtt AS integer[]))" % \
                    (DATA_TABLE_NAME + "_" + str(stream)))

    return db.insert_data(exp, DATA_TABLE_NAME, "amp_traceroute", stream,
            ts, result, insertfunc)



def process_data(db, exp, timestamp, data, source):
    """ Process data (which may have multiple paths) and insert into the DB """
    done = {}

    # For each path returned in the test data
    for d in data:
        if d["random"]:
            sizestr = "random"
        else:
            sizestr = str(d["packet_size"])

        d["source"] = source
        key = (source, d["target"], d['address'], sizestr)

        if key in amp_trace_streams:
            stream_id = amp_trace_streams[key]

            if stream_id in done:
                continue
        else:
            stream_id = insert_stream(db, exp, source, d["target"], sizestr,
                    d['address'], timestamp)

            if stream_id < 0:
                logger.log("AMPModule: Cannot create stream for:")
                logger.log("AMPModule: %s %s:%s:%s:%s\n" % (
                        "traceroute", source, d["target"], d["address"],
                        sizestr))
                return stream_id
            else:
                amp_trace_streams[key] = stream_id

        # TODO maybe we want to change the way ampsave gives us this data
        # so we don't need to change it up again
        d["path"] = [x["address"] for x in d["hops"]]
        d["hop_rtt"] = [x["rtt"] for x in d["hops"]]

        res = insert_data(db, exp, stream_id, timestamp, d)
        if res != DB_NO_ERROR:
            return res
        done[stream_id] = 0
    return db.update_timestamp(done.keys(), timestamp)

def generate_union(qb, table, streams):

    unionparams = []
    sql = "(("

    for i in range(0, len(streams)):
        unionparams.append(streams[i])
        sql += "SELECT * FROM %s_" % (table)
        sql += "%s"     # stream id will go here

        if i != len(streams) - 1:
            sql += " UNION ALL "

    sql += ") AS allstreams JOIN ("
    
    for i in range(0, len(streams)):
        sql += "SELECT * from data_amp_traceroute_paths_"
        sql += "%s"
        if i != len(streams) - 1:
            sql += " UNION ALL "

    sql += ") AS paths ON (allstreams.path_id = paths.path_id)) AS dataunion"
    qb.add_clause("union", sql, unionparams + unionparams)


def sanitise_columns(columns):

    sanitised = []

    for c in columns:
        if c in ['path', 'path_id', 'stream_id', 'timestamp', 'hop_rtt', 
                'packet_size', 'length', 'error_type', 'error_code']:
            sanitised.append(c)

    return sanitised 


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
