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
    String, MetaData, ForeignKey, UniqueConstraint, Index, Sequence, \
    ForeignKeyConstraint
from sqlalchemy.sql import and_, or_, not_, text
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import select, join, outerjoin, func, label
from libnntsc.partition import PartitionedTable
import libnntscclient.logger as logger

STREAM_TABLE_NAME="streams_amp_traceroute"
DATA_VIEW_NAME="data_amp_traceroute"

amp_trace_streams = {}
test_partitions = None
path_partitions = None

def stream_table(db):

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
        extend_existing=True,
    )

    Index('index_amp_traceroute_source', st.c.source)
    Index('index_amp_traceroute_destination', st.c.destination)

    return STREAM_TABLE_NAME

def data_tables(db):
    if DATA_VIEW_NAME in db.metadata.tables:
        return DATA_VIEW_NAME

    testtable = Table("internal_amp_traceroute_test", db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('traceroute_test_id', Integer, primary_key=True),
        Column('length', Integer, nullable=False),
        Column('error_type', Integer, nullable=False),
        Column('error_code', Integer, nullable=False),
        Column('packet_size', Integer, nullable=False),
        extend_existing=True,
    )


    pathtable = Table("internal_amp_traceroute_path", db.metadata,
        Column('teststart', Integer, nullable=False),
        Column('test_id', Integer,
                ForeignKey("internal_amp_traceroute_test.traceroute_test_id",
                    ondelete="CASCADE")),
        Column('hop_address', postgresql.INET, nullable=False),
        Column('path_ttl', Integer, nullable=False),
        Column('hop_rtt', Integer, nullable=True),
        extend_existing=True,
    )

    # This view is kinda tricky
    fh = pathtable.alias()

    viewquery = select([testtable.c.stream_id, testtable.c.timestamp,
            testtable.c.traceroute_test_id,
            testtable.c.length, testtable.c.error_type,
            testtable.c.error_code, testtable.c.packet_size,
            fh.c.path_ttl, fh.c.hop_rtt,
            fh.c.hop_address]).select_from(testtable.join(fh, \
            and_(fh.c.teststart == testtable.c.timestamp, \
            fh.c.test_id == testtable.c.traceroute_test_id)))

    dataview = db.create_view(DATA_VIEW_NAME, viewquery)

    return DATA_VIEW_NAME


def register(db):
    st_name = stream_table(db)
    dt_name = data_tables(db)

    db.register_collection("amp", "traceroute", st_name, dt_name)


def create_existing_stream(stream_data):
    """ Extract the stream key from the stream data provided by NNTSC
        when the AMP module is first instantiated.
    """

    key = (str(stream_data["source"]), str(stream_data["destination"]),
        str(stream_data["address"]), str(stream_data["packet_size"]))

    amp_trace_streams[key] = stream_data["stream_id"]


def data_stream_key(data, source):
    """ Extract the stream key from the data received from the AMP
        message broker.
    """

    return (source, data["target"], sizestr)


def insert_stream(db, exp, source, dest, size, address, timestamp):
    """ Insert a new traceroute stream into the streams table """

    name = "traceroute %s:%s:%s:%s" % (source, dest, address, size)

    props = {"name":name, "source":source, "destination":dest,
            "packet_size":size, "datastyle":"traceroute",
            "address": address}

    colid, streamid = db.register_new_stream("amp", "traceroute", name, 
            timestamp)

    if colid == -1:
        return -1

    # insert stream into our stream table
    st = db.metadata.tables[STREAM_TABLE_NAME]

    try:
        result = db.conn.execute(st.insert(), stream_id=streamid,
                source=source, destination=dest, packet_size=size,
                address=address, datastyle="traceroute")
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1

    if streamid >= 0 and exp != None:
        exp.send((1, (colid, "amp_traceroute", streamid, props)))

    return streamid


def insert_data(db, exp, stream, ts, test_info, hop_info):
    """ Insert data for a single traceroute test into the database """
    global test_partitions, path_partitions

    # information linking a test run together with the hops visited
    path_table = db.metadata.tables["internal_amp_traceroute_path"]

    # insert test information for this particular test run
    dt = db.metadata.tables["internal_amp_traceroute_test"]
    
    if test_partitions == None:
        test_partitions = PartitionedTable(db, "internal_amp_traceroute_test", 
                60 * 60 * 24 * 7, ["timestamp", "stream_id", "traceroute_test_id"])
    test_partitions.update(ts)
    db.commit_transaction()
   
    try:
        query = dt.insert().values(stream_id=stream, timestamp=ts, **test_info)
        db.conn.execute(query)
        
        # This is a terrible way of getting the new test id, but we can't
        # just rely on RETURNING it from the previous insert because table
        # partitioning doesn't really allow it.
        #
        # To clarify, it is possible to return the inserted row from our
        # "before insert" trigger function that inserts the data into the 
        # right partition. However, returning NULL is required to prevent the
        # row from also being inserted into the parent table. Some solutions
        # propose setting up a second trigger to remove the duplicate row
        # from the parent table but this doesn't work so well if you have a
        # ON DELETE CASCADE clause as it will purge everything that refers to
        # the new test.
        #
        # In short, inserting into table partitions sucks in postgresql.
        query = text("""SELECT max(traceroute_test_id) FROM 
                internal_amp_traceroute_test WHERE stream_id=:streamid AND
                "timestamp"=:ts AND length=:length AND
                error_type=:etype AND error_code=:ecode AND 
                packet_size=:size;""") 
        result = db.conn.execute(query, streamid=stream, ts=ts, 
                length=test_info['length'], etype=test_info['error_type'],
                ecode=test_info['error_code'], size=test_info['packet_size'])

        assert(result.rowcount == 1);
        row = result.fetchone()
        test_id = row[0]
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1

    db.commit_transaction()

    if path_partitions == None:
        path_partitions = PartitionedTable(db, "internal_amp_traceroute_path",
                60 * 60 * 24 * 7, ["test_id", "teststart"], "teststart")
    path_partitions.update(ts)

    # insert each hop along the path
    ttl = 1
    for hop in hop_info:
        try:
            db.conn.execute(path_table.insert(), teststart=ts,
                    test_id=test_id, hop_address=hop["address"], path_ttl=ttl,
                    hop_rtt=hop["rtt"])
        except IntegrityError, e:
            db.rollback_transaction()
            logger.log(e)
            return -1
        ttl += 1

        data = {}
        data['stream_id'] = stream
        data['timestamp'] = ts
        data['traceroute_test_id'] = test_id
        data['length'] = test_info['length']
        data['error_type'] = test_info['error_type']
        data['error_code'] = test_info['error_code']
        data['packet_size'] = test_info['packet_size']
        data['path_ttl'] = ttl
        data['hop_address'] = hop['address']
        data['hop_rtt'] = hop['rtt']

        #print "Exporting amp traceroute data"
        exp.send((0, ("amp_traceroute", stream, ts, data)))
        #print "Exported amp traceroute data"

    return 0

def process_data(db, exp, timestamp, data, source):

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
        else:
            stream_id = insert_stream(db, exp, source, d["target"], sizestr,
                    d['address'], timestamp)

            if stream_id == -1:
                logger.log("AMPModule: Cannot create stream for:")
                logger.log("AMPModule: %s %s:%s:%s:%s\n" % (
                        "traceroute", source, d["target"], d["address"], 
                        sizestr))
                return -1
            else:
                amp_trace_streams[key] = stream_id

        test_data = {
            #"source": source,
            #"target": d["target"],
            "length": d["length"],
            "error_type": d["error_type"],
            "error_code": d["error_code"],
            "packet_size": d["packet_size"],
        }

        insert_data(db, exp, stream_id, timestamp, test_data, d["hops"])
        db.update_timestamp(stream_id, timestamp)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
