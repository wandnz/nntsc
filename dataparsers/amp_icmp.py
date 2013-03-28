from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql

import sys, string

STREAM_TABLE_NAME="streams_amp_icmp"
DATA_TABLE_NAME="data_amp_icmp"

amp_icmp_streams = {}

def stream_table(db):
    """ Specify the description of an icmp stream, used to create the table """

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packet_size', String, nullable=False),
        Column('datastyle', String, nullable=False),
        UniqueConstraint('destination', 'source', 'packet_size'),
        useexisting=True,
    )

    Index('index_amp_icmp_source', st.c.source)
    Index('index_amp_icmp_destination', st.c.destination)

    return STREAM_TABLE_NAME

def data_table(db):
    """ Specify the description of icmp data, used to create the table """

    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME

    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                nullable = False),
        Column('timestamp', Integer, nullable=False),
        Column('address', postgresql.INET, nullable=False),
        Column('packet_size', Integer, nullable=False),
        Column('rtt', Integer, nullable=True),
        Column('ttl', Integer, nullable=True),
        Column('loss', Boolean, nullable=False),
        Column('error_type', Integer, nullable=False),
        Column('error_code', Integer, nullable=False),
        useexisting=True,
    )

    Index('index_amp_icmp_timestamp', dt.c.timestamp)
    Index('index_amp_icmp_packet_size', dt.c.packet_size)

    return DATA_TABLE_NAME

def create_existing_stream(stream_data):
    """Extract the stream key from the stream data provided by NNTSC
when the AMP module is first instantiated"""

    key = (str(stream_data["source"]), str(stream_data["destination"]),
        str(stream_data["packet_size"]))

    amp_icmp_streams[key] = stream_data["stream_id"]

def data_stream_key(data, source):
    """Extract the stream key from the data received from the AMP
message broker"""

    return (source, data["target"], sizestr)

def insert_stream(db, exp, source, dest, size):

    name = "icmp %s:%s:%s" % (source, dest, size)

    props = {"name":name, "source":source, "destination":dest,
            "packet_size":size, "datastyle":"rtt_ms"}

    colid, streamid = db.register_new_stream("amp", "icmp", name)

    if colid == -1:
        return -1

    # insert stream into our stream table
    st = db.metadata.tables[STREAM_TABLE_NAME]
    
    try:
        result = db.conn.execute(st.insert(), stream_id=streamid, 
                source=source, destination=dest, packet_size=size,
                datastyle="rtt_ms")
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    if streamid >= 0 and exp != None:
        exp.send((1, (colid, "amp_icmp", streamid, props)))

    return streamid

def insert_data(db, exp, stream, ts, result):

    dt = db.metadata.tables[DATA_TABLE_NAME]
    
    try:
        db.conn.execute(dt.insert(), stream_id=stream, timestamp=ts,
                **result)
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    exp.send((0, ("amp_icmp", stream, ts, result)))

    return 0

def process_data(db, exp, timestamp, data, source):

    for d in data:
        if d["random"]:
            sizestr = "random"
        else:
            sizestr = str(d["packet_size"])
        
        d["source"] = source
        key = (source, d["target"], sizestr)

        if key in amp_icmp_streams:
            stream_id = amp_icmp_streams[key]
        else:
            stream_id = insert_stream(db, exp, source, d["target"], sizestr)

            if stream_id == -1:
                print >> sys.stderr, "AMPModule: Cannot create stream for:"
                print >> sys.stderr, "AMPModule: %s %s:%s:%s\n" % (
                        "icmp", source, d["target"], sizestr)
                return -1
            else:
                amp_icmp_streams[key] = stream_id

        insert_data(db, exp, stream_id, timestamp, d)
        db.update_timestamp(stream_id, timestamp)

def register(db):
    st_name = stream_table(db)
    dt_name = data_table(db)

    db.register_collection("amp", "icmp", st_name, dt_name)
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

