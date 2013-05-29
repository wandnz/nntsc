from sqlalchemy import create_engine, Table, Column, Integer, \
	String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, BigInteger
from sqlalchemy.exc import IntegrityError, OperationalError

STREAM_TABLE_NAME="streams_rrd_muninbytes"
DATA_TABLE_NAME="data_rrd_muninbytes"

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

    Index('index_rrd_muninbytes_stream', dt.c.stream_id)
    Index('index_rrd_muninbytes_ts', dt.c.timestamp)
    return DATA_TABLE_NAME

def insert_stream(db, exp, name, filename, switch, interface, dir, minres,
        rows, label):

    props = {"name":name, "filename":filename, "switch":switch, 
            "interface":interface, "direction":dir, "minres":minres,
            "highrows":rows, "interfacelabel":label}

    colid, streamid = db.register_new_stream("rrd", "muninbytes", name)

    if colid == -1:
        return -1

    st = db.metadata.tables[STREAM_TABLE_NAME]

    try:
        result = db.conn.execute(st.insert(), stream_id=streamid,
                filename=filename, switch=switch, interface=interface,
                direction=dir, minres=minres, highrows=rows, 
                interfacelabel=label)
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    if streamid >= 0 and exp != None:
        exp.send((1, (colid, "rrd_muninbytes", streamid, props)))

    return streamid

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

    dt = db.metadata.tables[DATA_TABLE_NAME]

    try:
        db.conn.execute(dt.insert(), stream_id=stream, timestamp=ts,
                **kwargs)
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    exp.send((0, ("rrd_muninbytes", stream, ts, exportdict)))

    return 0




# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
