from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, Float
from sqlalchemy.exc import IntegrityError, OperationalError

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
        Column('uptime', Float, nullable=True),
        Column('loss', Integer, nullable=True),
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

    return DATA_TABLE_NAME

def insert_stream(db, exp, name, fname, source, host, minres, rows):

    props = {"name":name, "filename":fname, "source":source, "host":host,
            "minres":minres, "highrows":rows}

    colid, streamid = db.register_new_stream("rrd", "smokeping", name)

    if colid == -1:
        return -1

    # insert stream into our stream table
    st = db.metadata.tables[STREAM_TABLE_NAME]
    
    try:
        result = db.conn.execute(st.insert(), stream_id=streamid, 
                filename=fname, source=source,
                host=host, minres=minres, highrows=rows)
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    #id = db.insert_stream(mod="rrd", modsubtype="smokeping", name=name, 
    #        filename=fname, source=source, host=host, minres=minres,
    #        highrows=rows, lasttimestamp=0)

    if streamid >= 0 and exp != None:
        exp.send((1, (colid, "rrd_smokeping", streamid, props)))

    return streamid

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

    dt = db.metadata.tables[DATA_TABLE_NAME]
    
    try:
        db.conn.execute(dt.insert(), stream_id=stream, timestamp=ts,
                **kwargs)
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    exp.send((0, ("rrd_smokeping", stream, ts, exportdict)))

    return 0

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

