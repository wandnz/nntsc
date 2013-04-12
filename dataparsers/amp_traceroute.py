from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import select, join, outerjoin, func, label

STREAM_TABLE_NAME="streams_amp_traceroute"
DATA_VIEW_NAME="data_amp_traceroute"
HOP_TABLE_NAME="internal_amp_traceroute_hop"

amp_trace_streams = {}

def stream_table(db):

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packet_size', String, nullable=False),
        UniqueConstraint('source', 'destination', 'packet_size'),
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
        Column('address', postgresql.INET, nullable=False),
        Column('length', Integer, nullable=False),
        Column('error_type', Integer, nullable=False),
        Column('error_code', Integer, nullable=False),
        Column('packet_size', Integer, nullable=False),
        extend_existing=True,
    )

    Index('index_amp_traceroute_timestamp', testtable.c.timestamp)

    hoptable = Table("internal_amp_traceroute_hop", db.metadata,
        Column('hop_id', Integer, primary_key=True),
        Column('hop_address', postgresql.INET, nullable=False),
        UniqueConstraint('hop_address'),
        extend_existing=True,
    )

    Index('index_amp_traceroute_hop_address', hoptable.c.hop_address)

    pathtable = Table("internal_amp_traceroute_path", db.metadata,
        Column('test_id', Integer,
                ForeignKey("internal_amp_traceroute_test.traceroute_test_id",
                    ondelete="CASCADE")),
        Column('hop_id', Integer,
                ForeignKey("internal_amp_traceroute_hop.hop_id",
                    ondelete="CASCADE")),
        Column('path_ttl', Integer, nullable=False),
        Column('hop_rtt', Integer, nullable=True),
        extend_existing=True,
    )

    Index('index_amp_traceroute_path_test_id', pathtable.c.test_id)

    # This view is kinda tricky
    fullhops = select([pathtable.c.test_id, pathtable.c.path_ttl,
            pathtable.c.hop_rtt,
            hoptable.c.hop_address]).select_from(hoptable.join(pathtable))
    fh = fullhops.alias()

    viewquery = select([testtable.c.stream_id, testtable.c.timestamp,
            testtable.c.traceroute_test_id,
            testtable.c.address.label("target_address"),
            testtable.c.length, testtable.c.error_type, 
            testtable.c.error_code, testtable.c.packet_size, 
            fh.c.path_ttl,
            fh.c.hop_address]).select_from(testtable.join(fh))

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
        str(stream_data["packet_size"]))

    amp_trace_streams[key] = stream_data["stream_id"]


def data_stream_key(data, source):
    """ Extract the stream key from the data received from the AMP
        message broker.
    """

    return (source, data["target"], sizestr)


def insert_stream(db, exp, source, dest, size):
    """ Insert a new traceroute stream into the streams table """

    name = "traceroute %s:%s:%s" % (source, dest, size)

    props = {"name":name, "source":source, "destination":dest,
            "packet_size":size, "datastyle":"traceroute"}

    colid, streamid = db.register_new_stream("amp", "traceroute", name)

    if colid == -1:
        return -1

    # insert stream into our stream table
    st = db.metadata.tables[STREAM_TABLE_NAME]

    try:
        result = db.conn.execute(st.insert(), stream_id=streamid,
                source=source, destination=dest, packet_size=size,
                datastyle="traceroute")
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    if streamid >= 0 and exp != None:
        exp.send((1, (colid, "amp_traceroute", streamid, props)))

    return streamid


def get_or_create_hop_id(db, address):
    """ Get the hop id for a hop if it exists, otherwise create it """

    hop_table = db.metadata.tables[HOP_TABLE_NAME]
    try:
        item = db.conn.execute(hop_table.select().where(
                    hop_table.c.hop_address==address))
        # hop exists with this address, return the hop id
        if item.rowcount == 1:
            return item.fetchone()["hop_id"]

        # hop with this address doesn't exist, create it
        item = db.conn.execute(hop_table.insert(), hop_address=address)
        return item.inserted_primary_key[0]
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1


def insert_data(db, exp, stream, ts, test_info, hop_info):
    """ Insert data for a single traceroute test into the database """

    # information linking a test run together with the hops visited
    path_table = db.metadata.tables["internal_amp_traceroute_path"]

    # insert test information for this particular test run
    dt = db.metadata.tables["internal_amp_traceroute_test"]
    try:
        test = db.conn.execute(dt.insert(), stream_id=stream, timestamp=ts,
                **test_info)
        test_id = test.inserted_primary_key[0]
    except IntegrityError, e:
        db.rollback_transaction()
        print >> sys.stderr, e
        return -1

    # insert each hop along the path
    ttl = 1
    for hop in hop_info:
        # create hop if not already present and get hop id
        hop_id = get_or_create_hop_id(db, hop["address"])
        if hop_id < 0:
            return -1;

        # link this point in the path to this hop
        try:
            db.conn.execute(path_table.insert(), stream_id=stream, timestamp=ts,
                    test_id=test_id, hop_id=hop_id, path_ttl=ttl,
                    hop_rtt=hop["rtt"])
        except IntegrityError, e:
            db.rollback_transaction()
            print >> sys.stderr, e
            return -1
        ttl += 1

    # make the result pushed here look like the data view
    data_view = db.metadata.tables[DATA_VIEW_NAME]
    result = db.conn.execute(data_view.select().where(
                data_view.c.traceroute_test_id==test_id))
    if result.rowcount < 1:
        return -1

    for row in result:
        data = {}
        for k,v in row.items():
            data[k] = v
        exp.send((0, ("amp_traceroute", stream, ts, data)))

    return 0

def process_data(db, exp, timestamp, data, source):

    # For each path returned in the test data
    for d in data:
        if d["random"]:
            sizestr = "random"
        else:
            sizestr = str(d["packet_size"])

        d["source"] = source
        key = (source, d["target"], sizestr)

        if key in amp_trace_streams:
            stream_id = amp_trace_streams[key]
        else:
            stream_id = insert_stream(db, exp, source, d["target"], sizestr)

            if stream_id == -1:
                print >> sys.stderr, "AMPModule: Cannot create stream for:"
                print >> sys.stderr, "AMPModule: %s %s:%s:%s\n" % (
                        "traceroute", source, d["target"], sizestr)
                return -1
            else:
                amp_trace_streams[key] = stream_id

        test_data = {
            "source": source,
            "target": d["target"],
            "address": d["address"],
            "length": d["length"],
            "error_type": d["error_type"],
            "error_code": d["error_code"],
            "packet_size": d["packet_size"],
        }

        insert_data(db, exp, stream_id, timestamp, test_data, d["hops"])
        db.update_timestamp(stream_id, timestamp)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
