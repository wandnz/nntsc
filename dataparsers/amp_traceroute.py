from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import select, join, outerjoin, func, label

STREAM_TABLE_NAME="streams_amp_traceroute"
DATA_VIEW_NAME="data_amp_traceroute"

def stream_table(db):
   
    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME
    
    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        UniqueConstraint('source', 'destination'),
        useexisting=True,
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
        useexisting=True,
    )

    Index('index_amp_traceroute_timestamp', testtable.c.timestamp)

    hoptable = Table("internal_amp_traceroute_hop", db.metadata,
        Column('hop_id', Integer, primary_key=True),
        Column('hop_address', postgresql.INET, nullable=False),
        useexisting=True,
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
        useexisting=True,
    )

    Index('index_amp_traceroute_path_test_id', pathtable.c.test_id)

    # This view is kinda tricky
    
    fullhops = select([pathtable.c.test_id, pathtable.c.path_ttl, 
            hoptable.c.hop_address]).select_from(hoptable.join(pathtable))
    fh = fullhops.alias()

    viewquery = select([testtable.c.stream_id, testtable.c.timestamp, 
            testtable.c.traceroute_test_id, fh.c.path_ttl, 
            fh.c.hop_address]).select_from(testtable.join(fh))

    dataview = db.create_view(DATA_VIEW_NAME, viewquery)

    return DATA_VIEW_NAME

def register(db):
    st_name = stream_table(db)
    dt_name = data_tables(db)

    db.register_collection("amp", "traceroute", st_name, dt_name)

     

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
