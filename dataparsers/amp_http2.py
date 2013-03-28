from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import select, join, outerjoin, func, label

STREAM_TABLE_NAME="streams_amp_http2"
DATA_VIEW_NAME="data_amp_http2"

def stream_table(db):

    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata, 
        Column('stream_id', Integer, ForeignKey("streams.id"),
                primary_key=True),
        Column('source', String, nullable=False),
        Column('url', String, nullable=False),
        Column('persist', Boolean, nullable=False),
        Column('max_connections', Integer, nullable=False),
        Column('max_connections_per_server', Integer, nullable=False),
        Column('max_persistent_connections_per_server', Integer, nullable=False),
        Column('pipelining', Boolean, nullable=False),
        Column('pipelining_max_requests', Integer, nullable=False),
        Column('caching', Boolean, nullable=False),
        UniqueConstraint('source', 'url', 'persist', 'max_connections',
                'max_connections_per_server', 
                'max_persistent_connections_per_server', 'pipelining',
                'pipelining_max_requests', 'caching'),
        extend_existing=True,
    )

    Index('index_amp_http2_source', st.c.source)
    Index('index_amp_http2_url', st.c.url)

    return STREAM_TABLE_NAME

def data_tables(db):

    if DATA_VIEW_NAME in db.metadata.tables:
        return DATA_VIEW_NAME

    testtable = Table('internal_amp_http2_test', db.metadata, 
        Column('test_id', Integer, primary_key=True),
        Column('stream_id', Integer, 
            ForeignKey('streams.id', ondelete="CASCADE"),
            nullable=False),
        Column('test_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('test_servercount', Integer, nullable=False),
        Column('test_objectcount', Integer, nullable=False),
        Column('test_duration', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('test_bytecount', Integer, nullable=False),
        UniqueConstraint('stream_id', 'test_starttime'),
        extend_existing=True,
    )

    Index('index_amp_http2_teststart', testtable.c.test_starttime)
    Index('index_amp_http2_teststream', testtable.c.stream_id)


    servtable = Table('internal_amp_http2_servers', db.metadata,
        Column('server_id', Integer, primary_key=True),
        Column('test_id', Integer,
            ForeignKey('internal_amp_http2_test.test_id', ondelete="CASCADE"),
            nullable=False),
        Column('server_index', Integer, nullable=False),
        Column('server_name', String, nullable=False),
        Column('server_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('server_endtime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('server_bytecount', Integer, nullable=False),
        Column('server_objectcount', Integer, nullable=False),
        UniqueConstraint('test_id', 'server_index', 'server_name'),
        extend_existing=True,
    )
    
    Index('index_amp_http2_servertest', servtable.c.test_id)

    objtable = Table('internal_amp_http2_objects', db.metadata,
        Column('object_id', Integer, primary_key=True),
        Column('test_id', Integer, 
            ForeignKey('internal_amp_http2_test.test_id', ondelete="CASCADE"),
            nullable=False),
        Column('server_id', Integer, 
            ForeignKey('internal_amp_http2_servers.server_id', 
                ondelete="CASCADE"), nullable=False),
        Column('path', String, nullable=False),
        Column('code', Integer, nullable=False),
        Column('obj_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_endtime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_totaltime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_size', Integer, nullable=False),
        Column('numconnects', Integer, nullable=False),
        Column('public', Boolean, nullable=False),
        Column('private', Boolean, nullable=False),
        Column('nocache', Boolean, nullable=False),
        Column('nostore', Boolean, nullable=False),
        Column('notransform', Boolean, nullable=False),
        Column('mustrevalidate', Boolean, nullable=False),
        Column('proxyrevalidate', Boolean, nullable=False),
        Column('maxage', Integer, nullable=False),
        Column('s_maxage', Integer, nullable=False),
        Column('x_cache', Integer, nullable=False),
        Column('x_cache_lookup', Integer, nullable=False),
        extend_existing=True,
    )

    #Index('index_amp_http2_objecttest', objtable.c.test_id)
    Index('index_amp_http2_objectserver', objtable.c.server_id)

    fullobjs = servtable.join(objtable).select().\
            where(servtable.c.test_id == objtable.c.test_id).reduce_columns()
    fo = fullobjs.alias()

    viewquery = testtable.join(fo).select().reduce_columns()
    dataview = db.create_view(DATA_VIEW_NAME, viewquery)

    return DATA_VIEW_NAME

def register(db):
    st_name = stream_table(db)
    dt_name = data_tables(db)

    db.register_collection("amp", "http2", st_name, dt_name)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :		

