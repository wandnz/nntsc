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
from sqlalchemy.types import Integer, String, Float, Boolean
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import select, join, outerjoin, func, label, and_
from sqlalchemy.sql import text

from libnntsc.partition import PartitionedTable
import libnntscclient.logger as logger
from itertools import chain
import sys, string

STREAM_TABLE_NAME="streams_amp_http"
DATA_VIEW_NAME="data_amp_http"
TEST_TABLE_NAME="internal_amp_http_test"
SERVER_TABLE_NAME="internal_amp_http_servers"
OBJ_TABLE_NAME="internal_amp_http_objects"

amp_http_streams = {}
test_partitions = None
server_partitions = None
obj_partitions = None

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

    Index('index_amp_http_source', st.c.source)
    Index('index_amp_http_url', st.c.url)

    return STREAM_TABLE_NAME

def data_tables(db):

    if DATA_VIEW_NAME in db.metadata.tables:
        return DATA_VIEW_NAME

    testtable = Table(TEST_TABLE_NAME, db.metadata, 
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

    #Index('index_amp_http_teststart', testtable.c.test_starttime)
    #Index('index_amp_http_teststream', testtable.c.stream_id)


    servtable = Table(SERVER_TABLE_NAME, db.metadata,
        Column('server_id', Integer, primary_key=True),
        Column('test_id', Integer,
            ForeignKey(TEST_TABLE_NAME + '.test_id', ondelete="CASCADE"),
            nullable=False),
        Column('test_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('server_index', Integer, nullable=False),
        Column('server_name', String, nullable=False),
        Column('server_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('server_endtime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('server_bytecount', Integer, nullable=False),
        Column('server_objectcount', Integer, nullable=False),
        UniqueConstraint('test_id', 'server_index', 'server_name'),
        extend_existing=True,
    )
    
    #Index('index_amp_http_servertest', servtable.c.test_id)

    objtable = Table(OBJ_TABLE_NAME, db.metadata,
        Column('object_id', Integer, primary_key=True),
        Column('test_id', Integer, 
            ForeignKey(TEST_TABLE_NAME + '.test_id', ondelete="CASCADE"),
            nullable=False),
        Column('server_id', Integer, 
            ForeignKey(SERVER_TABLE_NAME + '.server_id', 
                ondelete="CASCADE"), nullable=False),
        Column('test_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('path', String, nullable=False),
        Column('code', Integer, nullable=False),
        Column('obj_starttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_endtime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_lookuptime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_connecttime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_starttransfertime', postgresql.DOUBLE_PRECISION, 
                nullable=False),
        Column('obj_totaltime', postgresql.DOUBLE_PRECISION, nullable=False),
        Column('obj_size', Integer, nullable=False),
        Column('numconnects', Integer, nullable=False),
        Column('pipeline', Integer, nullable=False),
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

    #Index('index_amp_http_objecttest', objtable.c.test_id)
    #Index('index_amp_http_objectserver', objtable.c.server_id)

    fullobjs = servtable.join(objtable).select().\
            where(and_(servtable.c.test_id == objtable.c.test_id, \
            servtable.c.test_starttime == objtable.c.test_starttime)) \
            .reduce_columns()
    fo = fullobjs.alias()

    viewquery = testtable.join(fo).select().where( \
            fo.c.test_starttime == testtable.c.test_starttime).reduce_columns()
    dataview = db.create_view(DATA_VIEW_NAME, viewquery)

    return DATA_VIEW_NAME

def register(db):
    st_name = stream_table(db)
    dt_name = data_tables(db)

    db.register_collection("amp", "http", st_name, dt_name)


def insert_stream(db, exp, source, data, timestamp):
    name = "http %s:%s" % (source, data['url'])

    props = {"name": name, "source":source, "url":data['url'],
            "persist":data['keep_alive'], 
            "max_connections":data['max_connections'],
            "max_connections_per_server":data['max_connections_per_server'],
            "max_persistent_connections_per_server":data['max_persistent_connections_per_server'],
            "pipelining":data['pipelining'],
            "pipelining_max_requests":data['pipelining_maxrequests'],
            "caching":data['caching']}

    colid, streamid = db.register_new_stream("amp", "http", name, timestamp)

    if colid == -1:
        return -1

    st = db.metadata.tables[STREAM_TABLE_NAME]
    try:
        result = db.conn.execute(st.insert(), stream_id=streamid, **props)
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1

    exp.send((1, (colid, "amp_http", streamid, props)))
    return streamid


def insert_test(db, stream, timestamp, data):
    global test_partitions

    dt = db.metadata.tables[TEST_TABLE_NAME]

    if test_partitions == None:
        test_partitions = PartitionedTable(db, TEST_TABLE_NAME, 
                60 * 60 * 24 * 7, ["test_starttime", "stream_id", "test_id"],
                "test_starttime")

    test_partitions.update(timestamp)

    test_info = {"stream_id":stream, "test_starttime":timestamp, \
            "test_servercount":data['server_count'], \
            "test_objectcount":data['object_count'], \
            "test_duration":data['duration'], \
            "test_bytecount":data['bytes']}

    try:
        query = dt.insert().values(**test_info)
        db.conn.execute(query)

        # Find the id of what we just inserted. I know this is generally not
        # the right way to do this, but we can't just RETURN it due to table
        # partitioning (see amp_traceroute for more detailed explanation).

        query = text("""SELECT max(test_id) FROM %s WHERE stream_id=:id AND
                test_starttime=:start AND test_servercount=:servers AND
                test_objectcount=:objects AND test_duration=:duration AND
                test_bytecount=:bytes;""" % (TEST_TABLE_NAME))
        
        
        result = db.conn.execute(query, id=stream, start=timestamp,
                servers=data['server_count'], objects=data['object_count'],
                duration=data['duration'], bytes=data['bytes'])
        assert(result.rowcount == 1)
        row = result.fetchone()
        test_id = row[0]
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1, None
    db.commit_transaction()

    return test_id, test_info

def insert_server(db, stream, timestamp, testid, server, index):
    global server_partitions

    dt = db.metadata.tables[SERVER_TABLE_NAME]
    if server_partitions == None:
        server_partitions = PartitionedTable(db, SERVER_TABLE_NAME,
                60 * 60 * 24 * 7, ["test_starttime", "test_id", "server_id"],
                "test_starttime")
    server_partitions.update(timestamp)

    server_info = {"test_id":testid, "server_index":index, \
            "test_starttime": timestamp, \
            "server_name": server["hostname"], \
            "server_starttime": server["start"], \
            "server_endtime": server["end"], \
            "server_bytecount": server["bytes"], \
            "server_objectcount": server["object_count"]}

    try:
        db.conn.execute(dt.insert(), **server_info);

        # Get the id of the server we just inserted. I know this is not
        # the normal way to do this, but we can't just RETURN it due to table
        # partitioning (see amp_traceroute for more detailed explanation).

        # This should be unique enough without having to check the other
        # columns...
        query = text("""SELECT max(server_id) FROM %s WHERE test_id=:testid AND
                server_index=:index;""" % (SERVER_TABLE_NAME))
        result = db.conn.execute(query, index=index, testid=testid)
        assert(result.rowcount == 1)
        row = result.fetchone()
        server_id = row[0]
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1, None
    db.commit_transaction()

    return server_id, server_info


def insert_object(db, stream, timestamp, testid, serverid, obj):
    global obj_partitions

    dt = db.metadata.tables[OBJ_TABLE_NAME]
    if obj_partitions == None:
        obj_partitions = PartitionedTable(db, OBJ_TABLE_NAME,
                60 * 60 * 24 * 7, ["test_starttime", "test_id", "server_id", \
                "object_id"],
                "test_starttime")
    obj_partitions.update(timestamp)

    obj_info = {"test_id": testid, "server_id":serverid, \
            "test_starttime":timestamp, "path":obj["path"], \
            "code": obj["code"], "obj_starttime":obj["start"], \
            "obj_endtime":obj["end"], "obj_totaltime":obj["total_time"], \
            "obj_lookuptime":obj["lookup_time"], \
            "obj_connecttime":obj["connect_time"], \
            "obj_starttransfertime":obj["start_transfer_time"], \
            "pipeline":obj["pipeline"], "obj_size":obj["bytes"], \
            "numconnects":obj["connect_count"], \
            "public":obj["headers"]["flags"]["pub"], \
            "private":obj["headers"]["flags"]["priv"], \
            "nocache":obj["headers"]["flags"]["no_cache"], \
            "nostore":obj["headers"]["flags"]["no_store"], \
            "notransform":obj["headers"]["flags"]["no_transform"], \
            "mustrevalidate":obj["headers"]["flags"]["must_revalidate"], \
            "proxyrevalidate":obj["headers"]["flags"]["proxy_revalidate"], \
            "maxage":obj["headers"]["max_age"], \
            "s_maxage":obj["headers"]["s_maxage"], \
            "x_cache":obj["headers"]["x_cache"], \
            "x_cache_lookup":obj["headers"]["x_cache_lookup"],
    }

    try:
        db.conn.execute(dt.insert(), **obj_info);

        # Get the id of the object we just inserted. I know this is not
        # the normal way to do this, but we can't just RETURN it due to table
        # partitioning (see amp_traceroute for more detailed explanation).

        # This should be unique enough without having to check the other
        # columns...
        query = text("""SELECT max(object_id) FROM %s WHERE test_id=:testid AND
                server_id=:serverid AND path=:path AND obj_starttime=:start AND
                obj_endtime=:end AND code=:code;""" % \
                (OBJ_TABLE_NAME))
        result = db.conn.execute(query, testid=testid, serverid=serverid,
                start=obj['start'], end=obj['end'], code=obj['code'],
                path=obj['path'])
        assert(result.rowcount == 1)
        row = result.fetchone()
        object_id = row[0]
    except IntegrityError, e:
        db.rollback_transaction()
        logger.log(e)
        return -1, None
    db.commit_transaction()

    return object_id, obj_info

def export_http_row(exp, stream, ts, objid, testinfo, serverinfo, objinfo):

    # Construct a dictionary-equivalent of a row from the data view using
    # the rows we added to the internal tables, so we can export it to 
    # anyone wanting live data.

    # Merge the three info dicts, removing duplicate keys
    dicts = [testinfo, serverinfo, objinfo]
    merged = dict(chain(*[d.iteritems() for d in dicts]))

    # Add in elements that aren't in one of the three info dicts
    merged['object_id'] = objid

    exp.send((0, ("amp_http", stream, ts, merged)))
    return 0


def create_existing_stream(data):
    key = (str(data['source']), str(data['url']), data['persist'], \
            str(data['max_connections']), \
            str(data['max_connections_per_server']), \
            str(data['max_persistent_connections_per_server']), \
            data['pipelining'], \
            str(data['pipelining_max_requests']), \
            data['caching'])
   
    print "Creating existing:", key 
    amp_http_streams[key] = data['stream_id'] 

def process_data(db, exp, timestamp, data, source):

    streamkey = (source, data['url'], data['keep_alive'], \
            str(data['max_connections']), \
            str(data['max_connections_per_server']), \
            str(data['max_persistent_connections_per_server']), \
            data['pipelining'], str(data['pipelining_maxrequests']), \
            data['caching'])

    if streamkey in amp_http_streams:
        stream_id = amp_http_streams[streamkey]
    else:
        stream_id = insert_stream(db, exp, source, data, timestamp)
        if stream_id == -1:
            logger.log("AMPModule: Cannot create stream for:")
            logger.log("AMPModule: %s %s %s \n" % ("http", source,    
                    data['url']))
            return -1
        amp_http_streams[streamkey] = stream_id

    test_id, test_info = insert_test(db, stream_id, timestamp, data)
    if test_id == -1:
        logger.log("AMPModule: Cannot create HTTP test row for stream %d\n" \
                % (stream_id))
        return -1

    servindex = 0
    for serv in data['servers']:
        server_id, server_info = insert_server(db, stream_id, timestamp, \
                test_id, serv, servindex)

        if server_id == -1:
            logger.log("AMPModule: Cannot create HTTP server for stream %d\n" \
                    % (stream_id))
            return -1

        for obj in serv['objects']:
            obj_id, obj_info = insert_object(db, stream_id, timestamp, \
                    test_id, server_id, obj)
            if obj_id == -1:
                logger.log("AMPModule: Cannot create HTTP object for %d\n" \
                        % (stream_id))
                return -1
            export_http_row(exp, stream_id, timestamp, obj_id, test_info, \
                    server_info, obj_info)

        servindex += 1
    db.update_timestamp(stream_id, timestamp)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :		

