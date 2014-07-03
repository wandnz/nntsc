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


from libnntsc.dberrorcodes import *
import libnntscclient.logger as logger

STREAM_TABLE_NAME = "streams_amp_traceroute"
DATA_TABLE_NAME = "data_amp_traceroute"

amp_trace_streams = {}
amp_trace_paths = {}

traceroute_datacols = [ \
    {"name":"path_id", "type":"integer", "null":False},
    {"name":"packet_size", "type":"smallint", "null":False},
    {"name":"length", "type":"smallint", "null":False},
    {"name":"error_type", "type":"smallint", "null":True},
    {"name":"error_code", "type":"smallint", "null":True},
    {"name":"hop_rtt", "type":"integer[]", "null":False},
]

def stream_table(db):
    """ Specify the description of a traceroute stream, to create the table """


    streamcols = [ \
        {"name":"source", "type":"varchar", "null":False},
        {"name":"destination", "type":"varchar", "null":False},
        {"name":"address", "type":"inet", "null":False},
        {"name":"packet_size", "type":"varchar", "null":False},
    ]

    uniqcols = ['source', 'destination', 'packet_size', 'address']

    err = db.create_streams_table(STREAM_TABLE_NAME, streamcols, uniqcols)
    if err != DB_NO_ERROR:
        logger.log("Failed to create streams table for amp-icmp")
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['source'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create source index on %s" % (STREAM_TABLE_NAME))
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['destination'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create dest index on %s" % (STREAM_TABLE_NAME))
        return None


    return STREAM_TABLE_NAME

def data_table(db):
    """ Specify the description of traceroute data, used to create the table """

    err = db.create_data_table(DATA_TABLE_NAME, traceroute_datacols)
    if err != DB_NO_ERROR:
        return None

    pathcols = [ \
        {"name":"path_id", "type":"serial primary key"},
        {"name":"path", "type":"inet[]", "null":False, "unique":True}
    ]

    err = db.create_misc_table("data_amp_traceroute_paths", pathcols)
    if err != DB_NO_ERROR:
        return None
        

    return DATA_TABLE_NAME


def register(db):
    st_name = stream_table(db)
    dt_name = data_table(db)

    if st_name is None or dt_name is None:
        logger.log("Failed to create AMP traceroute base tables")
        return DB_CODING_ERROR

    return db.register_collection("amp", "traceroute", st_name, dt_name)


def create_existing_stream(stream_data):
    """ Extract the stream key from the stream data provided by NNTSC
        when the AMP module is first instantiated.
    """

    key = (str(stream_data["source"]), str(stream_data["destination"]),
        str(stream_data["address"]), str(stream_data["packet_size"]))

    amp_trace_streams[key] = stream_data["stream_id"]


def insert_stream(db, exp, source, dest, size, address, timestamp):
    """ Insert a new traceroute stream into the streams table """

    props = {"source":source, "destination":dest,
            "packet_size":size, "address": address}

    while 1:
        errorcode = DB_NO_ERROR
        colid, streamid = db.insert_stream(STREAM_TABLE_NAME, DATA_TABLE_NAME, 
                timestamp, props)
        
        if colid < 0:
            errorcode = streamid

        if streamid < 0:
            errorcode = streamid

        if errorcode == DB_QUERY_TIMEOUT:
            continue
        if errorcode != DB_NO_ERROR:
            return errorcode

        errorcode = db.clone_table("data_amp_traceroute_paths", streamid)
        if errorcode == DB_QUERY_TIMEOUT:
            continue
        if errorcode != DB_NO_ERROR:
            return errorcode

        # Ensure our custom foreign key gets perpetuated
        newtable = "%s_%d" % (DATA_TABLE_NAME, streamid)
        pathtable = "data_amp_traceroute_paths_%d" % (streamid)
        errorcode = db.add_foreign_key(newtable, "path_id", pathtable, "path_id")
        if errorcode == DB_QUERY_TIMEOUT:
            continue
        if errorcode != DB_NO_ERROR:
            return errorcode
        
        err = db.commit_streams()
        if err == DB_QUERY_TIMEOUT:
            continue
        if err != DB_NO_ERROR:
            return err
        break

    if exp == None:
        return streamid

    exp.publishStream(colid, "amp_traceroute", streamid, props)
    
    return streamid

def insert_data(db, exp, stream, ts, result):
    """ Insert data for a single traceroute test into the database """

    # sqlalchemy is again totally useless and makes it impossible to cast
    # types on insert, so lets do it ourselves.

    keystr = "%s" % (stream)
    for p in result['path']:
        keystr += "_%s" % (p)

    if keystr in amp_trace_paths:
        result['path_id'] = amp_trace_paths[keystr]
    else:

        pathtable = "data_amp_traceroute_paths_%d" % (stream)

        pathinsert = "WITH s AS (SELECT path_id, path FROM %s " % (pathtable)
        pathinsert += "WHERE path = CAST (%s as inet[])), "
        pathinsert += "i AS (INSERT INTO %s (path) " % (pathtable)
        pathinsert += "SELECT CAST(%s as inet[]) WHERE NOT EXISTS "
        pathinsert += "(SELECT path FROM %s " % (pathtable)
        pathinsert += "WHERE path = CAST(%s as inet[])) RETURNING path_id,path)"
        pathinsert += "SELECT path_id, path FROM i UNION ALL "
        pathinsert += "SELECT path_id, path FROM s"

        params = (result['path'], result['path'], result["path"])
        err, queryret = db.custom_insert(pathinsert, params)
         
        if err != DB_NO_ERROR:
            return err

        if queryret == None:
            return DB_DATA_ERROR
                
        result['path_id'] = queryret[0]
        amp_trace_paths[keystr] = queryret[0]
   
    filtered = {}
    for col in traceroute_datacols:
        if col["name"] in result:
            filtered[col["name"]] = result[col["name"]]
        else:
            filtered[col["name"]] = None

    err = db.insert_data(DATA_TABLE_NAME, "amp_traceroute", stream,
            ts, filtered, {'hop_rtt':'integer[]'})

    if err != DB_NO_ERROR:
        return err

    filtered['path'] = result['path']
    if exp != None:
        exp.publishLiveData("amp_traceroute", stream, ts, filtered)

    return DB_NO_ERROR


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
    return db.update_timestamp(DATA_TABLE_NAME, done.keys(), timestamp)

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
