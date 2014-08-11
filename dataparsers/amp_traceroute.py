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
from libnntsc.parsers.amp_icmp import AmpIcmpParser

# Inherit from AmpIcmp -- the stream structures are virtually identical so
# we can reuse a lot of code from there.
class AmpTracerouteParser(AmpIcmpParser):
    def __init__(self, db):
        super(AmpTracerouteParser, self).__init__(db)

        self.streamtable = "streams_amp_traceroute"
        self.datatable = "data_amp_traceroute"
        self.colname = "amp_traceroute"
        self.source = "amp"
        self.module = "traceroute"

        self.paths = {}

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"address", "type":"inet", "null":False},
            {"name":"packet_size", "type":"varchar", "null":False},
        ]

        self.uniquecolumns = ['source', 'destination', 'packet_size', 'address']

        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name":"path_id", "type":"integer", "null":False},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"length", "type":"smallint", "null":False},
            {"name":"error_type", "type":"smallint", "null":True},
            {"name":"error_code", "type":"smallint", "null":True},
            {"name":"hop_rtt", "type":"integer[]", "null":False},
        ]
            
        self.dataindexes = []

    def data_table(self):
        super(AmpTracerouteParser, self).data_table()

        # Create the paths table as well
        pathcols = [ \
            {"name":"path_id", "type":"serial primary key"},
            {"name":"path", "type":"inet[]", "null":False, "unique":True}
        ]

        try:
            self.db.create_misc_table("data_amp_traceroute_paths", pathcols)
        except DBQueryException as e:
            logger.log("Failed to create paths table for %s" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise
            

    def create_existing_stream(self, stream_data):
        key = (str(stream_data["source"]), str(stream_data["destination"]),
            str(stream_data["address"]), str(stream_data["packet_size"]))

        self.streams[key] = stream_data["stream_id"]

    def _mangle_result(self, result):
        # Both flags are set to zero, shouldn't really happen but we'll
        # ignore these results just to be safe
        if 'ip' in result and result['ip'] == 0 and 'as' in result and \
                result['as'] == 0:
            return 0
        
        if 'ip' not in result or result['ip'] == 1:
            result["path"] = [x["address"] for x in result["hops"]]
            result["hop_rtt"] = [x["rtt"] for x in result["hops"]]
            return 1
        
        # This is an AS path result, which we don't support
        return 0 

    def create_new_stream(self, streamparams, timestamp):
        # Because we have the extra paths table, we can't use the generic
        # create_new_stream provided by the NNTSCParser class.
        """ Insert a new traceroute stream into the streams table """

        if 'datastyle' in streamparams:
            streamparams.pop('datastyle')

        try:
           streamid = self.db.insert_stream(self.streamtable,
                    self.datatable, timestamp, streamparams)
        except DBQueryException as e:
            logger.log("Failed to insert new stream for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise
            
        try:
            self.db.clone_table("data_amp_traceroute_paths", 
                    streamid)
        except DBQueryException as e:
            logger.log("Failed to clone paths table for new %s stream" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            # Ensure our custom foreign key gets perpetuated
            newtable = "%s_%d" % (self.datatable, streamid)
            pathtable = "data_amp_traceroute_paths_%d" % (streamid)
            errorcode = self.db.add_foreign_key(newtable, "path_id", pathtable, 
                    "path_id")
        except DBQueryException as e:
            logger.log("Failed to add foreign key to paths table for %s" \
                    % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            self.db.commit_streams()
        except DBQueryException as e:
            logger.log("Failed to commit new stream for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        if self.exporter == None:
            return streamid
    
        colid = self._get_collection_id()
        if colid <= 0:
            return streamid

        self.exporter.publishStream(colid, self.colname, streamid, streamparams)
        
        return streamid

    def insert_data(self, stream, ts, result):
        """ Insert data for a single traceroute test into the database """
        # Again, we have to provide our own version of insert_data as we have
        # the extra paths table to deal with

        keystr = "%s" % (stream)
        for p in result['path']:
            keystr += "_%s" % (p)

        if keystr in self.paths:
            result['path_id'] = self.paths[keystr]
        else:
            pathtable = "data_amp_traceroute_paths_%d" % (stream)
            pathinsert="WITH s AS (SELECT path_id, path FROM %s " % (pathtable)
            pathinsert += "WHERE path = CAST (%s as inet[])), "
            pathinsert += "i AS (INSERT INTO %s (path) " % (pathtable)
            pathinsert += "SELECT CAST(%s as inet[]) WHERE NOT EXISTS "
            pathinsert += "(SELECT path FROM %s " % (pathtable)
            pathinsert += "WHERE path = CAST(%s as inet[])) RETURNING path_id,path)"
            pathinsert += "SELECT path_id, path FROM i UNION ALL "
            pathinsert += "SELECT path_id, path FROM s"

            params = (result['path'], result['path'], result["path"])
            try:
                queryret = self.db.custom_insert(pathinsert, params)
            except DBQueryException as e:
                logger.log("Failed to get path_id for %s test result (stream %d)" % \
                        (self.colname, stream))
                logger.log("Error was: %s" % (str(e)))
                raise
             
            if queryret == None:
                logger.log("No query result when getting path_id for %s (stream %d)" % \
                        (self.colname, stream))
                raise DBQueryException(DB_DATA_ERROR)
                    
            result['path_id'] = queryret[0]
            self.paths[keystr] = queryret[0]
       
        # XXX Could almost just call parent insert_data here, except for the
        # line where we have to add an entry for "path" before exporting live
        # data :/
        filtered = {}
        for col in self.datacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        try:
            self.db.insert_data(self.datatable, self.colname, stream,
                ts, filtered, {'hop_rtt':'integer[]'})
        except DBQueryException as e:
            logger.log("Failed to insert new data for %s stream %d" % \
                    (self.colname, stream))
            logger.log("Error was: %s" % (str(e)))
            raise

        filtered['path'] = result['path']
        if self.exporter != None:
            self.exporter.publishLiveData(self.colname, stream, ts, filtered)


# Helper functions for dbselect module which deal with complications
# arising from the dual data tables.
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
