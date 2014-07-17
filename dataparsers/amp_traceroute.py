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
        self.aspaths = {}

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
            {"name":"aspath_id", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"length", "type":"smallint", "null":False},
            {"name":"error_type", "type":"smallint", "null":True},
            {"name":"error_code", "type":"smallint", "null":True},
            {"name":"hop_rtt", "type":"integer[]", "null":False},
        ]
            
        self.dataindexes = []

    def data_table(self):
        err = super(AmpTracerouteParser, self).data_table()
        if err != DB_NO_ERROR:
            logger.log("Failed to create %s data table" % (self.colname))
            return err

        # Create the paths tables as well
        aspathcols = [ \
            {"name":"aspath_id", "type":"serial primary key"},
            {"name":"aspath", "type":"varchar[]", "null":False, "unique":True}
        ]

        err = self.db.create_misc_table("data_amp_traceroute_aspaths", 
                aspathcols)
        if err != DB_NO_ERROR:
            logger.log("Failed to create aspath table for %s" % (self.colname))
            return err

        pathcols = [ \
            {"name":"path_id", "type":"serial primary key"},
            {"name":"path", "type":"inet[]", "null":False, "unique":True}
        ]

        return self.db.create_misc_table("data_amp_traceroute_paths", pathcols)
            

    def create_existing_stream(self, stream_data):
        key = (str(stream_data["source"]), str(stream_data["destination"]),
            str(stream_data["address"]), str(stream_data["packet_size"]))

        self.streams[key] = stream_data["stream_id"]

    def _mangle_result(self, result):
        result["path"] = [x["address"] for x in result["hops"]]
        result["hop_rtt"] = [x["rtt"] for x in result["hops"]]
       
        # TODO implement code to compress AS paths using pseudo-run-length
        # encoding
        result["aspath"] = None 

    def create_new_stream(self, streamparams, timestamp):
        # Because we have the extra paths table, we can't use the generic
        # create_new_stream provided by the NNTSCParser class.
        """ Insert a new traceroute stream into the streams table """

        if 'datastyle' in streamparams:
            streamparams.pop('datastyle')

        while 1:
            errorcode = DB_NO_ERROR
            colid, streamid = self.db.insert_stream(self.streamtable,
                    self.datatable, timestamp, streamparams)
            
            if colid < 0:
                errorcode = streamid

            if streamid < 0:
                errorcode = streamid

            if errorcode == DB_QUERY_TIMEOUT:
                continue
            if errorcode != DB_NO_ERROR:
                return errorcode
            
            errorcode = self.db.clone_table("data_amp_traceroute_aspaths", 
                    streamid)
            if errorcode == DB_QUERY_TIMEOUT:
                continue
            if errorcode != DB_NO_ERROR:
                return errorcode

            errorcode = self.db.clone_table("data_amp_traceroute_paths", 
                    streamid)
            if errorcode == DB_QUERY_TIMEOUT:
                continue
            if errorcode != DB_NO_ERROR:
                return errorcode

            # Ensure our custom foreign keys gets perpetuated
            newtable = "%s_%d" % (self.datatable, streamid)
            pathtable = "data_amp_traceroute_paths_%d" % (streamid)
            aspathtable = "data_amp_traceroute_aspaths_%d" % (streamid)

            errorcode = self.db.add_foreign_key(newtable, "aspath_id", 
                    aspathtable, "aspath_id")
            if errorcode == DB_QUERY_TIMEOUT:
                continue
            if errorcode != DB_NO_ERROR:
                return errorcode

            errorcode = self.db.add_foreign_key(newtable, "path_id", pathtable, 
                    "path_id")
            if errorcode == DB_QUERY_TIMEOUT:
                continue
            if errorcode != DB_NO_ERROR:
                return errorcode
            
            err = self.db.commit_streams()
            if err == DB_QUERY_TIMEOUT:
                continue
            if err != DB_NO_ERROR:
                return err
            break

        if self.exporter == None:
            return streamid

        err, colid = self.db.get_collection_id(self.source, self.module)
        if err != DB_NO_ERROR:
            return err
        if colid == 0:
            return streamid

        self.exporter.publishStream(colid, self.colname, streamid, streamparams)
        
        return streamid

    def _insert_path(self, pathtype, stream, path):
        if pathtype == "ip":
            pathtable = "data_amp_traceroute_paths_%d" % (stream)
            idcolumn = "path_id"
            pathcolumn = "path"
            castterm = "inet[]"
        elif pathtype == "as":
            pathtable = "data_amp_traceroute_aspaths_%d" % (stream)
            idcolumn = "aspath_id"
            pathcolumn = "aspath"
            castterm = "varchar[]"
        else:
            return DB_CODING_ERROR

        pathinsert="WITH s AS (SELECT %s, %s FROM %s WHERE %s " % (idcolumn, \
                pathcolumn, pathtable, pathcolumn)
        pathinsert += "= CAST (%s as inet[])), "
        pathinsert += "i AS (INSERT INTO %s (%s) " % (pathtable, pathcolumn)
        pathinsert += "SELECT CAST(%s as "
        pathinsert += "%s) WHERE NOT EXISTS " % (castterm)
        pathinsert += "(SELECT %s FROM %s WHERE %s " % (pathcolumn, pathtable, \
                pathcolumn)
        pathinsert += "= CAST(%s as inet[])) "
        pathinsert += "RETURNING %s,%s)" % (idcolumn, pathcolumn)
        pathinsert += "SELECT %s,%s FROM i UNION ALL " % (idcolumn, \
                pathcolumn)
        pathinsert += "SELECT %s,%s FROM s" % (idcolumn, pathcolumn)

        params = (path, path, path)
        err, queryret = self.db.custom_insert(pathinsert, params)
         
        if err != DB_NO_ERROR:
            return err

        if queryret == None:
            return DB_DATA_ERROR
            
        return int(queryret[0])


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
            pathid = self._insert_path("ip", stream, result['path'])
            if pathid < 0:
                return err
            
            result['path_id'] = pathid
            self.paths[keystr] = pathid
       
        if result['aspath'] != None:
            keystr = "%s" % (stream)
            for p in result['aspath']:
                keystr += "_%s" % (p)

            if keystr in self.aspaths:
                result['aspath_id'] = self.aspaths[keystr]
            else:
                pathid = self._insert_path("as", stream, result['aspath'])
                if pathid < 0:
                    return err
                
                result['aspath_id'] = pathid
                self.aspaths[keystr] = pathid
       
        # XXX Could almost just call parent insert_data here, except for the
        # line where we have to add an entry for "path" before exporting live
        # data :/
        filtered = {}
        for col in self.datacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        err = self.db.insert_data(self.datatable, self.colname, stream,
                ts, filtered, {'hop_rtt':'integer[]'})

        if err != DB_NO_ERROR:
            return err

        filtered['path'] = result['path']
        filtered['aspath'] = result['aspath']
        if self.exporter != None:
            self.exporter.publishLiveData(self.colname, stream, ts, filtered)

        return DB_NO_ERROR


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
                'packet_size', 'length', 'error_type', 'error_code',
                'aspath', 'aspath_id']:
            sanitised.append(c)

    return sanitised 


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
