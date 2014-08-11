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

class AmpTracerouteParser(AmpIcmpParser):
    def __init__(self, db):
        super(AmpTracerouteParser, self).__init__(db)

        self.streamtable = "streams_amp_traceroute"
        self.ipdatatable = "data_amp_traceroute"
        self.asdatatable = "data_amp_astraceroute"
        self.colname = "amp_traceroute"
        self.source = "amp"
        self.module = "traceroute"

        self.paths = {}
        self.aspaths = {}

        self.ipdatacolumns = [
            {"name":"path_id", "type":"integer", "null":False},
            {"name":"aspath_id", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"length", "type":"smallint", "null":False},
            {"name":"error_type", "type":"smallint", "null":True},
            {"name":"error_code", "type":"smallint", "null":True},
            {"name":"hop_rtt", "type":"integer[]", "null":False},
        ]

        self.asdatacolumns = [
            {"name":"aspath_id", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"length", "type":"smallint", "null":False},
            {"name":"errors", "type":"smallint", "null":False},
        ]

    

    def data_table(self):
        self.db.create_data_table(self.asdatatable, self.asdatacolumns)
        self._create_indexes(self.asdatatable, [])

        self.db.create_data_table(self.ipdatatable, self.ipdatacolumns)
        self._create_indexes(self.ipdatatable, [])

        self.db.commit_streams()

        # Create the paths tables as well
        aspathcols = [ \
            {"name":"aspath_id", "type":"serial primary key"},
            {"name":"aspath", "type":"varchar[]", "null":False, "unique":True}
        ]

        try:
            self.db.create_misc_table("data_amp_traceroute_aspaths", 
                    aspathcols)
        except DBQueryException as e:
            logger.log("Failed to create aspaths table for %s" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

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
            
    def register(self):
        # Very similar to the parent class, but we have to register two
        # collections instead of just one.
        try:
            self.stream_table()
        except DBQueryException as e:
            logger.log("Failed to create streams table for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            self.data_table()
        except DBQueryException as e:
            logger.log("Failed to create data table for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            self.db.register_collection(self.source, "astraceroute",
                self.streamtable, self.asdatatable)
        except DBQueryException as e:
            logger.log("Failed to register new collection %s in database" % \
                    ("astraceroute"))
            logger.log("Error was: %s" % (str(e)))
            raise 
            
        try:
            self.db.register_collection(self.source, "traceroute",
                self.streamtable, self.ipdatatable)
        except DBQueryException as e:
            logger.log("Failed to register new collection %s in database" % \
                    ("traceroute"))
            logger.log("Error was: %s" % (str(e)))
            raise 

    def create_new_stream(self, streamparams, timestamp):
        # Because we have the extra paths table, we can't use the generic
        # create_new_stream provided by the NNTSCParser class.
        """ Insert a new traceroute stream into the streams table """

        # This will automatically clone the ipdatatable
        try:
           streamid = self.db.insert_stream(self.streamtable,
                    self.ipdatatable, timestamp, streamparams)
        except DBQueryException as e:
            logger.log("Failed to insert new stream for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise
      
        # Manually clone the AS data table 
        try:
            self.db.clone_table("data_amp_astraceroute", streamid)
        except DBQueryException as e:
            logger.log("Failed to clone AS data table for new %s stream" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise
        
        # Don't forget to set the 'first' timestamp (just like insert_stream
        # does).
        self.db.update_timestamp("data_amp_astraceroute", [streamid], 
                timestamp, timestamp) 

        try:
            self.db.clone_table("data_amp_traceroute_aspaths", 
                    streamid)
        except DBQueryException as e:
            logger.log("Failed to clone aspaths table for new %s stream" % \
                    (self.colname))
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
            # Ensure our custom foreign keys gets perpetuated
            ipdatatable = "%s_%d" % (self.ipdatatable, streamid)
            asdatatable = "%s_%d" % (self.asdatatable, streamid)
            pathtable = "data_amp_traceroute_paths_%d" % (streamid)
            aspathtable = "data_amp_traceroute_aspaths_%d" % (streamid)

            self.db.add_foreign_key(asdatatable, "aspath_id", aspathtable, 
                    "aspath_id")
            self.db.add_foreign_key(ipdatatable, "aspath_id", aspathtable, 
                    "aspath_id")
            self.db.add_foreign_key(ipdatatable, "path_id", pathtable, 
                    "path_id")
        except DBQueryException as e:
            logger.log("Failed to add foreign key to paths tables for %s" \
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
    
        colid = self._get_collection_id("traceroute")
        if colid <= 0:
            return streamid

        self.exporter.publishStream(colid, "amp-traceroute", streamid, 
                streamparams)
        
        colid = self._get_collection_id("astraceroute")
        if colid <= 0:
            return streamid

        self.exporter.publishStream(colid, "amp-astraceroute", streamid, 
                streamparams)
        
        return streamid

    def _get_collection_id(self, module):
        if self.collectionid == None:
            try:
                colid = self.db.get_collection_id(self.source, module)
            except DBQueryException as e:
                logger.log("Failed to get collection id for %s %s" % \
                        (self.colname))
                logger.log("Error was: %s" % (str(e)))
                raise

            if colid > 0:
                self.collectionid = colid
            else:
                return -1

        return self.collectionid


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

        try:
            queryret = self.db.custom_insert(pathinsert, params)
        except DBQueryException as e:
            logger.log("Failed to get path_id for %s test result (stream %d)" \
                    % (self.colname, stream))
            logger.log("Error was: %s" % (str(e)))
            raise
         
        if queryret == None:
            logger.log("No query result when getting path_id for %s (stream %d)" % \
                    (self.colname, stream))
            raise DBQueryException(DB_DATA_ERROR)
     
        return int(queryret[0])


    def insert_ippath(self, stream, ts, result):
        """ Insert data for a single traceroute path into the database """

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
        for col in self.ipdatacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        try:
            self.db.insert_data(self.ipdatatable, self.colname, stream,
                ts, filtered, {'hop_rtt':'integer[]'})
        except DBQueryException as e:
            logger.log("Failed to insert new data for %s stream %d" % \
                    (self.colname, stream))
            logger.log("Error was: %s" % (str(e)))
            raise

        filtered['path'] = result['path']
        filtered['aspath'] = result['aspath']
        if self.exporter != None:
            self.exporter.publishLiveData("amp-traceroute", stream, ts, \
                    filtered)

    def _update_as_stream(self, observed, streamid, datapoint):
        if streamid not in observed:
            observed[streamid] = {"packet_size":datapoint["packet_size"],
                    "icmperrors":0, "paths":{}}
        
        if datapoint['error_type'] != None or datapoint['error_code'] != None:
            observed[streamid]["icmperrors"] += 1

        if datapoint['aspath'] != None:
            keystr = "%s" % (streamid)
            for p in datapoint['aspath']:
                keystr += "_%s" % (p)

            # XXX This is going to insert all observed AS paths, even
            # if they aren't going to appear in the AS-traceroute data
            # table. This may be a tad inefficient, but I imagine we are
            # not going to have many AS paths anyway.
            if keystr in self.aspaths:
                aspath_id = self.aspaths[keystr]
            else:
                pathid = self._insert_path("as", stream, datapoint['aspath'])
                if pathid < 0:
                    return
                
                aspath_id = pathid
                self.aspaths[keystr] = pathid
        
            if aspath_id not in observed[streamid]['paths']:
                observed[streamid]['paths'][aspath_id] = \
                        (1, len(datapoint['aspath']))
            else:
                prev = observed[streamid]['paths'][aspath_id]
                observed[streamid]['paths'][aspath_id] = (prev[0] + 1, prev[1])

    def _aggregate_streamdata(self, streamdata):
        # Find the most common AS path
        commonpath = max(streamdata['paths'].iteritems(), \
                key = operator.itemgetter(1))
        streamdata['aspath_id'] = commonpath[0]
        streamdata['length'] = commonpath[1]

    def process_data(self, timestamp, data, source):
        """ Process a AMP traceroute message, which can contain 1 or more 
            sets of results 
        """
        asobserved = {}
        ipobserved = {}

        for d in data:
            streamparams, key = self._stream_properties(source, d)

            if key is None:
                logger.log("Failed to determine stream for %s result" % \
                        (self.colname))
                return DB_DATA_ERROR

            if key not in self.streams:
                streamid = self.create_new_stream(streamparams, timestamp)
                if streamid < 0:
                    logger.log("Failed to create new %s stream" % \
                            (self.colname))
                    logger.log("%s" % (str(streamparams)))
                    return
                self.streams[key] = streamid
            else:
                streamid = self.streams[key]

            self._extract_paths(d)

            # IP flag tells us if this is intended as an IP traceroute.
            # If the flag isn't present, we're running an old ampsave 
            # that pre-dates AS path support so assume an IP traceroute in
            # that case
            if 'ip' not in d or d['ip'] != 0:
                self.insert_ippath(streamid, timestamp, d)
                ipobserved[streamid] = 1
            elif 'as' in d and d['as'] != 0:
                # Just insert the AS path
                self.update_as_stream(asobserved, streamid, d) 
            
        for sid, streamdata in asobserved.iteritems():
            self._aggregate_streamdata(streamdata)
            self.insert_aspath(sid, timestamp, streamdata)

        # update the last timestamp for all streams we just got data for
        self.db.update_timestamp(self.ipdatatable, ipobserved.keys(), timestamp)
        self.db.update_timestamp(self.asdatatable, asobserved.keys(), timestamp)

    def insert_aspath(self, stream, ts, result):
        filtered = {}
        for col in self.asdatacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        try:
            self.db.insert_data(self.asdatatable, "amp-astraceroute", stream, 
                    ts, filtered, casts)
        except DBQueryException as e:
            logger.log("Failed to insert new data for %s stream %d" % \
                    ("amp-astraceroute", stream))
            logger.log("Error was: %s" % (str(e)))
            raise

        if self.exporter != None:
            self.exporter.publishLiveData("amp-astraceroute", stream, ts, \
                    filtered)


    def _extract_paths(self, result):
        aspath = []
        ippath = []
        rtts = []
        currentas = None
        count = 0

        for x in result['hops']:
            if 'address' in x:
                ippath.append(x['address'])

            if 'rtt' in x:
                rtts.append(x['rtt'])

            if 'asn' not in x:
                continue

            if currentas != x['asn']:
                if currentas != None:
                    assert(count != 0)
                    aspath.append("%d.%d" % (count, currentas))
                currentas = x['asn']
                count = 1

        if currentas != None:
            assert(count != 0)
            aspath.append("%d.%d" % (count, currentas))
        
        if len(rtts) == 0:
            result["hop_rtt"] = None
        else:
            result['hop_rtt'] = rtts
       
        if len(aspath) == 0:
            result["aspath"] = None 
        else:
            result["aspath"] = aspath

        if len(ippath) == 0:
            result["path"] = None
        else:
            result['path'] = ippath


# Helper functions for dbselect module which deal with complications
# arising from the extra data tables that need to be joined.

# This is a bit of nasty join -- are we going to run into problems with
# joining and unioning so many tables?
def generate_union(qb, table, streams):

    unionparams = []
    sql = "(("

    for i in range(0, len(streams)):
        unionparams.append(streams[i])
        sql += "SELECT * FROM %s_" % (table)
        sql += "%s"     # stream id will go here

        if i != len(streams) - 1:
            sql += " UNION ALL "

    sql += ") AS allstreams LEFT JOIN ("
    
    if "astraceroute" not in table:
        for i in range(0, len(streams)):
            sql += "SELECT * from data_amp_traceroute_paths_"
            sql += "%s"
            if i != len(streams) - 1:
                sql += " UNION ALL "

        sql += ") AS paths ON (allstreams.path_id = paths.path_id) LEFT JOIN ("
    
    for i in range(0, len(streams)):
        sql += "SELECT * from data_amp_traceroute_aspaths_"
        sql += "%s"
        if i != len(streams) - 1:
            sql += " UNION ALL "
    
    sql += ") AS aspaths ON (allstreams.aspath_id = aspaths.aspath_id)) AS "
    sql += "dataunion"
    qb.add_clause("union", sql, unionparams + unionparams + unionparams)


def sanitise_columns(columns):

    sanitised = []

    # TODO Take 'amp-traceroute' or 'amp-astraceroute' as a parameter
    # and limit the query to just the appropriate columns for that
    # collection

    for c in columns:
        if c in ['path', 'path_id', 'stream_id', 'timestamp', 'hop_rtt', 
                'packet_size', 'length', 'error_type', 'error_code',
                'aspath', 'aspath_id', 'icmperrors']:
            sanitised.append(c)

    return sanitised 


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
