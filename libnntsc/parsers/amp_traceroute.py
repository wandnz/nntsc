#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# NNTSC is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# NNTSC is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with NNTSC; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#


import time
from libnntsc.dberrorcodes import DBQueryException
from libnntsc.dberrorcodes import DB_CODING_ERROR, DB_DATA_ERROR
import libnntscclient.logger as logger
from libnntsc.parsers.amp_icmp import AmpIcmpParser

PATH_FLUSH_FREQ = 60 * 60

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
        self.pending_paths = {}
        self.pending_aspaths = {}

        self.ipdatacolumns = [
            {"name":"path_id", "type":"integer", "null":False},
            {"name":"aspath_id", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"error_type", "type":"smallint", "null":True},
            {"name":"error_code", "type":"smallint", "null":True},
            {"name":"hop_rtt", "type":"integer[]", "null":False},
        ]

        self.asdatacolumns = [
            {"name":"aspath_id", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"errors", "type":"smallint", "null":True},
            {"name":"addresses", "type":"smallint", "null":True},
        ]

        self.ascollectionid = None
        self.ipcollectionid = None

        now = int(time.time())
        self.nextpathflush = (now - (now % PATH_FLUSH_FREQ)) + \
                PATH_FLUSH_FREQ

    def data_table(self):
        self.db.create_data_table(self.asdatatable, self.asdatacolumns)
        self._create_indexes(self.asdatatable, [])

        self.db.create_data_table(self.ipdatatable, self.ipdatacolumns)
        self._create_indexes(self.ipdatatable, [])

        self.db.commit_streams()

        # Create the paths tables as well
        aspathcols = [ \
            {"name":"aspath_id", "type":"serial primary key"},
            {"name":"aspath", "type":"varchar[]", "null":False, "unique":True},
            {"name":"aspath_length", "type":"smallint", "null":False},
            {"name":"uniqueas", "type":"smallint", "null":False},
            {"name":"responses", "type":"smallint", "null":False},
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
            {"name":"path", "type":"inet[]", "null":False, "unique":True},
            {"name":"length", "type":"smallint", "null":False},
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
                timestamp, False)

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

        if self.exporter is None:
            return streamid

        colid = self._get_iptraceroute_collection_id()
        if colid <= 0:
            return streamid

        self.exporter.publishStream(colid, "amp-traceroute", streamid,
                streamparams)

        colid = self._get_astraceroute_collection_id()
        if colid <= 0:
            return streamid

        self.exporter.publishStream(colid, "amp-astraceroute", streamid,
                streamparams)

        return streamid

    def _get_collection_id(self, module):
        try:
            colid = self.db.get_collection_id(self.source, module)
        except DBQueryException as e:
            logger.log("Failed to get collection id for %s %s" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        return colid

    def _get_astraceroute_collection_id(self):
        if self.ascollectionid is None:
            colid = self._get_collection_id("astraceroute")

            if colid > 0:
                self.ascollectionid = colid
            else:
                return -1

        return self.ascollectionid

    def _get_iptraceroute_collection_id(self):
        if self.ipcollectionid is None:
            colid = self._get_collection_id("traceroute")

            if colid > 0:
                self.ipcollectionid = colid
            else:
                return -1

        return self.ipcollectionid

    def _ippath_insertion_sql(self, stream, pathdata):
        pathtable = "data_amp_traceroute_paths_%d" % stream

        pathinsert = "WITH s AS (SELECT path_id, path FROM %s " % pathtable
        pathinsert += "WHERE path = CAST (%s as inet[])), "
        pathinsert += "i AS (INSERT INTO %s (path, length) " % pathtable
        pathinsert += "SELECT CAST(%s as inet[]), %s "
        pathinsert += "WHERE NOT EXISTS "
        pathinsert += "(SELECT path FROM %s WHERE path " % pathtable
        pathinsert += "= CAST(%s as inet[])) RETURNING path_id, path) "
        pathinsert += "SELECT path_id,path FROM i UNION ALL "
        pathinsert += "SELECT path_id,path FROM s"

        path = pathdata['path']
        params = (path, path, pathdata['length'], path)

        return pathinsert, params

    def _aspath_insertion_sql(self, stream, pathdata):
        pathtable = "data_amp_traceroute_aspaths_%d" % stream

        pathinsert = "WITH s AS (SELECT aspath_id, aspath FROM %s " % pathtable
        pathinsert += "WHERE aspath = CAST (%s as varchar[])), "
        pathinsert += "i AS (INSERT INTO %s " % pathtable
        pathinsert += "(aspath, aspath_length, uniqueas, responses) "
        pathinsert += "SELECT CAST(%s as varchar[]), %s, %s, %s "
        pathinsert += "WHERE NOT EXISTS "
        pathinsert += "(SELECT aspath FROM %s WHERE aspath " % pathtable
        pathinsert += "= CAST(%s as varchar[])) RETURNING aspath_id, aspath) "
        pathinsert += "SELECT aspath_id,aspath FROM i UNION ALL "
        pathinsert += "SELECT aspath_id,aspath FROM s"

        path = pathdata['aspath']
        params = (path, path, pathdata['aspathlen'], pathdata['uniqueas'], \
                pathdata['responses'], path)

        return pathinsert, params

    def _insert_path(self, pathtype, stream, path):
        if pathtype == "ip":
            pathinsert, params = self._ippath_insertion_sql(stream, path)

        elif pathtype == "as":
            pathinsert, params = self._aspath_insertion_sql(stream, path)
        else:
            return DB_CODING_ERROR


        try:
            queryret = self.db.custom_insert(pathinsert, params)
        except DBQueryException as e:
            logger.log("Failed to get path_id for %s test result (stream %d)" \
                    % (self.colname, stream))
            logger.log("Error was: %s" % (str(e)))
            raise

        if queryret is None:
            logger.log("No query result when getting path_id for %s (stream %d)" % \
                    (self.colname, stream))
            raise DBQueryException(DB_DATA_ERROR)

        return int(queryret[0])


    def insert_ippath(self, stream, ts, result):
        """ Insert data for a single traceroute path into the database """

        if 'path' not in result or result['path'] is None:
            return
        keystr = "%s" % (stream)
        for p in result['path']:
            keystr += "_%s" % (p)

        if keystr in self.paths:
            result['path_id'] = self.paths[keystr][0]
        else:
            pathid = self._insert_path("ip", stream, result)
            if pathid < 0:
                return

            result['path_id'] = pathid
            self.pending_paths[keystr] = (pathid, int(time.time()))

        if result['aspath'] != None:
            keystr = "%s" % (stream)
            for p in result['aspath']:
                keystr += "_%s" % (p)

            if keystr in self.aspaths:
                result['aspath_id'] = self.aspaths[keystr][0]
            else:
                pathid = self._insert_path("as", stream, result)
                if pathid < 0:
                    return

                result['aspath_id'] = pathid
                self.pending_aspaths[keystr] = (pathid, int(time.time()))

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
        filtered['length'] = result['length']
        filtered['aspath_length'] = result['aspathlen']
        filtered['uniqueas'] = result["uniqueas"]
        filtered['responses'] = result["responses"]

        colid = self._get_iptraceroute_collection_id()

        if self.exporter != None and colid > 0:
            self.exporter.publishLiveData(colid, stream, ts, filtered)

    def _update_as_stream(self, observed, streamid, datapoint):
        if streamid not in observed:
            observed[streamid] = {
                "packet_size": datapoint["packet_size"],
                "errors": None,
                "paths": {},
                "addresses": None
            }

        if datapoint['address'] != "0.0.0.0" and datapoint['address'] != "::":
            observed[streamid]['addresses'] = self._add_maybe_none(
                    observed[streamid]['addresses'], 1)
            # update errors from None to at least zero if we have an address
            observed[streamid]["errors"] = self._add_maybe_none(
                    observed[streamid]['errors'], 0)

        if datapoint['error_type'] != None or datapoint['error_code'] != None:
            observed[streamid]["errors"] = self._add_maybe_none(
                    observed[streamid]['errors'], 1)

        if datapoint['aspath'] != None:
            keystr = "%s" % (streamid)
            for p in datapoint['aspath']:
                keystr += "_%s" % (p)

            # XXX This is going to insert all observed AS paths, even
            # if they aren't going to appear in the AS-traceroute data
            # table. This may be a tad inefficient, but I imagine we are
            # not going to have many AS paths anyway.
            if keystr in self.aspaths:
                aspath_id = self.aspaths[keystr][0]
            else:
                pathid = self._insert_path("as", streamid, datapoint)
                if pathid < 0:
                    return

                aspath_id = pathid
                self.pending_aspaths[keystr] = (pathid, int(time.time()))

            if aspath_id not in observed[streamid]['paths']:
                observed[streamid]['paths'][aspath_id] = { \
                        'count':1, 'aspathlen':datapoint['aspathlen'],
                        'aspath':datapoint['aspath'],
                        'uniqueas':datapoint['uniqueas'],
                        'responses':datapoint['responses']
                }
            else:
                observed[streamid]['paths'][aspath_id]['count'] += 1

    def _aggregate_streamdata(self, streamdata):
        # Find the most common AS path
        maxfreq = 0
        commonpathid = -1

        for pathid, pdata in streamdata['paths'].iteritems():
            if pdata['count'] > maxfreq:
                commonpathid = pathid
                maxfreq = pdata['count']

        if commonpathid < 0:
            streamdata['aspath_id'] = None
            streamdata['aspath'] = None
            streamdata['aspathlen'] = None
            streamdata['uniqueas'] = None
            streamdata['responses'] = None
        else:
            streamdata['aspath_id'] = commonpathid
            streamdata['aspath'] = streamdata['paths'][commonpathid]['aspath']
            streamdata['aspathlen'] = streamdata['paths'][commonpathid]['aspathlen']
            streamdata['uniqueas'] = streamdata['paths'][commonpathid]['uniqueas']
            streamdata['responses'] = streamdata['paths'][commonpathid]['responses']


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
                self._update_as_stream(asobserved, streamid, d)

        for sid, streamdata in asobserved.iteritems():
            self._aggregate_streamdata(streamdata)
            self.insert_aspath(sid, timestamp, streamdata)

        # update the last timestamp for all streams we just got data for
        self.db.update_timestamp(self.ipdatatable, ipobserved.keys(),
                timestamp, False)
        self.db.update_timestamp(self.asdatatable, asobserved.keys(),
                timestamp, False)

        now = int(time.time())
        if now > self.nextpathflush:
            self._flush_unused_paths(now)
            self.nextpathflush = (now - (now % PATH_FLUSH_FREQ)) + \
                    PATH_FLUSH_FREQ

    def post_commit(self):
        # all pending paths are confirmed to have been committed, move them
        # into the main cache
        self.paths.update(self.pending_paths)
        self.aspaths.update(self.pending_aspaths)
        self.pending_paths.clear()
        self.pending_aspaths.clear()

    def _flush_unused_paths(self, now):
        toremove = []
        for k, v in self.aspaths.iteritems():
            if v[1] + PATH_FLUSH_FREQ < now:
                toremove.append(k)

        for k in toremove:
            del self.aspaths[k]

        toremove = []
        for k, v in self.paths.iteritems():
            if v[1] + PATH_FLUSH_FREQ * 3 < now:
                toremove.append(k)

        for k in toremove:
            del self.paths[k]

    def insert_aspath(self, stream, ts, result):
        filtered = {}
        for col in self.asdatacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        try:
            self.db.insert_data(self.asdatatable, "amp-astraceroute", stream,
                    ts, filtered, {'aspath':'varchar[]'})
        except DBQueryException as e:
            logger.log("Failed to insert new data for %s stream %d" % \
                    ("amp-astraceroute", stream))
            logger.log("Error was: %s" % (str(e)))
            raise

        colid = self._get_astraceroute_collection_id()

        if self.exporter != None and colid > 0:
            filtered['aspath'] = result['aspath']
            filtered['aspath_length'] = result['aspathlen']
            filtered['uniqueas'] = result["uniqueas"]
            filtered['responses'] = result["responses"]
            self.exporter.publishLiveData(colid, stream, ts, filtered)


    def _extract_paths(self, result):
        aspath = []
        ippath = []
        rtts = []
        currentas = None
        responses = 0
        count = 0
        aspathlen = 0

        seenas = []

        for x in result['hops']:
            if 'address' in x:
                ippath.append(x['address'])
            else:
                ippath.append(None)

            if 'rtt' in x:
                rtts.append(x['rtt'])
            else:
                rtts.append(None)

            if 'as' not in x:
                continue

            if currentas != x['as']:
                if currentas != None:
                    assert(count != 0)
                    aspath.append("%d.%d" % (count, currentas))
                currentas = x['as']
                count = 1
            else:
                count += 1

            # Keep track of unique AS numbers in the path, not counting
            # null hops, RFC 1918 addresses or failed lookups
            if x['as'] not in seenas and x['as'] >= 0:
                seenas.append(x['as'])

            aspathlen += 1
            responses += 1

        if currentas != None:
            assert(count != 0)
            assert(responses >= count)

            aspath.append("%d.%d" % (count, currentas))

            # Remove tailing "null hops" from our responses count
            if currentas == -1:
                responses -= count

        # these are fine as empty arrays if there was no path (e.g. it
        # couldn't be tested)
        result['path'] = ippath
        result['hop_rtt'] = rtts

        if len(aspath) == 0:
            result["aspath"] = None
            result["aspathlen"] = None
            result["uniqueas"] = None
            result["responses"] = None
        else:
            result["aspath"] = aspath
            result["aspathlen"] = aspathlen
            result["uniqueas"] = len(seenas)
            result["responses"] = responses


# Helper functions for dbselect module which deal with complications
# arising from the extra data tables that need to be joined.

# This is a bit of nasty join -- are we going to run into problems with
# joining and unioning so many tables?
def generate_union(qb, table, streams):

    allstreams = []
    unionparams = []
    sql = "(SELECT allstreams.*, "
    if "astraceroute" not in table:
        sql += "paths.path, paths.length, "
    sql += "aspaths.aspath, "
    sql += "aspaths.responses, aspaths.aspath_length, aspaths.uniqueas FROM ("

    for i in range(0, len(streams)):
        allstreams.append(streams[i])
        sql += "SELECT * FROM %s_" % (table)
        sql += "%s"     # stream id will go here

        if i != len(streams) - 1:
            sql += " UNION ALL "

    unionparams = list(allstreams)
    sql += ") AS allstreams LEFT JOIN ("

    if "astraceroute" not in table:
        for i in range(0, len(streams)):
            sql += "SELECT * from data_amp_traceroute_paths_"
            sql += "%s"
            if i != len(streams) - 1:
                sql += " UNION ALL "

        sql += ") AS paths ON (allstreams.path_id = paths.path_id) LEFT JOIN ("
        unionparams += allstreams

    for i in range(0, len(streams)):
        sql += "SELECT * "
        sql += "FROM data_amp_traceroute_aspaths_"
        sql += "%s"
        if i != len(streams) - 1:
            sql += " UNION ALL "
        unionparams += allstreams

    sql += ") AS aspaths ON (allstreams.aspath_id = aspaths.aspath_id)) AS "
    sql += "dataunion"
    qb.add_clause("union", sql, unionparams)


def sanitise_column(column):

    # TODO Take 'amp-traceroute' or 'amp-astraceroute' as a parameter
    # and limit the query to just the appropriate columns for that
    # collection

    if column in ['path_id', 'aspath_id']:
        return column

    if column in ['path', 'stream_id', 'timestamp', 'hop_rtt',
                'packet_size', 'length', 'error_type', 'error_code',
                'aspath', 'errors', 'aspath_length', 'responses',
                'uniqueas', 'addresses']:
        return column

    return None


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
