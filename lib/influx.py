# This file is part of NNTSC
#
# Copyright (C) 2013 The University of Waikato, Hamilton, New Zealand
# Authors: Shane Alcock
#          Brendon Jones
#          Nathan Overall
#          Andy Bell
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



from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from libnntsc.dberrorcodes import *
from libnntsc.querybuilder import QueryBuilder
from libnntsc.streamcache import StreamCache
import libnntscclient.logger as logger
from libnntsc.cqs import build_cqs, get_cqs, get_parser
from requests import ConnectionError
import requests
import time
import threading
import psutil

from multiprocessing import Pipe, Queue
import Queue as StdQueue

DEFAULT_RP = "default"
ROLLUP_RP = "rollups"
MAX_CQWORKERS = 1

requests.packages.urllib3.disable_warnings()

class InfluxConnection(object):
    """A class to represent a connection to an Influx Database"""
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost="localhost",
                 dbport="8086", timeout=None, cachetime=0):

        self.dbname = dbname
        
        if dbhost == "" or dbhost == None:
            dbhost = "localhost"

        if dbport == "" or dbport == None:
            dbport = 8086
            
        if dbpass == "":
            dbpass = None

        if dbuser == "":
            dbuser = None

        if timeout == 0:
            timeout = None

        try:
            self.client = InfluxDBClient(
                dbhost, dbport, dbuser, dbpass, self.dbname, timeout=timeout)
        except Exception as e:
            self.handler(e)

        self.started = 0
        self.streamcache = StreamCache(dbname, cachetime)


    def query(self, query):
        """Returns ResultSet object"""
        try:
            return self.client.query(query, epoch='s')
        except Exception as e:
            print query
            self.handler(e)

    def query_timestamp(self, table, streamid=None, first_or_last="last", rollup=None):
        """
        Returns either the first or last timestamp in database for
        given table and stream, or just table if no stream is given
        """
        if first_or_last == "max":
            first_or_last = "last"
        if first_or_last == "min":
            first_or_last = "first"    
        if first_or_last not in ["first", "last"]:
            return

        field = get_parser(table).get_random_field(rollup)

        if rollup is not None:
            table = "{}.{}_{}".format(ROLLUP_RP, table, rollup)

        if streamid is None:
            streamclause = ""
        else:
            streamclause = "WHERE stream = '%s'" % (streamid)

        query = "SELECT %s(%s) FROM %s %s" % (first_or_last, field, table, \
                streamclause)

        ts = self.query(query)
        try:
            point = ts.get_points().next()
        except StopIteration:
            return 0

        if point is None or "time" not in point or point["time"] == 0:
            if self.started != 0:
                x = self.started
            else:
                x = time.time()
            if ((x % (60 * 60 * 24)) < (60 * 60 * 4)):
                x -= (60 * 60 * 5)
            if first_or_last == "first":
                return int(x - (x % (60 * 60 * 12)))
            return int(x - (x % (60 * 60 * 2)))

        return point["time"]
            
    def handler(self, db_exception):
        """
        A basic error handler for queries to database
        """
        try:
            raise db_exception
        except InfluxDBClientError as e:
            logger.log(e)
            raise DBQueryException(DB_GENERIC_ERROR)
        except InfluxDBServerError as e:
            logger.log(e)
            raise DBQueryException(DB_QUERY_TIMEOUT)
        except ConnectionError as e:
            logger.log(e)
            raise DBQueryException(DB_QUERY_TIMEOUT)
        except KeyboardInterrupt:
            raise DBQueryException(DB_INTERRUPTED)
        except Exception as e:
            raise e

    def _get_influx_binsize(self, binsize):
        """Returns a string representing the binsize in largest unit possible"""
        if binsize % 86400 == 0:
            # convert to days
            return "{}d".format(binsize/86400)
        elif binsize % 3600 == 0:
            # convert to hours
            return "{}h".format(binsize/3600)
        elif binsize % 60 == 0:
            # convert to minutes
            return "{}m".format(binsize/60)
        else:
            # leave as seconds
            return "{}s".format(binsize)

    def _get_binsize(self, influx_binsize):
        """Returns the number of seconds represented by influx binsize of form [0-9]+[dhms]"""
        num = int(influx_binsize[:-1])
        unit = influx_binsize[-1]

        # convert to seconds
        if unit == "d":
            return num * 86400
        elif unit == "h":
            return num * 3600
        elif unit == "m":
            return num * 60
        elif unit == "s":
            return num

class ContinuousQueryRerunner(threading.Thread):
    def __init__(self, jobqueue, parent): 
        super(ContinuousQueryRerunner, self).__init__()
        self.jobqueue = jobqueue
        self.parent = parent

    def rerun_cq(self, job):
        """
        Rerun a continuous query. These queries don't seem to work with raw timestamps in 
        the WHERE clause, so we use now() - x, where x is the amount of time between now and 
        the timestamp we want
        """

        table = job[0]
        binsize = job[1]
        aggs = job[2]
        start = job[3]
        end = job[4]
        binsecs = job[5]

        agg_string = ",".join(
            ["{}({}) AS {}".format(agg, col, name) for (name, agg, col) in aggs])

        while start < end:
            query = """
            SELECT {0} INTO "{1}".{2}_{3} FROM {2} WHERE time >= {4}s and time < {5}s GROUP BY stream,time({3})
            """.format(agg_string, ROLLUP_RP, table, binsize, start, start + (10 * binsecs))
            result = self.parent.query(query)
            result = None
            start += (10 * binsecs)
        
    def run(self):
        running = 1

        while running:

            mem = psutil.virtual_memory()
            #if mem.percent > 50.0:
            #    time.sleep(5)
            #    continue

            try:
                job = self.jobqueue.get(True, 60)
            except StdQueue.Empty:
                continue
            except EOFError:
                break

            self.rerun_cq(job)

class InfluxInsertor(InfluxConnection):
    """
    A class for inserting data into the influx database
    """            
    def __init__(self, dbname, user, password, host, port, timeout=None):
        super(InfluxInsertor, self).__init__(dbname, user, password, host, port, timeout)
        cqs_in_db = self.query("show continuous queries").get_points()
        self.cqs_in_db = [cq["name"] for cq in cqs_in_db]
        self.to_write = []
        # A dictionary to keep track of the range of timestamps encountered for each table
        self.points_windows = {}

        self.cqrs = []
        self.cqjobs = Queue(10000)

        self.pending_cqs = {}

        for i in range(0, MAX_CQWORKERS):
            worker = ContinuousQueryRerunner(self.cqjobs, self)
            worker.daemon = True
            worker.start()
            self.cqrs.append(worker)

        self.started = time.time()

    def commit_data(self, retention_policy=DEFAULT_RP):
        """Send all data that has been observed"""
        try:
            self.client.write_points(self.to_write, time_precision="s",
                                     retention_policy=retention_policy)
            self.to_write = []
        except Exception as e:
            self.handler(e)

        # Rerun any continuous queries that need running
        now = int(time.time())

        # Go through ranges of timestamps we just inserted and see if we
        # need to run any continuous queries on them
        triggered = set()

        for table, (most_recent, last_binned) in self.points_windows.iteritems():
            cqs = get_cqs(table)
            for influx_binsizes, aggs in cqs:
                for influx_binsize in influx_binsizes:
                    binsize = self._get_binsize(influx_binsize)
                    # wrap up the last bin
                    time_from = last_binned[influx_binsize][0]
                    force_cq = last_binned[influx_binsize][1]
                    time_to = most_recent - (most_recent % binsize)
                    last_auto_store = now - (now % binsize)

                    # If we've filled a whole bin and we're not overlapping
                    # with influx continuous query auto stores...
                    if time_from < last_auto_store and time_to > time_from:

                        # still catching up on historical data -- don't run
                        # cqs until we catch up
                        if time_to < last_auto_store and \
                                time_to - time_from < (5 * binsize):

                            if influx_binsize not in self.pending_cqs:
                                self.pending_cqs[influx_binsize] = {}
                            self.pending_cqs[influx_binsize][table] = \
                                    (aggs, time_from, time_to)
                            continue

                        self._queue_cqs(time_from, time_to - 1, table, aggs, \
                                influx_binsize)

                        last_binned[influx_binsize] = (time_to, False)
                        if influx_binsize in self.pending_cqs and table in \
                                self.pending_cqs[influx_binsize]:
                            del self.pending_cqs[influx_binsize][table]

                        # If we have pending CQs for this binsize, make
                        # sure we run them now that we've caught up
                        if influx_binsize in self.pending_cqs:
                            triggered.add(influx_binsize)

        # Deal with the pending CQs
        for influx_binsize in triggered:
            for t, data in self.pending_cqs[influx_binsize].iteritems():
                self._queue_cqs(data[1], data[2] - 1, t, data[0], \
                        influx_binsize)

                mostrec, lbin = self.points_windows[t]
                lbin[influx_binsize] = (data[2], False)
            self.pending_cqs[influx_binsize] = {}

    def _queue_cqs(self, start, end, table, aggs, influx_binsize):

        binsize = self._get_binsize(influx_binsize)
        while start < end:

            nextend = end
            #if start + binsize > end:
            #    nextend = end
            #else:
            #    nextend = start + binsize - 1
            try:
                self.cqjobs.put((table, influx_binsize, aggs, start, nextend, binsize))
            except StdQueue.Full:
                logger.log("Too many CQ jobs queued!")
                raise
            start = nextend

    def insert_data(self, tablename, stream, ts, result, casts = {}):
        """Prepare data for sending to database"""
        for cast in casts:
            if cast in result.keys():
                result[cast] = casts[cast](result[cast])
        self.to_write.append(
            {
                "measurement": tablename,
                "tags":{
                    "stream": stream
                },
                "time":ts,
                "fields": result
            }
        )

        now = time.time()
        if self.started == 0:
            self.started = ts

        # Keep track of what time period we're collecting points for
        if tablename not in self.points_windows:
            # This is the first timestamp we've encountered, so make the
            # last_binned dictionary up
            cqs = get_cqs(tablename)
            last_binned = {}
            for influx_binsizes, aggs in cqs:
                for influx_binsize in influx_binsizes:
                    # Find the last bin that was created and start rollups
                    # from 1 bin after that
                    binsize = self._get_binsize(influx_binsize)
                    mints = self.query_timestamp(tablename, rollup=influx_binsize) 
                    if mints == 0:
                        last_binned[influx_binsize] = (ts - (ts % binsize) \
                                - binsize, False)
                    else:
                        last_binned[influx_binsize] = (mints - \
                                (mints % binsize), False)
            # Store the most recent point inserted and the last completed bin
            # for each bin
            self.points_windows[tablename] = (ts, last_binned)
        else:
            most_recent, last_binned = self.points_windows[tablename]
            # Check that we haven't got any data from an already binned up
            # time. If so, we need to set the 'last_binned' time to before our
            # new 'old' data so that it'll get binned up
            for influx_binsize, (last_bin_time, force) in last_binned.iteritems():
                binsize = self._get_binsize(influx_binsize)
                if self._rewind_last_bin_time(ts, last_bin_time, binsize, now):
                    last_binned[influx_binsize] = (ts - (ts % binsize), True)

            self.points_windows[tablename] = (ts, last_binned)


    def _rewind_last_bin_time(self, ts, last_bin_time, binsize, now):

            # Don't rewind when backfilling -- we already know this data is old
            if ts <= self.started:
                return False

            # Don't "rewind" if we're already binning at an earlier time
            if ts > last_bin_time:
                return False

            # Only worry about rewinding if the data is in the next bin to
            # aggregate
            if now <= last_bin_time + binsize:
                return False

            # Similarly, don't worry about 'new' data
            if ts >= now - (now % binsize):
                return False

            return True


    def build_cqs(self, postgresdb, retention_policy=DEFAULT_RP):
        """Build all continuous queries with given retention policy"""
        # Calls function in lib/cqs.py
        build_cqs(self, retention_policy)

    def destroy_cqs(self):
        """Destroy all continuous queries that have been created"""
        for query in self.cqs_in_db:
            try:
                self.client.query("drop continuous query {} on {}".format(query, self.dbname))
            except InfluxDBClientError:
                logger.log("Failed to drop CQ {} from {}. May not exist".format(query, self.dbname))
        self.cqs_in_db = []
        
    def create_cqs(self, cqs, measurement, retention_policy=DEFAULT_RP):
        """Create continuous queries for measurement as specified"""
        for agg_group in cqs:
            for time in agg_group[0]:
                try:
                    self.create_cq(agg_group[1], measurement, time, retention_policy)
                except DBQueryException as e:
                    logger.log("Failed to create CQ {}_{}. May already exist".format(
                        measurement, time))
        
    def create_cq(self, aggregations, measurement, time, retention_policy=DEFAULT_RP):
        """Create a particular continuous query with given aggregations at given time
        frequency on given measurement"""
        cq_name = "{}_{}".format(measurement, time)
        if cq_name in self.cqs_in_db:
            # Drop continuous query if it already exists
            self.query("drop continuous query {} on {}".format(cq_name, self.dbname))
            self.cqs_in_db.remove(cq_name)
        agg_string = ",".join(
            ["{}({}) AS {}".format(agg, col, name) for (name, agg, col) in aggregations])
        query = """
        CREATE CONTINUOUS QUERY {0}_{1} ON {2} BEGIN
        SELECT {3} INTO "{4}".{0}_{1} FROM {0} GROUP BY stream,time({1}) END
        """.format(
            measurement, time, self.dbname, agg_string, retention_policy)
        self.query(query)
        self.cqs_in_db.append(cq_name)

    def create_retention_policies(self, keepdata, keeprollups):
        """Create retention policies at given length"""
        try:
            rps = self.client.get_list_retention_policies()
            default_exists = False
            rollups_exists = False
            extra_rps = []

            for rp in rps:
                if rp["name"] == DEFAULT_RP:
                    default_exists = True
                elif rp["name"] == ROLLUP_RP:
                    rollups_exists = True
                else:
                    extra_rps.append(rp["name"])

            if default_exists:
                self.client.alter_retention_policy(
                    DEFAULT_RP, duration=keepdata, default=True)
            else:
                self.client.create_retention_policy(
                    DEFAULT_RP, duration=keepdata, replication=1, default=True)

            if rollups_exists:
                self.client.alter_retention_policy(
                    ROLLUP_RP, duration=keeprollups)
            else:
                self.client.create_retention_policy(
                    ROLLUP_RP, duration=keeprollups, replication=1)

            for rp in extra_rps:
                logger.log("Found extra retention policy: {}".format(rp))

        except Exception as e:
            self.handler(e)

    def new_db(self):
        """Drop the database and start again"""
        try:
            self.client.drop_database(self.dbname)
            self.client.create_database(self.dbname)
            self.cqs_in_db = []
        except Exception as e:
            self.handler(e)
        
class InfluxSelector(InfluxConnection):
    """A class for selecting things from influx database"""
    def __init__(self, thread_id, dbname, user, password, host, port, timeout):
        super(InfluxSelector, self).__init__(dbname, user, password, host, port, timeout)
        self.thread_id = thread_id
        self.qb = QueryBuilder()
        self.qb2 = QueryBuilder()
        self.influx_binsize = ""
        self.aggcols = []
        self.cqs = []
        self.streams = []
        self.table = ""
        self.rename = False
        self.streams_to_labels = {}

    def select_data(self, table, labels, selectcols, start_time, stop_time):
        """
        Selects time series data from influx with no aggregation

        Parameters:
                table -- the name of the table to query
                labels -- a dictionary of labels with lists of stream ids to get data for
                selectcols -- a list of data columns to select on. If not
                              included, 'stream_id' and 'timestamp' will
                              be added to this list before running the
                              query
                start_time -- a timestamp describing the start of the
                              time period that data is required for. If
                              None, this is set to 1 day before the stop
                              time
                stop_time -- a timestamp describing the end of the time
                             period that data is required for. If None,
                             this is set to the current time.

        This is a generator function and yields a tuple. Assumes prior
        sanitation of selectcols and is designed to be called by function of
        same name in dbselect

        """
        if table == "data_amp_dns":
            for i, col in enumerate(selectcols):
                if col == "timestamp":
                    selectcols[i] = "requests"

        self.qb.reset()
        self.qb.add_clause("select", "select {}".format(", ".join(selectcols)))

        self.qb.add_clause("from", "from {}".format(table))

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)
                continue

            fluxstreams = []
            for sid in streams:
                if self._was_stream_active(sid, table, start_time, stop_time):
                    fluxstreams.append(sid)

            if len(fluxstreams) == 0:
                continue

            self.qb.add_clause(
                    "where", "where time >= {}s and time < {}s and {}".format(
                            start_time, stop_time, " or ".join([
                            "stream = '{}'".format(s) for s in fluxstreams])))

            order = ["select","from","where"]
            querystring, _ = self.qb.create_query(order)
            try:
                results = self.query(querystring)
            except DBQueryException as e:
                yield(None, label, None, None, e)

            rows = []
            for (table, tags), results in results.items():
                for result in results:
                    result["nntsclabel"] = label
                    result["timestamp"] = result["time"]
                    del result["time"]
                    rows.append(result)

            yield(rows, label, "timestamp", 0, None)
    

    def _was_stream_active(self, sid, table, start, end):

        firststamps = self.streamcache.fetch_all_first_timestamps("influx",
            table)
        laststamps = self.streamcache.fetch_all_last_timestamps("influx",
            table)

        if sid not in firststamps:
            firststamps[sid] = None
        if firststamps[sid] == None:
            f = self.query_timestamp(table, sid, "first")
            if f != 0:
                firststamps[sid] = f
                self.streamcache.set_first_timestamps("influx", table,
                        firststamps)

        if sid not in laststamps:
            laststamps[sid] = None
        if laststamps[sid] == None:
            l = self.query_timestamp(table, sid, "last")
            if l != 0:
                laststamps[sid] = l
                self.streamcache.set_last_timestamps("influx", table,
                        laststamps)

        if firststamps[sid] is None or laststamps[sid] is None:
            return False

        if firststamps[sid] > end:
            return False
        if laststamps[sid] >= start and laststamps[sid] < time.time() - 600:
            return False

        return True


    def select_aggregated_data(self, table, labels, aggcols, start_time,
                               stop_time, binsize):
        """
        Selects aggregated data from a given table, within parameters.

            Parameters:
                table -- the name of the table to query
                labels -- a dictionary of label to streams mappings, describing
                         which streams to query and what labels to assign to
                         the results.
                aggcols -- a list of tuples describing the columns to 
                           aggregate and the aggregation function to apply to 
                           that column
                start_time -- a timestamp describing the start of the
                              time period that data is required for. If
                              None, this is set to 1 day before the stop
                              time
                stop_time -- a timestamp describing the end of the time
                             period that data is required for. If None,
                             this is set to the current time.
                groupcols -- a list of data columns to group the results by.
                             'stream_id' will always be added to this list
                             if not present.
                binsize -- the size of each time bin. If 0 (the default),
                           the entire data series will aggregated into a
                           single summary value.

        This is a generator function and will yield a tuple each time it is
        iterated over. The function is called by the select_aggregated_data in
        dbselect, and assumes that column names have been sanitised already.

        """

        self.qb.reset()
        self.influx_binsize = self._get_influx_binsize(binsize)
        
        self.table = table
        self.aggcols = aggcols
        lastbin = 0

        # Change "smoke" to "smokearray", because dns asks for this
        for i, (meas, agg) in enumerate(self.aggcols):
            if agg == "smoke":
                self.aggcols[i] = (meas, "smokearray")
            # Dirty hack to deal with the fact that influx doesn't let us to
            # the aggregation on the timestamp column that we used to use in
            # postgres. Once we switch over to influx properly, we can update
            # ampy to ask for 'requests' directly.
            if meas == "timestamp" and agg == "count":
                self.aggcols[i] = ("requests", agg)
                
        # If there are aggregations on the same column we need to rename our response
        self._set_rename()
        # Check if we're requesting at least one bin, otherwise query
        # the raw data
        if start_time >= time.time() - binsize:
            is_rollup = False
        else:
            # Also check if we have an appropriate rollup table
            self.cqs = get_cqs(self.table, self.influx_binsize)
            if self.cqs:
                # Get the names of the columns in the pre-aggregated table
                columns = self._get_rollup_columns()
                # _get_rollup_columns() returns None if there are aggregations asked for
                # that aren't in a rollup table
                is_rollup = columns is not None
            else:
                is_rollup = False

        if not is_rollup:
            # We just construct aggregation functions and roll up the data ourselves
            columns = self._get_rollup_functions()
            self.qb.add_clause("from", "from {}".format(table))
            if stop_time - start_time > binsize:
                self.qb.add_clause("group_by", "group by stream, time({})".format(self.influx_binsize))
            else:
                self.qb.add_clause("group_by", "group by stream".format(self.influx_binsize))

        else:
            # Otherwise we'll use the pre-aggregated columns we found
            self.qb.add_clause("from", "from {}.{}_{}".format(
                ROLLUP_RP, self.table, self.influx_binsize))
            lastbin = self.query_timestamp(self.table, None, "last", self.influx_binsize)
            self.qb.add_clause("group_by", "group by stream")
            # Second query is for last bin of data which may not have been collected by CQs
            self.qb2.add_clause("from", "from {}".format(table))
            self.qb2.add_clause("group_by", "group by stream, time({})".format(self.influx_binsize))
            columns2 = self._get_rollup_functions()
            self.qb2.add_clause("select", "select {}".format(", ".join(columns2)))
        
        self.qb.add_clause("select", "select {}".format(
            ", ".join(columns)))



        # Collect all the streams together so we can do one big query, but
        # take note of the labels associated with them
        self.streams_to_labels = {}
        all_streams = []
        labels_and_rows = {}

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)

            else:
                labels_and_rows[label] = {}
                for stream in streams:
                    all_streams.append(stream)
                    self.streams_to_labels[str(stream)] = label

        if len(all_streams) == 0:
            return

        # Conditional is disjunction of all of the streams with conjunction of time period
        self.qb.add_clause("where", "where time >= {}s and time < {}s and {}".format(
            start_time, lastbin if is_rollup else stop_time, " or ".join([
                "stream = '{}'".format(stream) for stream in all_streams])))

        order = ["select","from","where","group_by"]
        querystring, _ = self.qb.create_query(order)

        results = self.query(querystring)

        # Update the labels of the results
        for (series, tags), generator in results.items():
            for result in generator:
                label = self.streams_to_labels[tags["stream"]]
                row = self._row_from_result(result, label)
                if row:
                    ts = row['timestamp']
                    if ts not in labels_and_rows[label]:
                        labels_and_rows[label][ts] = row
                    else:
                        for k,v in row.iteritems():
                            if k not in labels_and_rows[label][ts]:
                                labels_and_rows[label][ts][k] = v
                            elif labels_and_rows[label][ts][k] is None:
                                labels_and_rows[label][ts][k] = row[k]
                    #labels_and_rows[label].append(row)

        if is_rollup and lastbin > 0:
            # Catch the last bin(s) with a second query, as this won't have
            # been rolled up by the database yet.
            # (Influx Continuous Queries only roll up bins that are completely
            # in the past and to avoid overusing memory, we often delay
            # the aggregation until there is sufficient free memory).
            # This will just be a query that aggregates the raw data, starting
            # from where the other query left off (lastbin)

            self.qb2.add_clause("where", 
                    "where time >= {}s and time < {}s and {}".format(
                        lastbin, stop_time, " or ".join([
                        "stream = '{}'".format(stream)
                        for stream in all_streams])))
            querystring, _ = self.qb2.create_query(order)
            try:
                results = self.query(querystring)
                # Append these results to the results we already got
                for (series, tags), generator in results.items():
                    label = self.streams_to_labels[tags["stream"]]
                    for result in generator:
                        row = self._row_from_result(result, label)
                        if row:
                            ts = row['timestamp']
                            if ts not in labels_and_rows[label]:
                                labels_and_rows[label][ts] = row
                            else:
                                for k,v in row.iteritems():
                                    if k not in labels_and_rows[label][ts]:
                                        labels_and_rows[label][ts][k] = v
                                    elif labels_and_rows[label][ts][k] is None:
                                        labels_and_rows[label][ts][k] = row[k]
                        #    labels_and_rows[label].append(row)
            except DBQueryException as e:
                logger.log("Failed to collect last bin, using CQ data only")
                
        for label, rows in labels_and_rows.iteritems():
            if len(rows) == 0:
                yield(None, label, None, None, None)
            else:
                yielding = [v for (k, v) in sorted(rows.items())]
                yield(yielding, label, "binstart", binsize, None)

    def _set_rename(self):
        """Decides whether response will need to be renamed or not"""
        columns = [k[0] for k in self.aggcols]
        self.rename = len(set(columns)) < len(columns)

    def _get_label(self, meas, agg):
        """Gets label for response given measure and aggregation"""
        return meas + "_" + agg if self.rename else meas

    def _get_rollup_functions(self):
        """
        Returns a list of columns to select if ther is no pre-aggregated table
        """

        col_names = []

        for meas, agg in self.aggcols:
            if agg == 'smokearray':
                if meas == "rtts":
                    meas = '"median"'
                col_names += ["percentile({0}, {1}) as \"{1}_percentile_rtt\"".format(
                    meas, i) for i in range(5,100,5)] + ["max({}) as \"max_rtt\"".format(
                        meas)]
            else:
                label = self._get_label(meas, agg)
                if agg == 'avg':
                    agg = 'mean'
                col_names.append("{0}(\"{1}\") AS \"{2}\"".format(agg, meas, label))
        return col_names


    def _get_rollup_columns(self):
        """
        Returns a list of columns to select if there is a pre-aggregated table
        """
        col_names = []
        for meas, agg in self.aggcols:
            label = self._get_label(meas, agg)
            if agg == "avg":
                agg = "mean"
            found_col_name = False
            for col_name, func, original_col in self.cqs:
                # Remove any quotes before comparing column names
                original_col = original_col.strip("\"'")
                if agg == 'smokearray':
                    if func == 'percentile':
                        col_names.append(col_name)
                        found_col_name = True
                    # We also want the max_rtt, so that we can get the 100th percentile
                    if col_name == 'max_rtt':
                        col_names.append(col_name)    
                elif meas == original_col and agg == func:
                    col_names.append("{} AS \"{}\"".format(col_name, label))
                    found_col_name = True
                    break
            if not found_col_name:
                # this would mean someone asked for an aggregation we haven't done
                # refer to unaggregated table
                return None

        return col_names
                
    def _row_from_result(self, result, nntsc_label):
        """
        Fixes up the result to be ready to send by packing up any \
        smoke arrays and removing unwanted empty results
        """

        # Pack up the smoke array to be sent back
        aggs = [k[1] for k in self.aggcols]
        
        if "count" in aggs:
            # Check we got any results
            index = aggs.index("count")
            meas = self.aggcols[index][0]
            label = self._get_label(meas, "count")
            if result[label] == 0:
                # We got no results, so ignore bin
                return {}
        
        if "smokearray" in aggs:
            index = aggs.index("smokearray")
            meas = self.aggcols[index][0]
            num_results = result.get("results", 20)
            if num_results is None or num_results <= 1:
                ntile_range = range(0)
            elif num_results < 20:
                # Don't return more percentiles than we have results
                # This is a bit of a hack.. Would be better to do this
                # in the database if we could, but influx doesn't have the
                # functionality for this
                # We do this by sort of
                # taking the 100/n, (100/n)*2, ... (100/n)*n percentiles
                range_top = 100
                range_step = range_top // num_results
                range_step = range_step - (range_step % 5)
                range_bottom = range_top - range_step * (num_results - 1)
                ntile_range = range(range_bottom, range_top, range_step)
                second_bottom = range_bottom + range_step
            else:
                ntile_range = range(5,100,5)
                range_bottom = 5
                second_bottom = 10

            # Also take the max_rtt, as this acts as the 100 percentile
            percentiles = ["{}_percentile_rtt".format(
                    i) for i in ntile_range] + ["max_rtt"]
            smokearray = []
            for percentile in percentiles:
                if result.get(percentile, None) is not None:
                    smokearray.append(result[percentile])

            for percentile in ["{}_percentile_rtt".format(
                    i) for i in range(5, 100, 5)]:
                del result[percentile]

            #if len(smokearray) > 0 and num_results < 20:
            #    logger.log("num results: {}, smokearray: {}".format(num_results, smokearray))

            # Don't return smoke array if it is empty
            if len(smokearray) == 0:
                smokearray = None
            result[meas] = smokearray

        # Add nntsclabel, and other fields that some results need
        # Cheating by using time (which is really binstart) as binstart, timestamp and
        # min_timestamp, but this doesn't seem to break anything. Influx only offers one
        # timestamp to work with, and times can't be queried, so this is our only option
        result["nntsclabel"] = nntsc_label
        result["timestamp"] = result["time"]
        result["binstart"] = result["time"]
        result["min_timestamp"] = result["time"]
        del result["time"]

        # get rid of loss or result if they are empty or ampweb gets upset
        for key in ["loss", "result"]:
            if key in result and result[key] is None:
                del result[key]
        return result
        
    def _meas_duplicated(self, meas):
        """True if one measure is being aggregated more than once"""
        count = 0
        for agg_meas, _ in self.aggcols:
            if agg_meas == meas:
                count += 1
                if count == 2:
                    return True
        return False

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
