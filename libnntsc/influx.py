#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#          Andy Bell
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



from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from libnntsc.dberrorcodes import *
from libnntsc.querybuilder import QueryBuilder
from libnntsc.cqs import getMatrixCQ, get_parser
import libnntscclient.logger as logger
from requests import ConnectionError
import requests
import math

DEFAULT_RP = "default"
MATRIX_LONG_RP = "matrixlong"
MATRIX_SHORT_RP = "matrixshort"


requests.packages.urllib3.disable_warnings()

class InfluxConnection(object):
    """A class to represent a connection to an Influx Database"""
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost="localhost",
                 dbport="8086", timeout=None, cachetime=0):

        self.dbname = dbname

        if dbhost == "" or dbhost is None:
            dbhost = "localhost"

        if dbport == "" or dbport is None:
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

    def query(self, query):
        """Returns ResultSet object"""
        try:
            return self.client.query(query, epoch='s')
        except Exception as e:
            print query
            self.handler(e)

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

class InfluxInsertor(InfluxConnection):
    """
    A class for inserting data into the influx database
    """
    def __init__(self, dbname, user, password, host, port, timeout=None):
        super(InfluxInsertor, self).__init__(dbname, user, password, host, port, timeout)
        self.to_write = []

    def commit_data(self, retention_policy=DEFAULT_RP):
        """Send all data that has been observed"""
        try:
            self.client.write_points(self.to_write, time_precision="s",
                                     retention_policy=retention_policy)
            self.to_write = []
        except Exception as e:
            self.handler(e)

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

    def create_matrix_cq(self, aggcols, measurement):
        daycqname = measurement + "_matrix_day"
        shortcqname = measurement + "_matrix_short"
        aggstring = ",".join(
                ["{}({}) AS {}".format(cq[1], cq[0], cq[2]) for cq in aggcols])

        aggstring = ""
        countcols = []
        for cq in aggcols:
            if aggstring != "":
                aggstring += ", "

            aggstring += "{}({}) AS {}".format(cq[1], cq[0], cq[2])
            if cq[0] not in countcols:
                countcols.append(cq[0])
                aggstring += ", count({}) AS magiccount_{}".format(cq[0], cq[0].replace('"', ''))

        query = """ DROP CONTINUOUS QUERY {0} ON {1}
                """.format(daycqname, self.dbname)
        self.query(query)

        query = """ DROP CONTINUOUS QUERY {0} ON {1}
                """.format(shortcqname, self.dbname)
        self.query(query)

        query = """
            CREATE CONTINUOUS QUERY {0} ON {1} RESAMPLE EVERY 1h FOR 3h BEGIN
            SELECT {2} INTO "{3}".{0} FROM {4} GROUP BY stream,time(1h) END
            """.format(daycqname, self.dbname, aggstring, "matrixlong",
                    measurement)
        self.query(query)

        query = """
            CREATE CONTINUOUS QUERY {0} ON {1} RESAMPLE EVERY 1m FOR 15m BEGIN
            SELECT {2} INTO "{3}".{0} FROM {4} GROUP BY stream,time(1m) END
            """.format(shortcqname, self.dbname, aggstring, "matrixshort",
                    measurement)
        self.query(query)

    def create_retention_policies(self, keepdata):
        """Create retention policies at given length"""
        try:
            rps = self.client.get_list_retention_policies()
            default_exists = False
            matrixshort_exists = False
            matrixlong_exists = False
            extra_rps = []

            for rp in rps:
                if rp["name"] == DEFAULT_RP:
                    default_exists = True
                elif rp["name"] == MATRIX_SHORT_RP:
                    matrixshort_exists = True
                elif rp["name"] == MATRIX_LONG_RP:
                    matrixlong_exists = True
                else:
                    extra_rps.append(rp["name"])

            if default_exists:
                self.client.alter_retention_policy(
                    DEFAULT_RP, duration=keepdata, default=True)
            else:
                self.client.create_retention_policy(
                    DEFAULT_RP, duration=keepdata, replication=1, default=True)

            if matrixlong_exists:
                self.client.alter_retention_policy(
                    MATRIX_LONG_RP, duration="48h")
            else:
                self.client.create_retention_policy(
                    MATRIX_LONG_RP, duration="48h", replication=1)

            if matrixshort_exists:
                self.client.alter_retention_policy(
                    MATRIX_SHORT_RP, duration="2h")
            else:
                self.client.create_retention_policy(
                    MATRIX_SHORT_RP, duration="2h", replication=1)

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
        self.qb.add_clause("select", "select " + ', '.join('"{0}"'.format(s) for s in selectcols))

        self.qb.add_clause("from", "from {}".format(table))

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)
                continue

            fluxstreams = streams

            if len(fluxstreams) == 0:
                continue

            self.qb.add_clause(
                    "where", "where time >= {}s and time < {}s and {}".format(
                            start_time, stop_time, " or ".join([
                            "stream = '{}'".format(s) for s in fluxstreams])))

            order = ["select", "from", "where"]
            querystring, _ = self.qb.create_query(order)
            try:
                results = self.query(querystring)
            except DBQueryException as e:
                yield(None, label, None, None, e)

            rows = []
            for (table, tags), results in results.iteritems():
                for result in results:
                    result["nntsclabel"] = label
                    result["timestamp"] = result["time"]
                    del result["time"]
                    rows.append(result)

            yield(rows, label, "timestamp", 0, None)


    def _was_stream_active(self, sid, table, start, end):

        field = get_parser(table).get_random_field(None)

        querystring = "SELECT count({}) FROM {} WHERE stream='{}' AND time >= {}s AND time <= {}s GROUP BY time(1h) fill(none) LIMIT 1".format( \
                field, table, sid, start, end)

        try:
            results = self.query(querystring)
        except DBQueryException as e:
            return False

        for (table, tags), results in results.iteritems():
            for r in results:
                if r['count'] > 0:
                    return True

        return False


    def get_last_timestamp(self, table, sid):

        lasthour = None
        field = get_parser(table).get_random_field(None)

        querystring = "SELECT count({}) FROM {} WHERE stream='{}' AND time >= now() - 3d GROUP BY time(1h) fill(none)".format( \
                field, table, sid)

        try:
            results = self.query(querystring)
        except DBQueryException as e:
            return None

        for (tbl, tags), results in results.iteritems():
            for r in results:
                if r['count'] > 0:
                    if lasthour is None or r['time'] > lasthour:
                        lasthour = r['time']

        if lasthour is None:
            return None

        querystring = "SELECT last({}) from {} WHERE stream='{}' AND time >= {}".format( \
                field, table, sid, lasthour)

        try:
            results = self.query(querystring)
        except DBQueryException as e:
            return None

        for (tbl, tags), results in results.iteritems():
            for r in results:
                return r['time'] / 1000000000

        return None


    def select_matrix_data(self, table, labels, start_time, stop_time):
        mcq = getMatrixCQ(table)


        if stop_time - start_time >= (60 * 60):
            fetchtable = MATRIX_LONG_RP + "." + table + "_matrix_day"
            if start_time % (60 * 60) < 2 * 60:
                start_time -= (60 * 60)
            start_time -= (start_time % (60 * 60))
        else:
            fetchtable = MATRIX_SHORT_RP + "." + table + "_matrix_short"
            start_time -= (start_time % 60)

        all_streams = []
        self.streams_to_labels = {}

        results = []
        labels_and_rows = {}

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                results.append({'data': None, 'label': label})
            else:
                labels_and_rows[label] = {}
                for stream in streams:
                    all_streams.append(stream)
                    self.streams_to_labels[str(stream)] = label

        if len(all_streams) == 0:
            return

        streamor = " OR ".join(["stream='{}'".format(s) for s in all_streams])

        query = """
                SELECT * FROM {0} WHERE time >= {1}s AND time < {2}s AND ({3})
                """.format(fetchtable, start_time, stop_time, streamor)

        try:
            mdata = self.query(query)
        except DBQueryException as e:
            return

        for (tbl, tags), data in mdata.iteritems():
            for row in data:
                if row['stream'] not in self.streams_to_labels:
                    continue
                lab = self.streams_to_labels[row['stream']]

                for col, value in row.iteritems():
                    if col in ['stream', 'time']:
                        continue
                    if col not in labels_and_rows[lab]:
                        labels_and_rows[lab][col] = [value]
                    else:
                        labels_and_rows[lab][col].append(value)

        for k, v in labels_and_rows.iteritems():
            finaldata = {'binstart': start_time, 'timestamp': stop_time}
            for cq in mcq:
                cq = (cq[0].replace('"', ''), cq[1], cq[2].replace('"', ''))
                if cq[2] not in v:
                    continue
                if cq[1] in ["sum", "count"]:
                    total = 0
                    for i in range(0, len(v[cq[2]])):
                        if v[cq[2]][i] is not None:
                            total += v[cq[2]][i]

                    finaldata[cq[2]] = total
                elif cq[1] in ['max']:
                    maxval = None
                    for i in range(0, len(v[cq[2]])):
                        if v[cq[2]][i] is not None and (maxval is None or v[cq[2]][i] > maxval):
                            maxval = v[cq[2]][i]
                    finaldata[cq[2]] = maxval
                elif cq[1] in ['min']:
                    minval = None
                    for i in range(0, len(v[cq[2]])):
                        if v[cq[2]][i] is not None and (minval is None or v[cq[2]][i] < minval):
                            minval = v[cq[2]][i]
                    finaldata[cq[2]] = minval
                elif cq[1] in ["avg", "mean"]:
                    countcol = labels_and_rows[k]["magiccount_" + cq[0]]

                    summean = 0.0
                    sumn = 0
                    for i in range(0, len(v[cq[2]])):
                        if countcol[i] is not None and v[cq[2]][i] is not None:
                            summean += (v[cq[2]][i] * countcol[i])
                            sumn += countcol[i]

                    if sumn > 0:
                        finaldata[cq[2]] = summean / sumn
                    else:
                        finaldata[cq[2]] = None
                elif cq[1] in ['stddev']:
                    countcol = labels_and_rows[k]["magiccount_" + cq[0]]

                    sumvar = 0.0
                    sumn = 0
                    for i in range(0, len(v[cq[2]])):
                        if v[cq[2]][i] is not None and countcol[i] > 1:
                            sumvar += (pow(v[cq[2]][i], 2) * countcol[i])
                            sumn += countcol[i]

                    if sumn > 0:
                        finaldata[cq[2]] = math.sqrt(sumvar / sumn)
                    else:
                        finaldata[cq[2]] = None
                elif cq[1] in ['most', 'mode']:
                    seen = {}
                    mode = None
                    modefreq = 0
                    countcol = labels_and_rows[k]["magiccount_" + cq[0]]

                    for i in range(0, len(v[cq[2]])):
                        if countcol[i] is None:
                            continue
                        if v[cq[2]][i] not in seen:
                            seen[v[cq[2]][i]] = countcol[i]
                        else:
                            seen[v[cq[2]][i]] += countcol[i]

                        if seen[v[cq[2]][i]] > modefreq:
                            mode = v[cq[2]][i]
                            modefreq = seen[v[cq[2]][i]]

                    if mode is not None:
                        finaldata[cq[2]] = float(mode)

            results.append({'data': finaldata, 'label': k})

        for r in results:
            yield([r['data']], r['label'], 'binstart', stop_time - start_time,
                    None)



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

        self.table = table
        self.aggcols = aggcols

        self._set_rename()

        # Change "smoke" to "smokearray", because dns asks for this
        for i, (meas, agg) in enumerate(self.aggcols):
            if agg == "smoke":
                self.aggcols[i] = (meas, "smokearray")
            # Dirty hack to deal with the fact that influx doesn't let us
            # do the aggregation on the timestamp column that we used to
            # use in postgres. Once we switch over to influx properly, we
            # can update ampy to ask for 'requests' directly.
            if meas == "timestamp" and agg == "count":
                self.aggcols[i] = ("requests", agg)

        columns = self._get_rollup_functions()

        self.qb.add_clause("from", "from {}".format(table))
        if stop_time - start_time > binsize:
            self.qb.add_clause("group_by", "group by stream, time({}s)".format(binsize))
        else:
            self.qb.add_clause("group_by", "group by stream")

        self.qb.add_clause("select", "select {}".format(", ".join(columns)))

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
        self.qb.add_clause("where",
                "where time >= {}s and time < {}s and ({})".format(
                        start_time, stop_time, " or ".join([
                        "stream = '{}'".format(stream)
                                for stream in all_streams])))

        order = ["select", "from", "where", "group_by"]
        querystring, _ = self.qb.create_query(order)

        results = self.query(querystring)

        #print querystring

        # Update the labels of the results
        for (series, tags), generator in results.iteritems():
            for result in generator:
                label = self.streams_to_labels[tags["stream"]]
                row = self._row_from_result(result, label)
                if row:
                    ts = row['timestamp']
                    if ts not in labels_and_rows[label]:
                        labels_and_rows[label][ts] = row
                    else:
                        for k, v in row.iteritems():
                            if k not in labels_and_rows[label][ts]:
                                labels_and_rows[label][ts][k] = v
                            elif labels_and_rows[label][ts][k] is None:
                                labels_and_rows[label][ts][k] = row[k]
                    #labels_and_rows[label].append(row)

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
            else:
                ntile_range = range(5,100,5)
                range_bottom = 5

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
            label = self._get_label(meas, "smoke")
            result[label] = smokearray

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
