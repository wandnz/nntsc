# This file is part of NNTSC
#
# Copyright (C) 2013 The University of Waikato, Hamilton, New Zealand
# Authors: Shane Alcock
#          Brendon Jones
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
import libnntscclient.logger as logger
from libnntsc.cqs import build_cqs, get_cqs
from requests import ConnectionError
import time

DEFAULT_RP = "default"
ROLLUP_RP = "rollups"

class InfluxConnection(object):
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost="localhost",
                 dbport="8086", timeout=None):

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

    def query(self, query):
        try:
            return self.client.query(query, epoch='s').get_points()
        except Exception as e:
            self.handler(e)
        
    def handler(self, db_exception):
        try:
            raise db_exception
        except InfluxDBClientError as e:
            #logger.log(e.message)
            raise DBQueryException(DB_GENERIC_ERROR)
        except ConnectionError as e:
            #logger.log(e.message)
            raise DBQueryException(DB_QUERY_TIMEOUT)
        except Exception as e:
            raise e

class InfluxInsertor(InfluxConnection):
    def __init__(self, dbname, user, password, host, port, timeout=None):
        super(InfluxInsertor, self).__init__(dbname, user, password, host, port, timeout)
        cqs_in_db = self.query("show continuous queries")
        self.cqs_in_db = [cq["name"] for cq in cqs_in_db]

    def insert_data(self, tablename, stream, ts, result, casts = {},
                    retention_policy=DEFAULT_RP):

        for cast in casts:
            if cast in result.keys():
                result[cast] = casts[cast](result[cast])
        try:
            self.client.write_points([
                {
                    "measurement": tablename,
                    "tags":{
                        "stream": stream
                    },
                    "time":ts,
                    "fields": result
                }
            ],
            time_precision="s",
            retention_policy=retention_policy)
        except Exception as e:
            self.handler(e)

    def build_cqs(self, postgresdb, retention_policy=DEFAULT_RP):
        build_cqs(self, retention_policy)

    def destroy_cqs(self, retention_policy=DEFAULT_RP):
        for query in self.cqs_in_db:
            try:
                self.client.query("drop continuous query {} on {}".format(query, self.dbname))
            except InfluxDBClientError:
                logger.log("Failed to drop CQ {} from {}. May not exist".format(query, self.dbname))
        self.cqs_in_db = []
        
    def create_cqs(self, cqs, measurement, retention_policy=DEFAULT_RP):
        for agg_group in cqs:
            for time in agg_group[0]:
                try:
                    self.create_cq(agg_group[1], measurement, time, retention_policy)
                except DBQueryException as e:
                    logger.log("Failed to create CQ {}_{}. May already exist".format(
                        measurement, time))
        
    def create_cq(self, aggregations, measurement, time, retention_policy=DEFAULT_RP):
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
        try:
            self.client.drop_database(self.dbname)
            self.client.create_database(self.dbname)
            self.cqs_in_db = []
        except Exception as e:
            self.handler(e)
        
class InfluxSelector(InfluxConnection):
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

    def select_data(self, table, labels, selectcols, start_time, stop_time):

        self.qb.reset()

        self.qb.add_clause("select", "select {}".format(", ".join(selectcols)))
        
        self.qb.add_clause("from", "from {}".format(table))
        
        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)

            else:
                self.qb.add_clause("where", "where time > {}s and time < {}s and {}".format(
                    start_time, stop_time, " or ".join([
                        "stream = '{}'".format(stream) for stream in streams])))

                order = ["select","from","where"]
                querystring, _ = self.qb.create_query(order)
                try:
                    results = self.query(querystring)
                except DBQueryException as e:
                    yield(None, label, None, None, e)

                rows = []
                
                for result in results:
                    result["nntsclabel"] = label
                    result["timestamp"] = result["time"]
                    del result["time"]
                    rows.append(result)
                    
                yield(rows, label, "timestamp", 0, None)
                
        
    def select_aggregated_data(self, table, labels, aggcols, start_time,
                               stop_time, groupcols, binsize):
        """
        Selects aggregated data from a given table, within parameters.
        """
        self.qb.reset()
        self._set_influx_binsize(binsize)

        self.table = table
        self.aggcols = aggcols
        self._set_rename()

        now = int(time.time())
        lastbin = now - (now % binsize) - binsize
        # Check if we're requesting at least one bin, otherwise query
        # the raw data
        if start_time >= lastbin:
            is_rollup = False
        else:
            # Also check if we have an appropriate rollup table
            is_rollup = self._is_rollup()

        columns = None
        if is_rollup:
            self.cqs = get_cqs(self.table, self.influx_binsize)
            columns = self._get_rollup_columns()            

        if columns is None:
            is_rollup = False
            columns = self._get_rollup_functions()
            self.qb.add_clause("from", "from {}".format(table))
            self.qb.add_clause("group_by", "group by stream, time({})".format(self.influx_binsize))
        else:
            self.qb.add_clause("from", "from {}.{}_{}".format(
                ROLLUP_RP, self.table, self.influx_binsize))
            self.qb.add_clause("group_by", "group by stream")
            # Second query is for last bin of data which may not have been collected
            self.qb2.add_clause("from", "from {}".format(table))
            self.qb2.add_clause("group_by", "group by stream, time({})".format(self.influx_binsize))
            columns2 = self._get_rollup_functions()
            self.qb2.add_clause("select", "select {}".format(", ".join(columns2)))
        
        self.qb.add_clause("select", "select {}".format(
            ", ".join(columns)))

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)

            else:
                self.qb.add_clause("where", "where time > {}s and time < {}s and {}".format(
                    start_time, lastbin if is_rollup else stop_time, " or ".join([
                        "stream = '{}'".format(stream) for stream in streams])))

                order = ["select","from","where","group_by"]
                querystring, _ = self.qb.create_query(order)
                try:
                    results = self.query(querystring)
                except DBQueryException as e:
                    yield(None, label, None, None, e)
                
                rows = []
                for result in results:
                    row = self._row_from_result(result, label)
                    if row:
                        rows.append(row)

                if is_rollup:
                    self.qb2.add_clause("where", "where time > {}s and time < {}s and {}".format(
                        lastbin, stop_time, " or ".join([
                            "stream = '{}'".format(stream) for stream in streams])))
                    querystring, _ = self.qb2.create_query(order)
                    try:
                        results = self.query(querystring)
                        row = self._row_from_result(results.next(), label)
                        if row:
                            rows.append(row)
                    except DBQueryException as e:
                        logger.log("Failed to collect last bin, using CQ data only")
#                if len(rows) > 1:
#                    logger.log("Result: [{},...]".format(rows[0]))
#                else:
#                    logger.log("Result: {}".format(rows))
                yield(rows, label, "binstart", binsize, None)

    def _set_rename(self):
        columns = [k[0] for k in self.aggcols]
        self.rename = len(set(columns)) < len(columns)
    
    def _get_label(self, meas, agg):
        return meas + "_" + agg if self.rename else meas
                
    def _get_rollup_functions(self):
        """
        Returns a list of columns to select if ther is no pre-aggregated table
        """
        
        col_names = []
        
        for meas, agg in self.aggcols:
            if agg == 'smokearray':
                col_names += ["percentile({0}, {1}) as \"{1}_percentile_rtt\"".format(
                    meas, i) for i in range(5,100,5)]
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
            index = aggs.index("count")
            meas = self.aggcols[index][0]
            label = self._get_label(meas, "count")
            if result[label] == 0:
                # We got no results, so ignore bin
                return {}
        
        if "smokearray" in aggs:
            index = aggs.index("smokearray")
            meas = self.aggcols[index][0]
            percentiles = ["{}_percentile_rtt".format(
                    i) for i in range(5,100,5)]
            smokearray = []
            for percentile in percentiles:
                if result[percentile] is not None:
                    smokearray.append(result[percentile])
                del result[percentile]
            
            if len(smokearray) == 0:
                smokearray = None
            result[meas] = smokearray
        
        result["nntsclabel"] = nntsc_label
        result["timestamp"] = result["time"]
        result["binstart"] = result["time"]
        result["min_timestamp"] = result["time"]
        del result["time"]

        # get rid of loss or result if they are empty
        for key in ["loss", "result"]:
            if key in result and result[key] is None:
                del result[key]
        
        return result
        
    def _meas_duplicated(self, meas):
        count = 0
        for agg_meas, _ in self.aggcols:
            if agg_meas == meas:
                count += 1
                if count == 2:
                    return True
        return False
                
    def _is_rollup(self):
        """Check to see if there is an aggregated table available"""
        return bool(self.query("show measurements with measurement = {}_{}".format(
                self.table, self.influx_binsize)).next())
        
        
    def _set_influx_binsize(self, binsize):
        """Returns a string representing the binsize in largest unit possible"""
        if binsize % 86400 == 0:
            # convert to days
            self.influx_binsize = "{}d".format(binsize/86400)
        elif binsize % 3600 == 0:
            # convert to hours
            self.influx_binsize = "{}h".format(binsize/3600)
        elif binsize % 60 == 0:
            # convert to minutes
            self.influx_binsize = "{}m".format(binsize/60)
        else:
            # leave as seconds
            self.influx_binsize = "{}s".format(binsize)
