import psycopg2
import psycopg2.extras
from libnntscclient.logger import *
import time, sys


class DBSelector:
    def __init__(self, dbname, dbuser, dbpass=None, dbhost=None):

        connstr = "dbname=%s user=%s" % (dbname, dbuser)
        if dbpass != "" and dbpass != None:
            connstr += " password=%s" % (dbpass)
        if dbhost != "" and dbhost != None:
            connstr += " host=%s" % (dbhost)

        try:
            self.conn = psycopg2.connect(connstr)
        except psycopg2.DatabaseError as e:
            log("DBSelector: Error connecting to database: %s" % e)
            self.conn = None
            self.cursor = None
            return

        try:
            self.cursor = self.conn.cursor(
                    cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError as e:
            log("DBSelector: Failed to create cursor: %s" % e)
            self.cursor = None
            return

    def __del__(self):
        if self.conn:
            self.conn.commit()
        
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


    def list_collections(self):
        if self.cursor == None:
            return []
        
        collections = []

        self.cursor.execute("SELECT * from collections")
        while True:
            row = self.cursor.fetchone()

            if row == None:
                break
            col = {}
            for k,v in row.items():
                col[k] = v
            collections.append(col)
        return collections
        
    def get_collection_schema(self, colid):
        if self.cursor == None:
            return [], []

        self.cursor.execute(
                "SELECT streamtable, datatable from collections WHERE id=%s",
                (colid,))
        
        tables = self.cursor.fetchone()

        # Parameterised queries don't work on the FROM clause -- our table
        # names *shouldn't* be an SQL injection risk, right?? XXX
        self.cursor.execute(
                "SELECT * from %s LIMIT 1" % (tables['streamtable']))
        
        streamcolnames = [cn[0] for cn in self.cursor.description]
              
        self.cursor.execute(
                "SELECT * from %s LIMIT 1" % (tables['datatable']))
        
        datacolnames = [cn[0] for cn in self.cursor.description]
        return streamcolnames, datacolnames         

    def select_streams_by_module(self, mod):
        # Find all streams for a given parent collection, e.g. amp, lpi
        #
        # For each stream:
        #   Form a dictionary containing all the relevant information about
        #   that stream (this will require info from both the combined streams
        #   table and the module/subtype specific table
        # Put all the dictionaries into a list

        # Find the collections matching this module
        self.cursor.execute(
                "SELECT * from collections where module=%s", (mod,))
        streamtables = {}
        
        cols = self.cursor.fetchall()

        for c in cols:
            streamtables[c["id"]] = (c["streamtable"], c["modsubtype"])

        streams = []
        for cid, (tname, sub) in streamtables.items():
            sql = """ SELECT * FROM streams, %s WHERE streams.collection=%s 
                      AND streams.id = %s.stream_id """ % (tname, "%s", tname)
            self.cursor.execute(sql, (cid,))

            while True:
                row = self.cursor.fetchone()
                if row == None:
                    break
                row_dict = {"modsubtype":sub}
                for k,v in row.items():
                    if k == "id":
                        continue
                    row_dict[k] = v
                streams.append(row_dict)
        return streams

    def select_streams_by_collection(self, coll, minid):
        self.cursor.execute(
                "SELECT * from collections where id=%s", (coll,))
        assert(self.cursor.rowcount == 1)

        coldata = self.cursor.fetchone()

        tname = coldata['streamtable']
        sql = """SELECT * FROM streams, %s WHERE streams.id = %s.stream_id
                 AND streams.id > %s""" % (tname, tname, "%s")
        self.cursor.execute(sql, (minid,))

        selected = []
        while True:
            row = self.cursor.fetchone()
            if row == None:
                break
            stream_dict = {}
            for k,v in row.items():
                if k == "id":
                    continue
                stream_dict[k] = v
            selected.append(stream_dict)
        return selected

    def select_aggregated_data(self, col, stream_ids, aggcols, 
            start_time = None, stop_time = None, groupcols = None, 
            binsize = 0, aggregator="avg"):

        # 'aggregator' can take many forms:
        #    It can be a string (in which case, the aggregation function will 
        #    be applied to all of the aggcols).
        #    It can be a list with one string in it (same result as above).
        #    It can be a list with multiple strings. In this case, there must
        #    be one entry in the aggregator list for every entry in the
        #    aggcols list -- the aggregator in position 0 will be applied to 
        #    the column in position 0, etc.

        if type(binsize) is not int:
            return [], 0

        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)

        table, baseparams, columns = \
                self._get_data_table(col, stream_ids, start_time, stop_time)
        if groupcols == None:
            groupcols = ['stream_id']
        elif 'stream_id' not in groupcols:
            groupcols.append('stream_id')
        
        # XXX Could we do this in one sanitise call?
        groupcols = self._sanitise_columns(columns, groupcols, baseparams) 
        aggcols = self._sanitise_columns(columns, aggcols, baseparams)

        labeled_aggcols, aggfuncs = self._apply_aggregation(aggcols, aggregator)
        labeled_groupcols = list(groupcols)

        # Add a column for the maximum timestamp in the bin
        labeled_aggcols.append("max(timestamp) AS timestamp") 

        if binsize == 0 or binsize == (stop_time - start_time):
            # Add minimum timestamp to help with determining frequency
            labeled_aggcols.append("min(timestamp) AS min_timestamp")
            tscol = "min_timestamp"
        else:
            labeled_groupcols.append(\
                    "(timestamp - (timestamp %%%% %u)) AS binstart" \
                    % (binsize)) 
            groupcols.append("binstart")
            tscol = "binstart"

        params = tuple(baseparams + [start_time] + [stop_time] + stream_ids)
        return self._generic_select(table, stream_ids, params,
                labeled_aggcols + labeled_groupcols, groupcols, tscol, binsize)

    def select_data(self, col, stream_ids, selectcols, start_time=None, 
            stop_time=None):

        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)

        table, baseparams, columns = \
                self._get_data_table(col, stream_ids, start_time, stop_time)
        selectcols = self._sanitise_columns(columns, selectcols, baseparams)
        
        if 'stream_id' not in selectcols:
            selectcols.append('stream_id')
        if 'timestamp' not in selectcols:
            selectcols.append('timestamp')

        params = tuple(baseparams + [start_time] + [stop_time] + stream_ids)

        return self._generic_select(table, stream_ids, params, selectcols, 
                None, 'timestamp', 0)

    def _generic_select(self, table, streams, params, selcols, groupcols,
            tscol, binsize):

        selclause = "SELECT "
        for i in range(0, len(selcols)):
            selclause += selcols[i]

            if i != len(selcols) - 1:
                selclause += ", "

        fromclause = " FROM %s " % table
        
        whereclause = self._generate_where(streams)

        if groupcols == None or len(groupcols) == 0:
            groupclause = ""
        else:
            groupclause = " GROUP BY "
            for i in range(0, len(groupcols)):
                groupclause += groupcols[i]
                if i != len(groupcols) - 1:
                    groupclause += ", "

        orderclause = " ORDER BY %s " % (tscol)

        sql = selclause + fromclause + whereclause + groupclause \
                + orderclause

        self.cursor.execute(sql, params)
        return self._form_datadict(selcols, tscol, binsize, streams)

    def _generate_where(self, streams):
        tsclause = " WHERE timestamp >= %s AND timestamp <= %s "

        assert(len(streams) > 0)
        streamclause = " AND ("
        for i in range(0, len(streams)):
            streamclause += "stream_id = %s"
            if i != len(streams) - 1:
                streamclause += " OR "
        streamclause += ")"

        return tsclause + streamclause

    def _get_data_table(self, col, streams, start, stop):
        
        self.cursor.execute(
                "SELECT * from collections where id=%s", (col,))
        assert(self.cursor.rowcount == 1)

        coldata = self.cursor.fetchone()
        tname = coldata['datatable']
        module = coldata['module']
        subtype = coldata['modsubtype']
        
        if module == "amp" and subtype == "traceroute":
            # Special case for amp traceroute, as we use a parameterised
            # function to select data rather than a single table 
            table = "select_amp_traceroute(%s, %s, %s)"
            params = [streams] + [start] + [stop]
        else:
            table = tname
            params = []
            
        self.cursor.execute(
                "SELECT * from information_schema.columns WHERE table_name=%s",
                (tname,))

        columns = []
        while True:
            row = self.cursor.fetchone()
            if row == None:
                break

            columns.append(row['column_name'])
        return table, params, columns

    def _sanitise_columns(self, columns, selcols, params):
        # Don't let anyone try to select on columns that aren't actually
        # in the data table -- this is mainly to prevent a user from asking
        # us to select on the column containing the string ';drop table X;'
        # which would be very bad.

        sanitised = []
        for cn in selcols:
            if cn in columns:
                sanitised.append(cn)
        return sanitised

    def _apply_aggregation(self, selectors, aggregator):
        
        if type(aggregator) is str:
            # Only one aggregator, use it for all columns
            aggfuncs = [aggregator] * len(selectors)
        elif len(aggregator) == 1:
            aggfuncs = aggregator * len(selectors)
        else:
            aggfuncs = []
            for agg in aggregator:
                aggfuncs.append(agg)

        # Ensure we have one aggregator per column
        if None in aggfuncs or len(aggfuncs) != len(selectors):
            return [], []

        index = 0
        rename = False
        aggcols = []

        # If we have duplicates in the select column list, we'll need
        # to rename them to differentiate them based on the aggregation
        # function applied to them
        if len(set(selectors)) < len(selectors):
            rename = True

        for colname in selectors:
            labelstr = colname
            if rename:
                labelstr += "_" + aggfuncs[index]

            colclause = "%s(%s) AS %s" % (aggfuncs[index], colname, labelstr)
            aggcols.append(colclause)
            index += 1

        return aggcols, aggfuncs

    def _form_datadict(self, selectcols, tscol, size, streams):
        """ Converts a result object into a list of dictionaries, one
            for each row. The dictionary is a map of column names to
            values.

            Also applies some heuristics across the returned result to try
            and determine the size of each bin (if data has been aggregated).
        """
        data = []
        perstream = {}

        # Long and complicated explanation follows....
        #
        # We need to know the 'binsize' so that we can determine whether
        # there are missing measurements in the returned result. This is
        # useful for leaving gaps in our graphs where data was missing.
        #
        # The database will give us results that are binned according to the
        # requested binsize, but this binsize may be smaller than the
        # measurement frequency. If it is, we can't use the requested binsize
        # to determine whether a measurement is missing because there will be
        # empty bins simply because no measurement fell within that time
        # period.
        #
        # Instead, we actually want to know the measurement frequency or
        # the binsize, whichever is bigger. The problem is that we don't have
        # any obvious way of knowing the measurement frequency, so we have to
        # infer it.
        #
        # Each row in the result object corresponds to a bin. For
        # non-aggregated data, the bin will always only cover one data
        # measurement.
        #
        # There are two timestamps associated with each result row:
        #
        #   'binstart' is the timestamp where a bin begins and is calculated
        #   based on the requested bin size. For non-aggregated data, this is
        #   the same as the timestamp of the data point.
        #
        #   'timestamp' is the timestamp of the *last* measurement included in
        #   the bin. This is the timestamp we use for plotting graphs.
        #
        #
        # There are two main cases to consider:
        #   1. The requested binsize is greater than or equal to the
        #      measurement frequency. In this case, use the requested binsize.
        #   2. The requested binsize is smaller than the measurement frequency.
        #      In this case, we need to use the measurement frequency.
        #
        # In case 1, the vast majority of bins are going to be separated by
        # the requested binsize. So we can detect this case by looking at the
        # number of occasions the time difference between bins (using
        # 'binstart' matches the binsize that we requested.
        #
        # Case 2 is trickier. Once we rule out case 1, we need to guess what
        # the measurement frequency is. Fortunately, we know that each bin
        # can only contain 1 measurement at most, so we can use the
        # 'timestamp' field from consecutive bins to infer the frequency.
        # We collect these time differences and use the mode of these values
        # as our measurement frequency. This will work even if the requested
        # binsize is not a factor of the measurement frequency.
        #
        # XXX Potential pitfalls
        # * What if there are a lot of non-consecutive missing measurements?
        # * What if the test changes its measurement frequency?

        while True:
            r = self.cursor.fetchone()
            if r == None:
                break

            streamid = r['stream_id']
            if streamid not in perstream:
                perstream[streamid] = { \
                        'lastts': 0,
                        'lastbin': 0,
                        'perfectbins': 0,
                        'total_diffs': 0,
                        'tsdiffs': {}
                }


            # Collecting data for our binsize heuristics
            if perstream[streamid]['lastts'] == 0:
                perstream[streamid]['lastts'] = r['timestamp']
                perstream[streamid]['lastbin'] = r[tscol]
            elif perstream[streamid]['lastts'] != r['timestamp']:
                tsdiff = r['timestamp'] - perstream[streamid]['lastts']
                bindiff = r[tscol] - perstream[streamid]['lastbin']

                # Difference between bins matches our requested binsize
                if bindiff == size:
                    perstream[streamid]['perfectbins'] +=1

                if tsdiff in perstream[streamid]['tsdiffs']:
                    perstream[streamid]['tsdiffs'][tsdiff] += 1
                else:
                    perstream[streamid]['tsdiffs'][tsdiff] = 1

                perstream[streamid]['total_diffs'] += 1
                perstream[streamid]['lastts'] = r['timestamp']
                perstream[streamid]['lastbin'] = r[tscol]

            datadict = {}
            for k,v in r.items():
                datadict[k] = v
            data.append(datadict)

        frequencies = {}

        for streamid in streams:
            if streamid not in perstream:
                frequencies[streamid] = 0
                continue

            if perstream[streamid]['total_diffs'] == 0:
                if size < 300:
                    freq = 300
                else:
                    freq = size
                
                frequencies[streamid] = freq
                continue

            # If this check passes, we requested a binsize greater than the
            # measurement frequency (case 1, above)
            if perstream[streamid]['perfectbins'] / \
                    float(perstream[streamid]['total_diffs']) > 0.9:
                frequencies[streamid] = size
                continue

            # If we get here, then our binsize is more than likely smaller
            # than our measurement frequency. Now, we need to try and figure
            # out what that frequency was...

            # Try and set a sensible default frequency. In particular, let's
            # not set the frequency too low, otherwise our graphs will end up
            # lots of gaps if we get it wrong.
            if size < 300:
                freq = 300
            else:
                freq = size

            # Find a suitable mode in all the timestamp differences.
            # I require a strong mode, i.e. at least half the differences
            # must be equal to the mode, as there shouldn't be a lot of
            # variation in the time differences (unless your measurements are
            # very patchy).
            for td, count in perstream[streamid]['tsdiffs'].items():
                if count >= 0.5 * perstream[streamid]['total_diffs']:
                    freq = td
                    break
            frequencies[streamid] = freq
            
        return data, frequencies


    def select_percentile_data(self, col, stream_ids, ntilecols, othercols, 
            start_time = None, stop_time = None, binsize=0, 
            ntile_aggregator = "avg", other_aggregator = "avg"):

        # This is a horrible looking function, but we are trying to construct
        # a pretty complicated query
        #
        # The main use case for this code is to generate "smokeping" style
        # data from AMP latency measurements. AMP measurements have only one
        # result but are conducted more frequently than smokeping, so we can
        # group together nearby measurements to approximate the N pings that
        # Smokeping does.
        #
        # In that case, we're trying to generate a query that looks roughly
        # like (sub in BINSIZE, DATATABLE, STREAMS, START, END as appropriate):
        #   SELECT max(recent) AS timestamp,
        #       binstart,
        #       array_agg(rtt_avg) AS values,
        #       avg(loss_avg) AS loss,  
        #       stream_id
        #   FROM (
        #       SELECT max(timestamp) AS recent,
        #           binstart,
        #           coalesce(avg(rtt), ARRAY[]::text[]) AS rtt_avg,
        #           avg(loss) AS loss_avg,
        #           stream_id
        #       FROM (
        #           SELECT timestamp,
        #               stream_id,
        #               timestamp-timestamp%BINSIZE AS binstart,
        #               rtt,
        #               loss,
        #               ntile(20) OVER (
        #                   PARTITION BY timestamp-timestamp%BINSIZE
        #                   ORDER BY rtt
        #               )
        #           FROM DATATABLE
        #           WHERE stream_id in STREAMS AND timestamp >= START AND
        #               timestamp <= END
        #           ORDER BY binstart
        #       ) AS ntiles
        #       GROUP BY binstart, ntile, stream_id ORDER BY binstart, rtt_avg
        #   ) AS agg
        #   GROUP BY binstart, stream_id ORDER BY binstart"
        #
        # We only support percentiles across a single column at the moment.
        # If more than one column is in the list, we ignore every one except
        # the first one.
        #
        # You can have multiple supporting columns (othercols) and the 
        # aggregation functions can be something other than 'avg' if desired.

        if type(binsize) is not int:
            return [], {}

        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)
       
        # Let's just limit ourselves to 1 set of percentiles for now, huh
        if len(ntilecols) != 1:
            ntilecols = ntilecols[0:1]

        table, baseparams, columns = \
                self._get_data_table(col, stream_ids, start_time, stop_time)
        ntilecols = self._sanitise_columns(columns, ntilecols, baseparams)
        othercols = self._sanitise_columns(columns, othercols, baseparams)

        labeledntilecols, ntileaggfuncs = \
                self._apply_aggregation(ntilecols, ntile_aggregator)
        labeledothercols, otheraggfuncs = \
                self._apply_aggregation(othercols, other_aggregator)
       
        # Constructing the innermost SELECT query, which lists the ntile for
        # each measurement
        sql_ntile = """ SELECT stream_id, timestamp, 
                        timestamp - (timestamp %%%% %d) AS binstart, """ \
                % (binsize)
        
        initcols = ntilecols + othercols
        for i in range(0, len(initcols)):
            sql_ntile += initcols[i]
            sql_ntile += ", "
        
        # XXX rename ntile to something unique if we support multiple
        # percentile columns
        sql_ntile += "ntile(20) OVER ( PARTITION BY "
        sql_ntile += "timestamp - (timestamp %%%% %d)" % (binsize)
        sql_ntile += " ORDER BY %s )" % (ntilecols[0])
        
        sql_ntile += " FROM %s " % (table)

        where_clause = self._generate_where(stream_ids)

        sql_ntile += where_clause
        sql_ntile += " ORDER BY binstart "

        # Constructing the middle SELECT query, which will aggregate across
        # each ntile to find the average value within each ntile
        sql_agg = "SELECT stream_id, max(timestamp) AS recent, "

        for l in labeledntilecols:
            sql_agg += l + ", "
        for l in labeledothercols:
            sql_agg += l + ", "

        sql_agg += "binstart FROM ( %s ) AS ntiles" % (sql_ntile)
        sql_agg += " GROUP BY stream_id, binstart, ntile ORDER BY binstart, "

        # Extracting the labels for the ntile columns for the ORDER BY clause
        for i in range(0, len(labeledntilecols)):
            labelsplit = labeledntilecols[i].split("AS")
            assert(len(labelsplit) == 2)

            sql_agg += labelsplit[1].strip()
            if i != len(labeledntilecols) - 1:
                sql_agg += ", "

        # Finally, construct the outermost SELECT which will collapse the
        # ntiles into an array so we get one row per bin with all of the
        # ntiles in a single "values" column
        qcols = ["timestamp", "binstart", "stream_id"]
        sql = "SELECT stream_id, max(recent) AS timestamp, "
        
        # Again, horrible label splitting so that we use the right column
        # names
        for i in range(0, len(labeledntilecols)):
            labelsplit = labeledntilecols[i].split("AS")
        
            # This is nasty -- we often end up with null entries at the end
            # of our array as part of the ntile process. We can drop these by
            # casting the array to a string and then back again.
            # The coalesce will ensure we return an empty array in cases where
            # we have no data.

            # XXX Need unique names if we ever support mulitple ntiles 
            sql += "coalesce(string_to_array(string_agg(cast(%s AS TEXT), ','), ','), ARRAY[]::text[]) AS values, " % (labelsplit[1].strip())
            qcols.append("values")
         
        for i in range(0, len(labeledothercols)):
            labelsplit = labeledothercols[i].split("AS")

            assert(len(labelsplit) == 2)
            sql += "%s(%s) AS %s, " % (otheraggfuncs[i], \
                    labelsplit[1].strip(), othercols[i])
            qcols.append(othercols[i])

        sql += "binstart FROM (%s) AS agg GROUP BY binstart, stream_id ORDER BY binstart" % (sql_agg)
            
        # Execute our query!
        params = tuple(baseparams + [start_time] + [stop_time] + stream_ids)

        self.cursor.execute(sql, params)
        return self._form_datadict(qcols, "binstart", binsize, stream_ids)

            
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
