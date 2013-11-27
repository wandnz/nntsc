import psycopg2
import psycopg2.extras
from libnntscclient.logger import *
import time, sys

# Class used for querying the NNTSC database.
# Uses psycopg2 rather than SQLAlchemy for the following reasons:
#  * simple to understand and use
#  * supports parameterised queries
#  * named cursors allow us to easily deal with large result sets
#  * documentation that makes sense

class DBSelector:
    def __init__(self, uniqueid, dbname, dbuser, dbpass=None, dbhost=None):

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
            self.basiccursor = None
            self.datacursor = None
            return

        # The basiccursor is used for all "short" queries that are
        # unlikely to produce a large result. The full result will be
        # sent to us and held in memory.
        #
        # The main advantage of using a client-side cursor is that
        # we can re-use it without having to recreate it after each
        # query.
        try:
            self.basiccursor = self.conn.cursor(
                    cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError as e:
            log("DBSelector: Failed to create basic cursor: %s" % e)
            self.basiccursor = None
            return

        # The datacursor is used for querying the time series data tables.
        # It is a named server-side cursor which means that the results
        # will be sent back to the DBSelector in small chunks as required.
        #
        # Because the cursor is on the database itself, it uses a minimal
        # amount of memory even for large result sets. There will be some
        # additional overhead due to periodically fetching more results
        # from the database but in most use cases, the database and the
        # DBSelector are located on the same host so this should not be
        # a major issue.
        self.datacursor = None
        self.cursorname = "cursor_" + uniqueid

    def _reset_cursor(self):
        if self.conn == None:
            return

        # Re-create the datacursor as it seems you can't just re-use a
        # a named cursor for multiple queries.
        if self.datacursor:
            self.datacursor.close()

        try:
            self.datacursor = self.conn.cursor(self.cursorname,
                    cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError as e:
            log("DBSelector: Failed to create data cursor: %s" % e)
            self.datacursor = None
            return

    def __del__(self):
        self.close()

    def close(self):

        if self.datacursor:
            self.datacursor.close()
        if self.basiccursor:
            self.basiccursor.close()
        if self.conn:
            self.conn.close()

        self.basiccursor = None
        self.datacursor = None
        self.conn = None


    def list_collections(self):
        """ Fetches all of the collections that are supported by this
            instance of NNTSC.
        """
        if self.basiccursor == None:
            return []

        collections = []

        self.basiccursor.execute("SELECT * from collections")
        while True:
            row = self.basiccursor.fetchone()

            if row == None:
                break
            col = {}
            for k,v in row.items():
                col[k] = v
            collections.append(col)
        return collections

    def get_collection_schema(self, colid):
        """ Fetches the column names for both the stream and data tables
            for the given collection.

            Returns a tuple where the first item is the list of column
            names from the streams table and the second item is the list
            of column names from the data table.
        """
        if self.basiccursor == None:
            return [], []

        self.basiccursor.execute(
                "SELECT streamtable, datatable from collections WHERE id=%s",
                (colid,))

        tables = self.basiccursor.fetchone()

        # Parameterised queries don't work on the FROM clause -- our table
        # names *shouldn't* be an SQL injection risk, right?? XXX
        self.basiccursor.execute(
                "SELECT * from %s LIMIT 1" % (tables['streamtable']))

        streamcolnames = [cn[0] for cn in self.basiccursor.description]

        self.basiccursor.execute(
                "SELECT * from %s LIMIT 1" % (tables['datatable']))

        datacolnames = [cn[0] for cn in self.basiccursor.description]
        return streamcolnames, datacolnames

    def select_streams_by_module(self, mod):
        """ Fetches all streams that belong to collections that have a common
            parent module, e.g. amp, lpi or rrd.

            For example, passing "amp" into this function would give you
            all amp-icmp and amp-traceroute streams.

            Note that if you want the streams for a single collection, you
            should use select_streams_by_collection.

            Returns a list of streams, where each stream is a dictionary
            describing all of the stream parameters.
        """

        # Find all streams for a given parent collection, e.g. amp, lpi
        #
        # For each stream:
        #   Form a dictionary containing all the relevant information about
        #   that stream (this will require info from both the combined streams
        #   table and the module/subtype specific table
        # Put all the dictionaries into a list

        # Find the collections matching this module
        if self.basiccursor == None:
            return []
        self.basiccursor.execute(
                "SELECT * from collections where module=%s", (mod,))
        streamtables = {}

        cols = self.basiccursor.fetchall()

        for c in cols:
            streamtables[c["id"]] = (c["streamtable"], c["modsubtype"])

        streams = []
        for cid, (tname, sub) in streamtables.items():
            sql = """ SELECT * FROM streams, %s WHERE streams.collection=%s
                      AND streams.id = %s.stream_id """ % (tname, "%s", tname)
            self.basiccursor.execute(sql, (cid,))

            while True:
                row = self.basiccursor.fetchone()
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
        """ Fetches all streams that belong to a given collection.

            For example, passing "amp-icmp" into this function would give you
            all amp-icmp streams.

            Only streams with an id number greater than 'minid' will be
            returned. This is useful for getting all of the new streams that
            have been created since the last time you called this function,
            as stream ids are assigned sequentially.

            To get all streams for a collection, set minid to 0.

            Returns a list of streams, where each stream is a dictionary
            describing all of the stream parameters.
        """
        if self.basiccursor == None:
            return []
        self.basiccursor.execute(
                "SELECT * from collections where id=%s", (coll,))
        assert(self.basiccursor.rowcount == 1)

        coldata = self.basiccursor.fetchone()

        tname = coldata['streamtable']
        sql = """SELECT * FROM streams, %s WHERE streams.id = %s.stream_id
                 AND streams.id > %s""" % (tname, tname, "%s")
        self.basiccursor.execute(sql, (minid,))

        selected = []
        while True:
            row = self.basiccursor.fetchone()
            if row == None:
                break
            stream_dict = {}
            for k,v in row.items():
                if k == "id":
                    continue
                stream_dict[k] = v
            selected.append(stream_dict)
        return selected


    def select_aggregated_data(self, col, labels, aggcols,
            start_time = None, stop_time = None, groupcols = None,
            binsize = 0, aggregator="avg"):

        """ Queries the database for time series data, splits the time
            series into bins and applies the given aggregation function(s)
            to each time bin.

            This function is mainly used for fetching data for display
            on a graph, as you probably don't want to plot every individual
            data point when the graph scale is measured in days. Instead,
            this function can be used to return the average value for each
            hour, for example.

            Parameters:
                col -- the id of the collection to query
                stream_ids -- a list of stream ids to get data for
                aggcols -- a list of data columns to aggregate
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
                aggregator -- the aggregator function(s) to be applied to the
                              aggcols. If this is a string, the described
                              function will be applied to all columns. If
                              this is a list with one string in it, the same
                              thing happens. If this is a list with multiple
                              strings, then each string describes the function
                              to apply to the column in the matching position
                              in the aggcols list.

                              In the last case, there MUST be an entry in the
                              aggregator list for every column provided in
                              aggcols.

            This function is a generator function and will yield a tuple each
            time it is iterated over. The tuple contains a row from the result
            set, the name of the column describing the start of each bin and
            the binsize.

            Example usage -- get the hourly average of 'value' for streams
            1, 2 and 3 from collection 1 for a given week:

                for row, tscol, binsize in db.select_aggregated_data(1,
                        [1,2,3], ['value'], 1380758400, 1381363200, None,
                        60 * 60, "avg"):
                    process_row(row)
        """

        if type(binsize) is not int:
            return

        # Make sure we have a usable cursor
        self._reset_cursor()
        if self.datacursor == None:
            return

        # Set default time boundaries
        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)
        
        assert(type(labels) is dict)

        # TODO cast to a set to make unique entries?
        all_streams = reduce(lambda x, y: x+y, labels.values())

        # Find the data table and make sure we are only querying for
        # valid columns
        table, columns = self._get_data_table(col)

        # XXX get rid of stream_id, ideally it wouldnt even get to here
        if "stream_id" in groupcols:
            del groupcols[groupcols.index("stream_id")]

        # Make sure we only query for columns that exist in the data table
        # XXX Could we do this in one sanitise call?
        groupcols = self._sanitise_columns(columns, groupcols)
        aggcols = self._sanitise_columns(columns, aggcols)

        # Convert our column and aggregator lists into useful bits of SQL
        labeled_aggcols, aggfuncs = self._apply_aggregation(aggcols, aggregator)
        labeled_groupcols = list(groupcols)

        # Add a column for the maximum timestamp in the bin
        labeled_aggcols.append("max(timestamp) AS timestamp")

        if binsize == 0 or binsize == (stop_time - start_time):
            # Add minimum timestamp to help with determining frequency
            labeled_aggcols.append("min(timestamp) AS min_timestamp")
            tscol = "min_timestamp"
        else:
            # We're going to (probably) have multiple bins, so we also
            # want to group measurements into the appropriate bin
            labeled_groupcols.append(\
                    "(timestamp - (timestamp %% %s)) AS binstart")
            groupcols.append("binstart")
            tscol = "binstart"

        # Constructing the innermost SELECT query, which lists the label for
        # each measurement

        # Use a CASE statement to combine all the streams within a label
        case, caseparams = self._generate_label_case(labels)
        sql_agg = """ SELECT %s AS label, timestamp """ % (case)

        uniquecols = list(set(aggcols))
        for col in uniquecols:
            sql_agg += ", " + col

        sql_agg += " FROM %s " % (table)
        sql_agg += self._generate_where(all_streams)

        # Constructing the outer SELECT query, which will aggregate across
        # each label to find the aggregate values
        sql = "SELECT label"
        for col in labeled_groupcols:
            sql += "," + col
        for col in labeled_aggcols:
            sql += "," + col

        sql += " FROM ( %s ) AS aggregates" % (sql_agg)
        sql += " GROUP BY label"
        for col in groupcols:
            sql += ", " + col

        sql += " ORDER BY label, timestamp"

        # Execute our query!
        # XXX Getting these parameters in the right order is a pain!
        params = tuple([binsize] + caseparams + [start_time] + [stop_time] + all_streams)

        self.datacursor.execute(sql, params)

        while True:
            # fetchmany seems to be recommended over repeated calls to fetchone
            fetched = self.datacursor.fetchmany(100)
            if fetched == []:
                break

            for row in fetched:
                yield (row, tscol, binsize)


    def select_data(self, col, labels, selectcols, start_time=None,
            stop_time=None):

        """ Queries the database for time series data.

            This function will return all measurements for the given
            streams that fall between the start and end time.

            Parameters:
                col -- the id of the collection to query
                stream_ids -- a list of stream ids to get data for
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

            This function is a generator function and will yield a tuple each
            time it is iterated over. The tuple contains a row from the result
            set, the name of the timestamp column and the binsize (which is
            always zero in this case).

            Example usage -- get the contents of the 'value' column for streams
            1, 2 and 3 from collection 1 for a given week:

                for row, tscol, binsize in db.select_data(1, [1,2,3],
                        ['value'], 1380758400, 1381363200):
                    process_row(row)
        """

        # Set default time boundaries
        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)

        # Find the data table for the requested collection
        table, columns = self._get_data_table(col)

        # Make sure we only query for columns that are in the data table
        selectcols = self._sanitise_columns(columns, selectcols)
        
        # XXX for now, lets try to munge graph types that give a list of
        # stream ids into the label dictionary format that we want
        assert(type(labels) is dict)

        # TODO cast to a set to make unique entries?
        all_streams = reduce(lambda x, y: x+y, labels.values())

        case, caseparams = self._generate_label_case(labels)
        # These columns are important so include them regardless
        selectcols.append(case + " AS label")
        if 'timestamp' not in selectcols:
            selectcols.append('timestamp')

        params = tuple(caseparams + [start_time] + [stop_time] + all_streams)

        for resultrow in self._generic_select(table, all_streams, params,
                selectcols, None, 'timestamp', 0):
            yield resultrow


    def _generic_select(self, table, streams, params, selcols, groupcols,
            tscol, binsize):
        """ Generator function that constructs and executes the SQL query
            required for both select_data and select_aggregated_data.
        """

        # Make sure our datacursor is usable
        self._reset_cursor()
        if self.datacursor == None:
            return

        selclause = "SELECT "
        for i in range(0, len(selcols)):
            selclause += selcols[i]
            # rename stream_id to label so it matches the results if we are
            # querying and aggregating across many stream ids
            if selcols[i] == "stream_id":
                selclause += " AS label"

            if i != len(selcols) - 1:
                selclause += ", "

        fromclause = " FROM %s " % table

        whereclause = self._generate_where(streams)

        # Form the "GROUP BY" section of our query (if asking
        # for aggregated data)
        if groupcols == None or len(groupcols) == 0:
            groupclause = ""
        else:
            groupclause = " GROUP BY "
            for i in range(0, len(groupcols)):
                groupclause += groupcols[i]
                if i != len(groupcols) - 1:
                    groupclause += ", "

        # Order the results both chronologically and by stream id
        orderclause = " ORDER BY label, %s " % (tscol)

        sql = selclause + fromclause + whereclause + groupclause \
                + orderclause
        self.datacursor.execute(sql, params)

        while True:
            # fetchmany seems to be recommended over repeated calls to
            # fetchone
            fetched = self.datacursor.fetchmany(100)
            if fetched == []:
                break

            for row in fetched:
                yield (row, tscol, binsize)


    def _generate_label_case(self, labels):
        """ Forms a CASE statement for an SQL query that converts all stream
            ids into the label to which they belong
        """
        case = "CASE"
        caseparams = []
        for label,stream_ids in labels.iteritems():
            #case += " WHEN stream_id in (%s) THEN '%s'" % (
                #",".join(str(x) for x in stream_ids), label)
            case += " WHEN stream_id in (%s)" % (
                ",".join(["%s"] * len(stream_ids)))
            case += " THEN %s"

            caseparams += stream_ids
            caseparams.append(label)
        case += " END"
        return case, caseparams


    def _generate_where(self, streams):
        """ Forms a WHERE clause for an SQL query that encompasses all
            streams in the provided list
        """
        tsclause = " WHERE timestamp >= %s AND timestamp <= %s "

        assert(len(streams) > 0)
        streamclause = "AND stream_id IN ("
        for i in range(0, len(streams)):
            streamclause += "%s"
            if i != len(streams) - 1:
                streamclause += ", "
        streamclause += ")"

        return tsclause + streamclause

    def _get_data_table(self, col):
        """ Finds the data table for a given collection

            Returns a tuple containing three items:
             1. the name of the data table
             2. a list of columns present in the table

        """
        if self.basiccursor == None:
            return
        self.basiccursor.execute(
                "SELECT * from collections where id=%s", (col,))
        assert(self.basiccursor.rowcount == 1)

        coldata = self.basiccursor.fetchone()
        tname = coldata['datatable']
        module = coldata['module']
        subtype = coldata['modsubtype']

        table = tname

        # This is the quickest way to get the column names -- don't
        # try querying the data table itself because that could be slow
        # if the table is, for example, a complicated view.
        self.basiccursor.execute(
                "SELECT * from information_schema.columns WHERE table_name=%s",
                (tname,))

        columns = []
        while True:
            row = self.basiccursor.fetchone()
            if row == None:
                break

            columns.append(row['column_name'])
        return table, columns

    def _sanitise_columns(self, columns, selcols):
        """ Removes columns from the provided list if they are not present
            in the list of columns available for a table.

            Parameters:
                columns -- the column list to be sanitised
                selcols -- the list of available columns for the table

            Returns:
                A list of columns with any bogus entries removed
        """

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

        """ Given a list of columns and a list of aggregation functions
            to apply to those columns, produces a list of SQL AS clauses
            describing how to apply the appropriate aggregation to each
            column.

            Returns a tuple with 2 items:
             1. The list of aggregations as SQL AS clauses, e.g.
                "avg(foo) AS foo_avg"
             2. The list of aggregation functions being applied to
                each column.

            Parameters:
                selectors -- the list of columns to apply aggregation to
                aggregator -- the aggregation function(s) to apply. This may
                              be a string if one aggregation function is to
                              be applied to all columns. Otherwise, it should
                              be a list of strings.

            See in-code comment for a more detailed description of how
            'aggregator' may be specified.
        """

        # 'aggregator' can take many forms:
        #    It can be a string (in which case, the aggregation function will
        #    be applied to all of the aggcols).
        #    It can be a list with one string in it (same result as above).
        #    It can be a list with multiple strings. In this case, there must
        #    be one entry in the aggregator list for every entry in the
        #    aggcols list -- the aggregator in position 0 will be applied to
        #    the column in position 0, etc.

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

    def select_percentile_data(self, col, labels, ntilecols, othercols,
            start_time = None, stop_time = None, binsize=0,
            ntile_aggregator = "avg", other_aggregator = "avg"):
        """ Queries the database for time series data, splits the time
            series into bins and calculates the percentiles for the data
            within each bin.

            This function is mainly used for converting conventional time
            series data into a suitable format for display on a
            smokeping-style graph. The percentiles are used to draw 'smoke'
            in addition to the median to show variation within the bin.

            "other" is used to describe columns that should be fetched in
            addition to the percentile columns. The data values for these
            columns will be aggregated into bins as though they had been
            fetched using select_aggregated_data, i.e. there will be just
            one value per bin rather than an array, as with percentile
            columns.

            Parameters:
                col -- the id of the collection to query
                stream_ids -- a list of stream ids to get data for
                ntilecols -- a list of data columns to fetch as percentiles
                othercols -- a list of data columns to fetch as aggregated
                             data
                start_time -- a timestamp describing the start of the
                              time period that data is required for. If
                              None, this is set to 1 day before the stop
                              time
                stop_time -- a timestamp describing the end of the time
                             period that data is required for. If None,
                             this is set to the current time.
                binsize -- the size of each time bin. If 0 (the default),
                           the entire data series will aggregated into a
                           single summary value.
                ntile_aggregator -- the aggregator function(s) to be applied
                                    to the percentile columns
                other_aggregator -- the aggregator function(s) to be applied
                                    to the aggregate columns

            At present, we only allow support one column being fetched as
            percentile data.

            There may be multiple 'other' columns, though. Specifying
            aggregation functions for them (i.e. the other_aggregator
            parameter) works exactly the same as it does in
            select_aggregated_data.

            This function is a generator function and will yield a tuple each
            time it is iterated over. The tuple contains a row from the result
            set, the name of the column describing the start of each bin and
            the binsize.

            Example usage -- get the hourly percentiles of 'value' for streams
            1, 2 and 3 from collection 1 for a given week:

                for row, tscol, binsize in db.select_percentile_data(1,
                        [1,2,3], ['value'], [], 1380758400, 1381363200,
                        60 * 60, "avg", "avg"):
                    process_row(row)
        """

        # TODO this SQL is out of date slightly
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
            return

        # Make sure we have a usable cursor
        self._reset_cursor()
        if self.datacursor == None:
            return

        # Set default time boundaries
        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)

        # Let's just limit ourselves to 1 set of percentiles for now, huh
        if len(ntilecols) != 1:
            ntilecols = ntilecols[0:1]

        assert(type(labels) is dict)

        # TODO cast to a set to make unique entries?
        all_streams = reduce(lambda x, y: x+y, labels.values())

        # Find the data table and make sure we are only querying for
        # valid columns
        table, columns = self._get_data_table(col)
        ntilecols = self._sanitise_columns(columns, ntilecols)
        othercols = self._sanitise_columns(columns, othercols)

        # Convert our column and aggregator lists into useful bits of SQL
        labeledntilecols, ntileaggfuncs = \
                self._apply_aggregation(ntilecols, ntile_aggregator)
        labeledothercols, otheraggfuncs = \
                self._apply_aggregation(othercols, other_aggregator)

        # Constructing the innermost SELECT query, which lists the ntile for
        # each measurement

        # Use a CASE statement to combine all the streams within a label
        case, caseparams = self._generate_label_case(labels)
        sql_ntile = """ SELECT %s as label, """ % (case)
        sql_ntile += "timestamp, timestamp - (timestamp %% %s) AS binstart, "

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

        where_clause = self._generate_where(all_streams)

        sql_ntile += where_clause
        sql_ntile += " ORDER BY binstart "

        # Constructing the middle SELECT query, which will aggregate across
        # each ntile to find the average value within each ntile
        sql_agg = "SELECT label, max(timestamp) AS recent, "

        for l in labeledntilecols:
            sql_agg += l + ", "
        for l in labeledothercols:
            sql_agg += l + ", "

        sql_agg += "binstart FROM ( %s ) AS ntiles" % (sql_ntile)
        sql_agg += " GROUP BY label, binstart, ntile ORDER BY binstart, "

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
        qcols = ["timestamp", "binstart", "label"]
        sql = "SELECT label, max(recent) AS timestamp, "

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

        # make sure the outer query is sorted by stream_id then binsize, so
        # that we get all the time sorted data for each stream_id in turn
        sql += "binstart FROM (%s) AS agg GROUP BY binstart, label ORDER BY label, binstart" % (sql_agg)

        # Execute our query!
        params = tuple([binsize] + caseparams + [start_time] + [stop_time] + all_streams)
        self.datacursor.execute(sql, params)

        while True:
            # fetchmany is generally recommended over fetchone
            fetched = self.datacursor.fetchmany(100)

            if fetched == []:
                break

            for row in fetched:
                yield (row, "binstart", binsize)



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
