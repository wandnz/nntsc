import psycopg2
import psycopg2.extras
from libnntscclient.logger import *
from libnntsc.parsers import amp_traceroute
from libnntsc.querybuilder import QueryBuilder
import time

# Class used for querying the NNTSC database.
# Uses psycopg2 rather than SQLAlchemy for the following reasons:
#  * simple to understand and use
#  * supports parameterised queries
#  * named cursors allow us to easily deal with large result sets
#  * documentation that makes sense

DB_QUERY_OK = 0
DB_QUERY_CANCEL = -1
DB_QUERY_RETRY = -2

class NNTSCDatabaseTimeout(Exception):
    def __init__(self, secs):
        self.timeout = secs
    def __str__(self):
        return "Database Query timed out after %d secs" % (self.timeout)

class NNTSCDatabaseDisconnect(Exception):
    def __init__(self):
        pass
    def __str__(self):
        return "Connection to NNTSC Database was lost"

class DBSelector:
    def __init__(self, uniqueid, dbname, dbuser, dbpass=None, dbhost=None,
            timeout=0):

        self.qb = QueryBuilder()
        self.dbselid = uniqueid
        self.timeout = timeout
        connstr = "dbname=%s user=%s" % (dbname, dbuser)
        if dbpass != "" and dbpass != None:
            connstr += " password=%s" % (dbpass)
        if dbhost != "" and dbhost != None:
            connstr += " host=%s" % (dbhost)

        # Force all queries we make to timeout after 50 seconds so they don't
        # hang around causing trouble if the user changes page and abandons
        # the request or similar. Varnish will return an error if the page
        # doesn't load after 60s, so we don't need to go for longer than that.
        if timeout != 0:
            connstr += " options='-c statement_timeout=%d'" % (timeout * 1000.0)
        
        #log("Setting DB timeout to %d" % (timeout * 1000.0))

        self.conn = None
        reconnect = 30
        while self.conn == None:

            try:
                self.conn = psycopg2.connect(connstr)
            except psycopg2.DatabaseError as e:
                log("DBSelector: Error connecting to database: %s" % e)
                log("Retrying in %d seconds" % reconnect);
                self.conn = None
                self.basiccursor = None
                self.datacursor = None
                
                time.sleep(reconnect)
                reconnect *= 2
                if (reconnect > 600):
                    reconnect = 600;


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

        #log("DBSelector: Successfully created DBSelector %s" % self.dbselid)

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
            #log("DBSelector: Closed database connection for %s" % self.dbselid)

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

        try:
            self.basiccursor.execute("SELECT * from collections")
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()
        
        while True:
            row = self.basiccursor.fetchone()

            if row == None:
                break
            col = {}
            for k, v in row.items():
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

        try:
            self.basiccursor.execute(
                "SELECT streamtable, datatable from collections WHERE id=%s",
                (colid,))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

        tables = self.basiccursor.fetchone()

        # Parameterised queries don't work on the FROM clause -- our table
        # names *shouldn't* be an SQL injection risk, right?? XXX
        try:
            self.basiccursor.execute(
                    "SELECT * from %s LIMIT 1" % (tables['streamtable']))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

        streamcolnames = [cn[0] for cn in self.basiccursor.description]

        try:
            self.basiccursor.execute(
                    "SELECT * from %s LIMIT 1" % (tables['datatable']))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

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
        try:
            self.basiccursor.execute(
                    "SELECT * from collections where module=%s", (mod,))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

        streamtables = {}

        cols = self.basiccursor.fetchall()

        for c in cols:
            streamtables[c["id"]] = (c["streamtable"], c["modsubtype"])

        streams = []
        for cid, (tname, sub) in streamtables.items():
            sql = """ SELECT * FROM streams, %s WHERE streams.collection=%s
                      AND streams.id = %s.stream_id """ % (tname, "%s", tname)
            try:
                self.basiccursor.execute(sql, (cid,))
            except psycopg2.extensions.QueryCanceledError:
                self.conn.rollback()
                raise NNTSCDatabaseTimeout(self.timeout)
            except psycopg2.OperationalError:
                raise NNTSCDatabaseDisconnect()


            while True:
                row = self.basiccursor.fetchone()
                if row == None:
                    break
                row_dict = {"modsubtype":sub}
                for k, v in row.items():
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
        try:
            self.basiccursor.execute(
                    "SELECT * from collections where id=%s", (coll,))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()
       
        assert(self.basiccursor.rowcount == 1)

        coldata = self.basiccursor.fetchone()

        tname = coldata['streamtable']
        sql = """SELECT * FROM streams, %s WHERE streams.id = %s.stream_id
                 AND streams.id > %s""" % (tname, tname, "%s")
        
        try:
            self.basiccursor.execute(sql, (minid,))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

        selected = []
        while True:
            row = self.basiccursor.fetchone()
            if row == None:
                break
            stream_dict = {}
            for k, v in row.items():
                if k == "id":
                    continue
                stream_dict[k] = v
            selected.append(stream_dict)
        return selected

    def select_active_streams_by_collection(self, coll, lastactivity):
        """ Fetches all recently active streams belonging to a given collection.

            For example, passing "amp-icmp" into this function would give you
            all amp-icmp streams.

            Only streams with data after the lastactivity timestamp will be
            returned. To get all streams for a collection, set lastactivity
            to 0.

            Returns a list of stream ids
        """
        if self.basiccursor == None:
            return []

        sql = "SELECT id FROM streams WHERE collection=%s AND lasttimestamp>%s"
        try:
            self.basiccursor.execute(sql, (coll, lastactivity))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

        active = []
        while True:
            row = self.basiccursor.fetchone()
            if row == None:
                break
            for stream_id in row.values():
                active.append(stream_id)
        return active


    def select_aggregated_data(self, col, labels, aggcols,
            start_time = None, stop_time = None, groupcols = None,
            binsize = 0):

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
                labels -- a dictionary of labels and their corresponding
                          stream ids
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

            This function is a generator function and will yield a tuple each
            time it is iterated over. The tuple contains a row from the result
            set, the name of the column describing the start of each bin and
            the binsize.

            Example usage -- get the hourly average of 'value' for streams
            1, 2 and 3 from collection 1 for a given week:

                for row, tscol, binsize in db.select_aggregated_data(1,
                        {'stream1':[1], 'stream2':[2], 'stream3':[3]}, 
                        {'value':'avg'}, 1380758400, 1381363200, None,
                        60 * 60):
                    process_row(row)
        """

        if type(binsize) is not int:
            return


        # Set default time boundaries
        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)

        assert(type(labels) is dict)

        # Find the data table and make sure we are only querying for
        # valid columns
        try:
            table, columns = self._get_data_table(col)
        except NNTSCDatabaseTimeout as e:
            yield (None, None, None, DB_QUERY_CANCEL)
        except NNTSCDatabaseDisconnect as e:
            yield (None, None, None, DB_QUERY_RETRY)
            

        # XXX get rid of stream_id, ideally it wouldnt even get to here
        if "stream_id" in groupcols:
            del groupcols[groupcols.index("stream_id")]

        # Make sure we only query for columns that exist in the data table
        if table == "data_amp_traceroute":
            groupcols = amp_traceroute.sanitise_columns(groupcols)
        else:
            groupcols = self._sanitise_columns(columns, groupcols)

        aggcols = self._filter_aggregation_columns(table, aggcols)

        self.qb.reset()

        # Convert our column and aggregator lists into useful bits of SQL
        labeled_aggcols = self._apply_aggregation(aggcols)
        labeled_groupcols = list(groupcols)

        # Add a column for the maximum timestamp in the bin
        labeled_aggcols.append("max(timestamp) AS timestamp")

        if binsize == 0 or binsize == (stop_time - start_time):
            # Add minimum timestamp to help with determining frequency
            labeled_aggcols.append("min(timestamp) AS min_timestamp")
            tscol = "min_timestamp"
            binparam = []
        else:
            # We're going to (probably) have multiple bins, so we also
            # want to group measurements into the appropriate bin
            labeled_groupcols.append(\
                    "(timestamp - (timestamp %% %s)) AS binstart")
            groupcols.append("binstart")
            tscol = "binstart"
            binparam = [binsize]

        # Constructing the innermost SELECT query, which lists the label for
        # each measurement
        innselclause = " SELECT label, timestamp "

        uniquecols = list(set([k[0] for k in aggcols]))
        for col in uniquecols:
            innselclause += ", " + col

        self.qb.add_clause("innersel", innselclause, [])

        self._generate_where(start_time, stop_time)
        
        # Constructing the outer SELECT query, which will aggregate across
        # each label to find the aggregate values
        outselclause = "SELECT label"
        for col in labeled_groupcols:
            outselclause += "," + col
        for col in labeled_aggcols:
            outselclause += "," + col
        outselclause += " FROM ( "

        self.qb.add_clause("outsel", outselclause, binparam)

        outselend = " ) AS aggregates"
        self.qb.add_clause("outselend", outselend, [])

        outgroup = " GROUP BY label"
        for col in groupcols:
            outgroup += ", " + col

        outgroup += " ORDER BY label, timestamp"

        self.qb.add_clause("outgroup", outgroup, [])

        for label, streams in labels.iteritems():

            # Make sure we have a usable cursor
            self._reset_cursor()
            if self.datacursor == None:
                return
            
            self._generate_from(table, label, streams, start_time, stop_time)
            order = ["outsel", "innersel", "activestreams", "activejoin", 
                    "union", "joincondition", "wheretime", "outselend",
                    "outgroup"]
            query, params = self.qb.create_query(order)
           
            try:
                self.datacursor.execute(query, params)
            except psycopg2.extensions.QueryCanceledError:
                self.conn.rollback()
                self.datacursor = None
                yield (None, None, None, DB_QUERY_CANCEL)
            except psycopg2.OperationalError:
                self.datacursor = None
                yield (None, None, None, DB_QUERY_RETRY) 
            

            fetched = self._query_data_generator()
            for row, cancelled in fetched:
                yield (row, tscol, binsize, cancelled)


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
        try:
            table, columns = self._get_data_table(col)
        except NNTSCDatabaseTimeout as e:
            yield (None, None, None, DB_QUERY_CANCEL)
        except NNTSCDatabaseDisconnect as e:
            yield (None, None, None, DB_QUERY_RETRY)


        # Make sure we only query for columns that are in the data table
        if table == "data_amp_traceroute":
            selectcols = amp_traceroute.sanitise_columns(selectcols)
        else:
            selectcols = self._sanitise_columns(columns, selectcols)

        # XXX for now, lets try to munge graph types that give a list of
        # stream ids into the label dictionary format that we want
        assert(type(labels) is dict)

        # These columns are important so include them regardless
        if 'timestamp' not in selectcols:
            selectcols.append('timestamp')
        if 'stream_id' not in selectcols:
            selectcols.append('stream_id')
        if 'label' not in selectcols:
            selectcols.append('label')

        self.qb.reset()
        order = []

        selclause = "SELECT "
        for i in range(0, len(selectcols)):
            selclause += selectcols[i]

            if i != len(selectcols) - 1:
                selclause += ", "

        self.qb.add_clause("select", selclause, [])

        self._generate_where(start_time, stop_time)

        # Order the results both chronologically and by stream id
        orderclause = " ORDER BY label, timestamp " 
        self.qb.add_clause("order", orderclause, [])

        for label, streams in labels.iteritems():
            # Make sure our datacursor is usable
            self._reset_cursor()
            if self.datacursor == None:
                return
            
            self._generate_from(table, label, streams, start_time, stop_time)
            order = ["select", "activestreams", "activejoin", "union",
                    "joincondition", "wheretime", "order"]
            sql, params = self.qb.create_query(order)

            try:
                self.datacursor.execute(sql, params)
            except psycopg2.extensions.QueryCanceledError:
                self.conn.rollback()
                self.datacursor = None
                yield (None, None, None, DB_QUERY_CANCEL)
            except psycopg2.OperationalError:
                self.datacursor = None
                yield (None, None, None, DB_QUERY_RETRY) 
            
            fetched = self._query_data_generator()
            for row, cancelled in fetched:
                yield (row, "timestamp", 0, cancelled)


    def _generate_label_case(self, label, stream_ids):
        """ Forms a CASE statement for an SQL query that converts all stream
            ids into the label to which they belong
        """
        case = "CASE"
        caseparams = []

        if len(stream_ids) > 0:
            case += " WHEN id in (%s)" % (
                ",".join(["%s"] * len(stream_ids)))
            case += " THEN %s"

            caseparams += stream_ids
            caseparams.append(label)
        case += " END"
        self.qb.add_clause("caselabel", case, caseparams)


    def _generate_union(self, basetable, streams):

        unionparams = []
        sql = "("

        for i in range(0, len(streams)):
            unionparams.append(streams[i])
            sql += "SELECT * FROM %s_" % (basetable)
            sql += "%s"     # stream id will go here

            if i != len(streams) - 1:
                sql += " UNION ALL "

        sql += ") AS dataunion"
        self.qb.add_clause("union", sql, unionparams)


    # It looks like restricting the number of stream ids that are checked for
    # in the data table helps significantly with performance, so if we can
    # exclude all the streams that aren't in scope, we have a much smaller
    # search space.
    # TODO this needs to be tidied up, returning lists of arguments back
    # through multiple levels of function calls doesn't feel very nice, and
    # anyway, the whole way sql query parameters are done needs to be reworked.
    def _generate_from(self, table, label, streams, start, end):
        """ Forms a FROM clause for an SQL query that encompasses all
            streams in the provided list that fit within a given time period.
        """
        uniquestreams = {}

        for s in streams:
            if s not in uniquestreams:
                uniquestreams[s] = 0

        
        
        # build the case statement that will label our stream ids
        self._generate_label_case(label, streams)

        # get all stream ids that are active in the period
        caseparams = []
        active = "FROM ((SELECT id, CASE "
        
        if len(streams) > 0:
            active += " WHEN id in (%s)" % (
                ",".join(["%s"] * len(streams)))
            active += " THEN %s"

            caseparams += streams
            caseparams.append(label)
        active += " END as label FROM streams "
        
        active += "WHERE lasttimestamp >= %s AND firsttimestamp <= %s"
        caseparams += [start, end]

        active += " AND id in ("
        count = len(uniquestreams)
        for i in range(0, count):
            active += "%s"
            if i != count - 1:
                active += ", "
        active += ")) AS activestreams"
        caseparams += uniquestreams.keys()

        self.qb.add_clause("activestreams", active, caseparams)
        self.qb.add_clause("activejoin", "INNER JOIN", [])

        joincond = "ON dataunion.stream_id = activestreams.id)"
        self.qb.add_clause("joincondition", joincond, [])


        if table == "data_amp_traceroute":
            amp_traceroute.generate_union(self.qb, table, uniquestreams.keys())
        else:
            self._generate_union(table, uniquestreams.keys())


    def _generate_where(self, start, end):
        """ Forms a WHERE clause for an SQL query based on a time period """
       
        sql = " WHERE timestamp >= %s AND timestamp <= %s "
        self.qb.add_clause("wheretime", sql, [start, end])
        return "wheretime" 

    def _get_data_table(self, col):
        """ Finds the data table for a given collection

            Returns a tuple containing three items:
             1. the name of the data table
             2. a list of columns present in the table

        """
        if self.basiccursor == None:
            return None, []
        try:
            self.basiccursor.execute(
                    "SELECT * from collections where id=%s", (col,))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()

        assert(self.basiccursor.rowcount == 1)

        coldata = self.basiccursor.fetchone()
        tname = coldata['datatable']
        module = coldata['module']
        subtype = coldata['modsubtype']

        table = tname

        # This is the quickest way to get the column names -- don't
        # try querying the data table itself because that could be slow
        # if the table is, for example, a complicated view.
        try:
            self.basiccursor.execute(
                "SELECT * from information_schema.columns WHERE table_name=%s",
                (tname,))
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise NNTSCDatabaseTimeout(self.timeout)
        except psycopg2.OperationalError:
            raise NNTSCDatabaseDisconnect()


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

        for i in range(0, len(selcols)):
            cn = selcols[i]
            
            if cn in columns:
                sanitised.append(cn)
        return sanitised

    def _apply_aggregation(self, aggregators):

        rename = False
        aggcols = []

        columns = [k[0] for k in aggregators]

        # If we have duplicates in the select column list, we'll need
        # to rename them to differentiate them based on the aggregation
        # function applied to them
        if len(set(columns)) < len(columns):
            rename = True

        for colname, func in aggregators:
            labelstr = colname
            if rename:
                labelstr += "_" + func

            # this isn't the greatest, but we have to treat this one different
            if func == "most_array":
                colclause = "string_to_array(" + \
                    "most(array_to_string(%s,',')),',') AS %s" % (
                        colname, labelstr)
            else:
                colclause = "%s(%s) AS %s" % (
                        func, colname, labelstr)
            aggcols.append(colclause)

        return aggcols
    
    def _filter_aggregation_columns(self, table, aggcols):
        keys = [k[0] for k in aggcols]
        
        if table == "data_amp_traceroute":
            keys = amp_traceroute.sanitise_columns(keys)

        filtered = []
        for k,v in aggcols:
            if k not in keys:
                continue
            filtered.append((k,v))

        return filtered
      

    def select_percentile_data(self, col, labels, ntilecols, othercols,
            start_time = None, stop_time = None, binsize=0):
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
        #                   PARTITION BY timestamp-timestamp%BINSIZE, label
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

        # Set default time boundaries
        if stop_time == None:
            stop_time = int(time.time())
        if start_time == None:
            start_time = stop_time - (24 * 60 * 60)

        # Let's just limit ourselves to 1 set of percentiles for now, huh
        if len(ntilecols) != 1:
            ntilecols = ntilecols[0:1]

        assert(type(labels) is dict)

        # Find the data table and make sure we are only querying for
        # valid columns
        try:
            table, columns = self._get_data_table(col)
        except NNTSCDatabaseTimeout as e:
            yield (None, None, None, DB_QUERY_CANCEL)
        except NNTSCDatabaseDisconnect as e:
            yield (None, None, None, DB_QUERY_RETRY)
        
        ntilecols = self._filter_aggregation_columns(table, ntilecols)
        othercols = self._filter_aggregation_columns(table, othercols)
        

        # Convert our column and aggregator lists into useful bits of SQL
        labeledntilecols = self._apply_aggregation(ntilecols)
        labeledothercols = self._apply_aggregation(othercols)

        self.qb.reset()

        # Constructing the innermost SELECT query, which lists the ntile for
        # each measurement

        innersel =  "SELECT label, timestamp, "
        innersel += "timestamp - (timestamp %% %s) AS binstart, "

        selected = []
        for col, agg in ntilecols:
            if col in selected:
                continue
            innersel += col
            innersel += ", "
            selected.append(col)
        for col, agg in othercols:
            if col in selected:
                continue
            innersel += col
            innersel += ", "
            selected.append(col)

        # XXX rename ntile to something unique if we support multiple
        # percentile columns
        innersel += "ntile(20) OVER ( PARTITION BY "
        innersel += "timestamp - (timestamp %% %s), label"
        innersel += " ORDER BY %s )" % (ntilecols[0][0])

        self.qb.add_clause("ntilesel", innersel, [binsize, binsize])

        self._generate_where(start_time, stop_time)
        
        self.qb.add_clause("ntileorder", " ORDER BY binstart ", [])

        # Constructing the middle SELECT query, which will aggregate across
        # each ntile to find the average value within each ntile
        sql_agg = "SELECT label, max(timestamp) AS recent, "

        for l in labeledntilecols:
            sql_agg += l + ", "
        for l in labeledothercols:
            sql_agg += l + ", "

        sql_agg += "binstart FROM ("
        
        self.qb.add_clause("aggsel", sql_agg, [])
        
        agggroup = " ) AS ntiles GROUP BY label, binstart, ntile"
        agggroup += " ORDER BY binstart, "

        # Extracting the labels for the ntile columns for the ORDER BY clause
        for i in range(0, len(labeledntilecols)):
            labelsplit = labeledntilecols[i].split("AS")
            assert(len(labelsplit) == 2)

            agggroup += labelsplit[1].strip()
            if i != len(labeledntilecols) - 1:
                agggroup += ", "
        
        self.qb.add_clause("agggroup", agggroup, [])

        # Finally, construct the outermost SELECT which will collapse the
        # ntiles into an array so we get one row per bin with all of the
        # ntiles in a single "values" column
        outersel = "SELECT label, max(recent) AS timestamp, "

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
            outersel += "coalesce(string_to_array(string_agg(cast(%s AS TEXT), ',' ORDER BY %s), ','), ARRAY[]::text[]) AS values, " % \
                    (labelsplit[1].strip(), labelsplit[1].strip())

        for i in range(0, len(labeledothercols)):
            labelsplit = labeledothercols[i].split("AS")
            aggregator = labelsplit[0].split("(")[0]

            outersel += "%s(%s) AS %s," % (aggregator,labelsplit[1].strip(), \
                    labelsplit[1].strip())
            #outersel += labeledothercols[i] + ","

        outersel += "binstart FROM ( "

        self.qb.add_clause("outersel", outersel, [])

        # make sure the outer query is sorted by stream_id then binsize, so
        # that we get all the time sorted data for each stream_id in turn
        outergroup = " ) AS agg GROUP BY binstart, label "
        outergroup += "ORDER BY label, binstart"
        self.qb.add_clause("outergroup", outergroup, [])

        for label, streams in labels.iteritems():
            # Make sure we have a usable cursor
            self._reset_cursor()
            if self.datacursor == None:
                return

            self._generate_from(table, label, streams, start_time, stop_time)

            order = ["outersel", "aggsel", "ntilesel", "activestreams",
                    "activejoin", "union", "joincondition", "wheretime",
                    "ntileorder", "agggroup", "outergroup"] 

            query, params = self.qb.create_query(order)

            try:
                self.datacursor.execute(query, params)
            except psycopg2.extensions.QueryCanceledError:
                self.conn.rollback()
                self.datacursor = None
                yield (None, None, None, DB_QUERY_CANCEL)
            except psycopg2.OperationalError:
                self.datacursor = None
                yield (None, None, None, DB_QUERY_RETRY) 

            fetched = self._query_data_generator()
            for row, cancelled in fetched:
                yield (row, "binstart", binsize, cancelled)


    # This generator is called by a generator function one level up, but
    # nesting them all seems to work ok
    def _query_data_generator(self):
        while True:
            try:
                fetched = self.datacursor.fetchmany(100)
            except psycopg2.extensions.QueryCanceledError:
                # The named datacursor is invalidated as soon as the
                # transaction ends/fails, we don't need to close it (and it
                # won't allow us to close it). We do have to rollback though
                # so that the basic cursor will continue to work.
                self.datacursor = None
                self.conn.rollback()
                #info = sql % params
                #log("DBSelector: Query cancelled")
                yield None, DB_QUERY_CANCEL 
                #break
            except psycopg2.OperationalError:
                yield None, DB_QUERY_RETRY

            if fetched == []:
                break

            for row in fetched:
                yield row, DB_QUERY_OK

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
