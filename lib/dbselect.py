import psycopg2
import psycopg2.extras
from libnntscclient.logger import *
from libnntsc.parsers import amp_traceroute
from libnntsc.querybuilder import QueryBuilder
from libnntsc.database import DatabaseCore, NNTSCCursor
from libnntsc.dberrorcodes import *
import time

# Class used for querying the NNTSC database.
# Uses psycopg2 rather than SQLAlchemy for the following reasons:
#  * simple to understand and use
#  * supports parameterised queries
#  * named cursors allow us to easily deal with large result sets
#  * documentation that makes sense

#DB_QUERY_OK = 0
#DB_QUERY_CANCEL = -1
#DB_QUERY_RETRY = -2

class DBSelector(DatabaseCore):
    def __init__(self, uniqueid, dbname, dbuser=None, dbpass=None, dbhost=None,
            timeout=0, cachetime=86400):

        super(DBSelector, self).__init__(dbname, dbuser, dbpass, dbhost, 
                False, False, timeout, cachetime)

        self.qb = QueryBuilder()
        self.dbselid = uniqueid

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
        self.cursorname = "cursor_" + uniqueid

        self.data = NNTSCCursor(self.connstr, False, self.cursorname)

    def connect_db(self, retrywait):
        if self.data.connect(retrywait) == -1:
            return -1
        return super(DBSelector, self).connect_db(retrywait)

    def disconnect(self):
        self.data.destroy()

        super(DBSelector, self).disconnect()

    def _dataquery(self, query, params=None):

        while 1:
            err = self.data.closecursor()
            if err == DB_OPERATIONAL_ERROR:
                continue
            if err != DB_NO_ERROR:
                break

            err = self.data.executequery(query, params)
            if err == DB_OPERATIONAL_ERROR:
                # Retry the query, as we just reconnected
                continue
            if err != DB_NO_ERROR:
                self.data.cursor = None
            break

        return err           

    def release_data(self):
        err = self.data.closecursor()
        return err
 
    def get_collection_schema(self, colid):
        """ Fetches the column names for both the stream and data tables
            for the given collection.

            Returns a tuple where the first item is the list of column
            names from the streams table and the second item is the list
            of column names from the data table.
        """

        err = self._basicquery(
                "SELECT streamtable, datatable from collections WHERE id=%s",
                (colid,))
        if err != DB_NO_ERROR:
            log("Error selecting table names from collections")
            raise DBQueryException(err)

        tables = self.basic.cursor.fetchone()

        # Parameterised queries don't work on the FROM clause -- our table
        # names *shouldn't* be an SQL injection risk, right?? XXX
        err = self._basicquery(
                    "SELECT * from %s LIMIT 1" % (tables['streamtable']))
        if err != DB_NO_ERROR:
            log("Error selecting single row from stream table")
            raise DBQueryException(err)

        streamcolnames = [cn[0] for cn in self.basic.cursor.description]

        err = self._basicquery(
                    "SELECT * from %s LIMIT 1" % (tables['datatable']))
        if err != DB_NO_ERROR:
            log("Error selecting single row from data table")
            raise DBQueryException(err)

        datacolnames = [cn[0] for cn in self.basic.cursor.description]
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Error while tidying up after querying column names")
            raise DBQueryException(err)

        return streamcolnames, datacolnames

    def select_streams_by_collection(self, coll, minid):
        """ Fetches all streams that belong to a given collection id.

            Only streams with an id number greater than 'minid' will be
            returned. This is useful for getting all of the new streams that
            have been created since the last time you called this function,
            as stream ids are assigned sequentially.

            To get all streams for a collection, set minid to 0.

            Returns a list of streams, where each stream is a dictionary
            describing all of the stream parameters.
        """
        err = self._basicquery(
                    "SELECT * from collections where id=%s", (coll,))

        if err != DB_NO_ERROR:
            log("Failed to query database for collection id %d" % (coll))
            raise DBQueryException(err)
       
        assert(self.basic.cursor.rowcount == 1)

        coldata = self.basic.cursor.fetchone()

        tname = coldata['streamtable']
        sql = """SELECT * FROM %s WHERE stream_id > %s""" \
                 % (tname, "%s")
      
        err = self._basicquery(sql, (minid,))
        if err != DB_NO_ERROR:
            log("Failed to query streams for collection id %d" % (coll))
            raise DBQueryException(err)
              
        selected = []
        while True:
            row = self.basic.cursor.fetchone()
            if row == None:
                break
            stream_dict = {}
            for k, v in row.items():
                if k == "id":
                    continue
                stream_dict[k] = v
            selected.append(stream_dict)
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Error while tidying up after querying streams by collection")
            raise DBQueryException(err)
        return selected

    def select_active_streams_by_collection(self, coll, lastactivity):
        """ Fetches all recently active streams belonging to a given collection
            id.

            Only streams with data after the lastactivity timestamp will be
            returned. To get all streams for a collection, set lastactivity
            to 0.

            Returns a list of stream ids
        """

        sql = "SELECT id FROM streams WHERE collection=%s AND lasttimestamp>%s"
        err = self._basicquery(sql, (coll, lastactivity))
        
        if err != DB_NO_ERROR:
            log("Failed to query active streams for collection id %d" % (coll))
            raise DBQueryException(err)
        
        active = []
        while True:
            row = self.basic.cursor.fetchone()
            if row == None:
                break
            for stream_id in row.values():
                active.append(stream_id)

        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Error while tidying up after querying active streams by collection")
            raise DBQueryException(err)
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
            table, columns, streamtable = self._get_data_table(col)
        except DBQueryException as e:
            yield(None, None, None, None, e)

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
        innselclause = " SELECT nntsclabel, timestamp "

        uniquecols = list(set([k[0] for k in aggcols]))
        for col in uniquecols:
            innselclause += ", " + col

        self.qb.add_clause("innersel", innselclause, [])

        self._generate_where(start_time, stop_time)
        
        # Constructing the outer SELECT query, which will aggregate across
        # each label to find the aggregate values
        outselclause = "SELECT nntsclabel"
        for col in labeled_groupcols:
            outselclause += "," + col
        for col in labeled_aggcols:
            outselclause += "," + col
        outselclause += " FROM ( "

        self.qb.add_clause("outsel", outselclause, binparam)

        outselend = " ) AS aggregates"
        self.qb.add_clause("outselend", outselend, [])

        outgroup = " GROUP BY nntsclabel"
        for col in groupcols:
            outgroup += ", " + col

        outgroup += " ORDER BY nntsclabel, timestamp"

        self.qb.add_clause("outgroup", outgroup, [])

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)
                continue

            err = self._generate_from(table, label, streams, streamtable)
            if err != DB_NO_ERROR:
                yield(None, label, None, None, DBQueryException(err))

            order = ["outsel", "innersel", "activestreams", "activejoin", 
                    "union", "joincondition", "wheretime", "outselend",
                    "outgroup"]
            query, params = self.qb.create_query(order)
   
            err = self._dataquery(query, params)
            if err != DB_NO_ERROR:
                yield(None, label, None, None, DBQueryException(err))
 
            fetched = self._query_data_generator()
            for row, errcode in fetched:
                if errcode != DB_NO_ERROR:
                    yield(None, label, None, None, DBQueryException(errcode))
                else:
                    yield (row, label, tscol, binsize, None)


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
            table, columns, streamtable = self._get_data_table(col)
        except DBQueryException as e:
            yield(None, None, None, None, e)

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
        while 'stream_id' in selectcols:
            selectcols.remove('stream_id')
        
        selectcols.append("activestreams.stream_id")
        selectcols.append("nntsclabel")

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
        orderclause = " ORDER BY nntsclabel, timestamp " 
        self.qb.add_clause("order", orderclause, [])

        for label, streams in labels.iteritems():
            if len(streams) == 0:
                yield(None, label, None, None, None)
                continue
            err = self._generate_from(table, label, streams, streamtable)
            if err != DB_NO_ERROR:
                yield(None, label, None, None, DBQueryException(err))
            
            order = ["select", "activestreams", "activejoin", "union",
                    "joincondition", "wheretime", "order"]
            sql, params = self.qb.create_query(order)

            err = self._dataquery(sql, params)
            if err != DB_NO_ERROR:
                yield(None, label, None, None, DBQueryException(err))

            fetched = self._query_data_generator()
            for row, errcode in fetched:
                if errcode != DB_NO_ERROR:
                    yield(None, label, None, None, DBQueryException(errcode))
                else:
                    yield (row, label, "timestamp", 0, None)


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


    def _get_last_timestamp_stream(self, sid, table):
        lastts = self.streamcache.fetch_stream(sid)

        if lastts != -1:
            # Reset cache timeout to keep stream in cache longer
            self.streamcache.store_stream(sid, lastts)
            self.cachehits += 1
            return DB_NO_ERROR, lastts

        self.dbqueries += 1

        # Nothing useful in cache, query data table for max timestamp
        # Warning, this isn't going to be fast so try to avoid doing
        # this wherever possible!
        query = "SELECT max(timestamp) FROM %s_" % (table)
        query += "%s"   # stream id goes in here
       
        err = self._basicquery(query, (sid,))
        if err != DB_NO_ERROR:
            return err, 0

        if self.basic.cursor.rowcount != 1:
            log("Unexpected number of results when querying for max timestamp: %d" % (self.basic.cursor.rowcount))
            return DB_CODING_ERROR, 0

        row = self.basic.cursor.fetchone()

        if row[0] == None:
            lastts = 0
        else:
            lastts = int(row[0])
            self.streamcache.store_stream(sid, lastts)
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            return err, 0
        
        return DB_NO_ERROR, lastts
    
    def _get_first_timestamp_stream(self, sid, table):
        firstts = self.streamcache.fetch_firstts(sid)

        if firstts != -1:
            # Reset cache timeout to keep stream in cache longer
            self.streamcache.store_firstts(sid, firstts)
            self.cachehits += 1
            return DB_NO_ERROR, firstts

        self.dbqueries += 1
        # Nothing useful in cache, query data table for max timestamp
        # Warning, this isn't going to be fast so try to avoid doing
        # this wherever possible!
        query = "SELECT min(timestamp) FROM %s_" % (table)
        query += "%s"   # stream id goes in here
       
        err = self._basicquery(query, (sid,))
        if err != DB_NO_ERROR:
            return err, 0

        if self.basic.cursor.rowcount != 1:
            log("Unexpected number of results when querying for min timestamp: %d" % (self.basic.cursor.rowcount))
            return DB_CODING_ERROR, 0

        row = self.basic.cursor.fetchone()

        if row[0] == None:
            firstts = 0
        else:
            firstts = int(row[0])
            self.streamcache.store_firstts(sid, firstts)
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            return err, 0
        
        return DB_NO_ERROR, firstts

    def filter_active_streams(self, collection, streams, start, end):
        filtered = []
        self.cachehits = 0
        self.dbqueries = 0
        
        try:
            table, columns, streamtable = self._get_data_table(collection)
        except DBQueryException as e:
            return e.code

        for sid in streams:
            err, lastts = self._get_last_timestamp_stream(sid, table)
            if err != DB_NO_ERROR:
                return err, []

            err, firstts = self._get_first_timestamp_stream(sid, table)
            if err != DB_NO_ERROR:
                return err, []

            if firstts <= end and lastts >= start:
                filtered.append(sid)

        #print len(streams), len(filtered), self.cachehits, self.dbqueries
        return DB_NO_ERROR, filtered

    # It looks like restricting the number of stream ids that are checked for
    # in the data table helps significantly with performance, so if we can
    # exclude all the streams that aren't in scope, we have a much smaller
    # search space.
    # TODO this needs to be tidied up, returning lists of arguments back
    # through multiple levels of function calls doesn't feel very nice, and
    # anyway, the whole way sql query parameters are done needs to be reworked.
    def _generate_from(self, table, label, streams, streamtable):
        """ Forms a FROM clause for an SQL query that encompasses all
            streams in the provided list that fit within a given time period.
        """
        uniquestreams = list(set(streams))

        # build the case statement that will label our stream ids
        self._generate_label_case(label, streams)

        # get all stream ids that are active in the period
        caseparams = []
        active = "FROM ((SELECT stream_id, CASE "
        
        if len(streams) > 0:
            active += " WHEN stream_id in (%s)" % (
                ",".join(["%s"] * len(streams)))
            active += " THEN %s"

            caseparams += streams
            caseparams.append(label)
        active += " END as nntsclabel FROM %s " % (streamtable)
       
        #print "Querying for streams", len(uniquestreams)
        
        active += "WHERE stream_id in ("
        count = len(uniquestreams)
        for i in range(0, count):
            active += "%s"
            if i != count - 1:
                active += ", "
        active += ")) AS activestreams"
        caseparams += uniquestreams

        self.qb.add_clause("activestreams", active, caseparams)
        self.qb.add_clause("activejoin", "INNER JOIN", [])

        joincond = "ON dataunion.stream_id = activestreams.stream_id)"
        self.qb.add_clause("joincondition", joincond, [])


        if table == "data_amp_traceroute":
            amp_traceroute.generate_union(self.qb, table, uniquestreams)
        else:
            self._generate_union(table, uniquestreams)

        return DB_NO_ERROR

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
        err = self._basicquery(
                    "SELECT * from collections where id=%s", (col,))
        if err != DB_NO_ERROR:
            log("Failed to query for collection id %d" % (col))
            raise DBQueryException(err)

        assert(self.basic.cursor.rowcount == 1)

        coldata = self.basic.cursor.fetchone()
        tname = coldata['datatable']
        streamtname = coldata['streamtable']
        module = coldata['module']
        subtype = coldata['modsubtype']

        table = tname

        # This is the quickest way to get the column names -- don't
        # try querying the data table itself because that could be slow
        # if the table is, for example, a complicated view.
        err = self._basicquery(
                "SELECT * from information_schema.columns WHERE table_name=%s",
                (tname,))

        if err != DB_NO_ERROR:
            log("Failed to query for data table column names")
            raise DBQueryException(err)

        columns = []
        while True:
            row = self.basic.cursor.fetchone()
            if row == None:
                break

            columns.append(row['column_name'])
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Error while tidying up after querying data table properties")
            raise DBQueryException(err)
        return table, columns, streamtname

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
      
    # This generator is called by a generator function one level up, but
    # nesting them all seems to work ok
    def _query_data_generator(self):
        while True:
            try:
                fetched = self.data.cursor.fetchmany(100)
            except psycopg2.extensions.QueryCanceledError:
                yield None, DB_QUERY_TIMEOUT 
            except psycopg2.OperationalError:
                yield None, DB_OPERATIONAL_ERROR
            except psycopg2.ProgrammingError as e:
                log(e)
                yield None, DB_CODING_ERROR
            except psycopg2.IntegrityError as e:
                # XXX Duplicate key shouldn't be an issue here
                log(e)
                yield None, DB_DATA_ERROR
            except psycopg2.DataError as e:
                log(e)
                yield None, DB_DATA_ERROR
            except KeyboardInterrupt:
                yield None, DB_INTERRUPTED
            except psycopg2.Error as e:
                log(e)
                yield None, DB_CODING_ERROR

            if fetched == []:
                break

            yield fetched, DB_NO_ERROR

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
