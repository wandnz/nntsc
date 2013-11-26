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


from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint, event, DDL, Index
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import and_, or_, not_, text
from sqlalchemy.sql.expression import select, outerjoin, func, label
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import reflection

import time, sys

from sqlalchemy.schema import DDLElement, DropTable, ForeignKeyConstraint, \
        DropConstraint, Table
from sqlalchemy.sql import table
from sqlalchemy.ext import compiler

from libnntscclient.logger import *

class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name=name
        self.selectable=selectable

class DropView(DDLElement):
    def __init__(self, name):
        self.name=name

@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (element.name, compiler.sql_compiler.process(element.selectable))

@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)

class Database:
    def __init__(self, dbname, dbuser, dbpass=None, dbhost=None, \
            new=False, debug=False):

        #no host means use the unix socket
        if dbhost == "":
            dbhost = None

        if dbpass == "":
            dbpass = None

        connect_string = URL('postgresql',username=dbuser,password=dbpass, \
                host=dbhost, database=dbname)

        if debug:
            log('Connecting to db using "%s"' % connect_string)

        self.init_error = False
        self.dbname = dbname
        self.engine = create_engine(connect_string, echo=debug, 
                implicit_returning = False)

        self.__reflect_db()

        self.conn = self.engine.connect()

        #self.stream_tables = {}
        #self.data_tables = {}

        #for name, tab in self.meta.tables.items():
        #    if name[0:5] == "data_":
        #        self.data_tables[name] = tab
        #    if name[0:8] == "streams_":
        #        self.stream_tables[name] = tab

        self.trans = self.conn.begin()
        self.pending = 0

    def __reflect_db(self):
        self.metadata = MetaData(self.engine)
        try:
            self.metadata.reflect(bind=self.engine)
        except OperationalError, e:
            log("Error binding to database %s" % (self.dbname))
            log("Are you sure you've specified the right database name?")
            self.init_error = True
            sys.exit(1)

        # reflect() is supposed to take a 'views' argument which will
        # force it to reflects views as well as tables, but our version of
        # sqlalchemy didn't like that. So fuck it, I'll just reflect the
        # views manually
        inspector = reflection.Inspector.from_engine(self.engine)
        views = inspector.get_view_names()
        for v in views:
            view_table = Table(v, self.metadata, autoload=True)


    def __del__(self):
        if not self.init_error:
            self.commit_transaction()
            self.conn.close()

    def create_view(self, name, query):

        t = table(name)

        for c in query.c:
            c._make_proxy(t)

        creator = DDL("CREATE VIEW %s AS %s" % (name, str(query.compile())))
        event.listen(self.metadata, 'after_create', creator)

        dropper = DDL("DROP VIEW %s" % (name))
        event.listen(self.metadata, 'before_drop', dropper)

        #CreateView(name, query).execute_at('after-create', self.metadata)
        #DropView(name).execute_at('before-drop', self.metadata)

        return t


    def build_databases(self, modules, new=False):
        if new:
            self.__delete_everything(self.engine)
            self.__reflect_db()

        if 'collections' not in self.metadata.tables:
            collections = Table('collections', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('module', String, nullable=False),
                Column('modsubtype', String, nullable=True),
                Column('streamtable', String, nullable=False),
                Column('datatable', String, nullable=False),
                UniqueConstraint('module', 'modsubtype')
            )
            collections.create()

        if 'streams' not in self.metadata.tables:
            streams = Table('streams', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('collection', Integer, ForeignKey('collections.id'),
                        nullable=False),
                Column('name', String, nullable=False),
                Column('lasttimestamp', Integer, nullable=False),
                Column('firsttimestamp', Integer, nullable=True),
            )

            streams.create()

            Index('index_streams_collection', streams.c.collection)

        # Create a useful function to select a mode from any data
        # http://scottrbailey.wordpress.com/2009/05/22/postgres-adding-custom-aggregates-most/
        mostfunc = text("""
            CREATE OR REPLACE FUNCTION _final_most(anyarray)
                RETURNS anyelement AS
            $BODY$
                SELECT a
                FROM unnest($1) a
                GROUP BY 1 ORDER BY count(1) DESC
                LIMIT 1;
            $BODY$
                LANGUAGE 'sql' IMMUTABLE;""")
        self.conn.execute(mostfunc)

        # we can't check IF EXISTS or use CREATE OR REPLACE, so just query it
        mostcount = self.conn.execute(
                """SELECT * from pg_proc WHERE proname='most';""")
        assert(mostcount.rowcount <= 1)

        # if it doesn't exist, create the aggregate function that applies
        # _final_most to multiple rows of data
        if mostcount.rowcount == 0:
            aggfunc = text("""
                CREATE AGGREGATE most(anyelement) (
                    SFUNC=array_append,
                    STYPE=anyarray,
                    FINALFUNC=_final_most,
                    INITCOND='{}'
                );""")
            self.conn.execute(aggfunc)

        for base,mod in modules.items():
            mod.tables(self)

        self.metadata.create_all()
        self.commit_transaction()

    def register_collection(self, mod, subtype, stable, dtable):
        table = self.metadata.tables['collections']

        try:
            self.conn.execute(table.insert(), module=mod, modsubtype=subtype,
                    streamtable=stable, datatable=dtable)
        except IntegrityError, e:
            self.rollback_transaction()
            log("Failed to register collection for %s:%s, probably already exists" % (mod, subtype))
            #print >> sys.stderr, e
            return -1

        self.commit_transaction()

    def register_new_stream(self, mod, subtype, name, ts):

        # Find the appropriate collection id
        coltable = self.metadata.tables['collections']

        sql = coltable.select().where(and_(coltable.c.module==mod,
                coltable.c.modsubtype==subtype))
        result = sql.execute()

        if result.rowcount == 0:
            log("Database Error: no collection for %s:%s" % (mod, subtype))
            return -1, -1

        if result.rowcount > 1:
            log("Database Error: duplicate collections for %s:%s" % (mod, subtype))
            return -1, -1

        col = result.fetchone()
        col_id = col['id']
        result.close()

        # Insert entry into the stream table
        sttable = self.metadata.tables['streams']

        try:
            result = self.conn.execute(sttable.insert(), collection=col_id,
                    name=name, lasttimestamp=0, firsttimestamp=ts)
        except IntegrityError, e:
            log("Failed to register stream %s for %s:%s, probably already exists" % (name, mod, subtype))
            #print >> sys.stderr, e
            return -1, -1

        # Return the new stream id
        newid = result.inserted_primary_key
        result.close()

        return col_id, newid[0]

    def __delete_everything(self, engine):
        #self.meta.drop_all(bind=engine)

        newmeta = MetaData()

        tbs = []
        all_fks = []
        views = []
        partitions = []

        inspector = reflection.Inspector.from_engine(self.engine)
        for table_name in inspector.get_table_names():
            fks = []
            for fk in inspector.get_foreign_keys(table_name):
                if not fk['name']:
                    continue
                fks.append(
                    ForeignKeyConstraint((),(),name=fk['name'])
                    )
            t = Table(table_name,newmeta,*fks)
            if table_name[0:5] == "part_":
                partitions.append(t)
            else:
                tbs.append(t)
            all_fks.extend(fks)

        for v in inspector.get_view_names():
            self.conn.execute(DropView(v))

        for fkc in all_fks:
            self.conn.execute(DropConstraint(fkc))

        for table in partitions:
            self.conn.execute(DropTable(table))

        for table in tbs:
            self.conn.execute(DropTable(table))

        self.commit_transaction()


    """ Find the correct module table for the specified stream_id """
    def __get_mod_table(self, stream_id):
        # XXX This seems kinda slow....
        for i in self.metadata.tables.keys():
            if i.find('streams_') != -1:
                mod = self.metadata.tables[i]
                sql = mod.select().where(mod.c.stream_id==stream_id)
                result = sql.execute()
                if result.rowcount == 1:
                    return mod

    def __get_data_table(self, stream_id):
        # XXX This seems kinda slow....
        for i in self.metadata.tables.keys():
            if i.find('data_') != -1:
                mod = self.metadata.tables[i]
                sql = mod.select().where(mod.c.stream_id==stream_id)
                result = sql.execute()
                if result.rowcount == 1:
                    return mod

    def list_collections(self):
        collections = []

        table = self.metadata.tables['collections']

        result = table.select().execute()
        for row in result:

            col = {}
            for k,v in row.items():
                col[k] = v
            collections.append(col)

        return collections

    def get_collection_schema(self, col_id):

        table = self.metadata.tables['collections']

        result = select([table.c.streamtable, table.c.datatable]).where(table.c.id ==col_id).execute()
        for row in result:
            stream_table = self.metadata.tables[row[0]]
            data_table = self.metadata.tables[row[1]]
            return stream_table.columns, data_table.columns

    def select_streams_by_module(self, mod):

        # Find all streams matching a given module type

        # For each stream:
        #   Form a dictionary containing all the relevant information about
        #   that stream (this will require info from both the combined streams
        #   table and the module/subtype specific table

        # Put all the dictionaries into a list

        col_t = self.metadata.tables['collections']
        streams_t = self.metadata.tables['streams']

        # Find the collection matching the given module
        sql = col_t.select().where(col_t.c.module == mod)
        result = sql.execute()

        stream_tables = {}

        for row in result:
            stream_tables[row['id']] = (row['streamtable'], row['modsubtype'])
        result.close()

        streams = []
        for cid, (tname, sub) in stream_tables.items():
            t = self.metadata.tables[tname]
            sql = t.join(streams_t, streams_t.c.id == t.c.stream_id).select().where(streams_t.c.collection==cid)
            result = sql.execute()

            for row in result:
                row_dict = {"modsubtype":sub}
                for k,v in row.items():
                    if k == 'id':
                        continue
                    row_dict[k] = v
                streams.append(row_dict)
            result.close()
        return streams

    def select_streams_by_collection(self, coll, minid):

        coll_t = self.metadata.tables['collections']
        streams_t = self.metadata.tables['streams']

        selected = []

        sql = coll_t.select().where(coll_t.c.id == coll)
        result = sql.execute()

        assert(result.rowcount == 1)
        coldata = result.fetchone()

        colstrtable = self.metadata.tables[coldata['streamtable']]

        sql = select([colstrtable, streams_t]).select_from(colstrtable.join(streams_t, streams_t.c.id == colstrtable.c.stream_id)).where(colstrtable.c.stream_id > minid);
        result = sql.execute()

        for row in result:
            stream_dict = {}
            for k,v in row.items():
                if k == "id":
                    continue
                stream_dict[k] = v
            selected.append(stream_dict)
        result.close()
        return selected



    def select_stream_by_id(self, stream_id):
        # find the mod table this id is in
        mod = self.__get_stream_table(stream_id)

        result = outerjoin(self.metadata.tables['streams'], mod).select( \
                mod.c.stream_id==stream_id).execute()

        stream = dict(result.fetchone())

        result.close()

        return stream

    def commit_transaction(self):
        # TODO: Better error handling!

        #print "Committing %d statements (%s)" % (self.pending, \
        #        time.strftime("%d %b %Y %H:%M:%S", time.localtime()))
        try:
            self.trans.commit()
        except:
            self.trans.rollback()
            raise
        self.trans = self.conn.begin()

    def rollback_transaction(self):
        #if self.pending == 0:
        #    return
        self.trans.rollback()
        self.trans = self.conn.begin()

    def update_timestamp(self, stream_id, lasttimestamp):
        table = self.metadata.tables['streams']
        result = self.conn.execute(table.update().where( \
                table.c.id==stream_id).values( \
                lasttimestamp=lasttimestamp))
        result.close()
        self.pending += 1

    def set_firsttimestamp(self, stream_id, ts):
        table = self.metadata.tables['streams']
        result = self.conn.execute(table.update().where( \
                table.c.id==stream_id).values( \
                firsttimestamp=ts))
        result.close()
        self.pending += 1


    def _get_aggregator(self, agg):
        if agg == "max":
            return func.max
        elif agg == "min":
            return func.min
        elif agg == "sum":
            return func.sum
        elif agg == "avg" or agg == "average":
            return func.avg
        elif agg == "count":
            return func.count
        elif agg == "stddev":
            return func.stddev
        else:
            log("Unsupported aggregator function: %s" % (aggregator))
            return None


    def _where_clause(self, table, start_time, stop_time, stream_ids):

        # Create the start time clause for our query
        if start_time:
            start_str = "%s >= %s" % (table.c.timestamp, start_time)
        else:
            start_str = ""

        # Create the stop time clause for our query
        if stop_time:
            stop_str = "%s <= %s" % (table.c.timestamp, stop_time)
        else:
            stop_str = ""

        # Create the streams clause for our query - make sure we
        # separate our terms with OR so we match any of our chosen
        # streams
        if stream_ids:
            # Need parentheses so that we have higher precedence over
            # any neighbouring ANDs
            stream_str="("

            ind = 0
            for i in stream_ids:
                stream_str += "%s = %s" % (table.c.stream_id, i)
                ind += 1
                # Don't put OR after the last stream!
                if ind != len(stream_ids):
                    stream_str += " OR "
            stream_str += ")"
        else:
            stream_str = ""

        # Start putting together our complete WHERE clause
        query = start_str

        # Don't place an AND unless we have something to go on both sides
        # of the AND!
        if query != "" and stop_str != "":
            query += " AND "
            query += stop_str

        if query != "" and stream_str != "":
            query += " AND "
            query += stream_str

        return query

    def _select_unmodified(self, table, wherecl, columns):
        if 'stream_id' not in columns:
            columns.append('stream_id')
        if 'timestamp' not in columns:
            columns.append('timestamp')
        tablecols = filter(lambda a: a.name in columns, table.columns)

        # Run the query and convert the results into something we can use
        result = select(tablecols).where(wherecl).order_by(
                table.c.timestamp).execute()

        data, frequency = self._form_datadict(result, tablecols, table.c.timestamp, 0)
        result.close()

        return data, frequency


    def _group_columns(self, table, selectors, groups, aggregator, bts=None):
        if type(aggregator) is str:
            # single (string) aggregator, use it for all columns
            aggfuncs = [self._get_aggregator(aggregator)] * len(selectors)
        else:
            # list (iterable) aggregator, different one per column
            aggfuncs = []
            for agg in aggregator:
                aggfuncs.append(self._get_aggregator(agg))

        # make sure we have a valid aggregator for each column
        if None in aggfuncs or len(aggfuncs) != len(selectors):
            return []

        if groups == None:
            groups = []

        index = 0
        rename = False
        aggcols = []
        groupcols = []

        # check if we have duplicate selector columns - if so then we will
        # need to rename them based on the aggregation function used
        if len(set(selectors)) < len(selectors):
            rename = True

        # iterate over the selectors rather than the table columns to ensure
        # any columns listed multiple times all get included appropriately
        for colname in selectors:
            # find the next (only) column with the matching name, if present
            column = next((x for x in table.columns if x.name == colname), None)
            if column is not None:
                labelstr = colname
                if rename:
                    # append the aggregate function name to differentiate this
                    # result column from any others using the same column name
                    labelstr += "_" + aggregator[index]
                newcol = label(labelstr, aggfuncs[index](column))
                aggcols.append(newcol)
            index += 1

        for colname in groups:
            column = next((x for x in table.columns if x.name == colname), None)
            if column is not None:
                groupcols.append(column)

        # If we are binning, put the timestamp column into the group list to
        # ensure the bins are of appropriate size
        # However, we also want to create a special aggregator column that
        # contains the maximum timestamp from the bin -- this helps a lot
        # when graphing ts data because the last data point will have the
        # timestamp of the most recent data.
        if bts is not None:
            aggts = label('timestamp', func.max(table.c.timestamp))
            aggcols.append(aggts)
            groupcols.append(bts)
            selectcols = aggcols + groupcols
        else:
            selectcols = aggcols + groupcols


        return selectcols, groupcols

    def _form_datadict(self, result, selectcols, tscol, size):
        """ Converts a result object into a list of dictionaries, one
            for each row. The dictionary is a map of column names to
            values.

            Also applies some heuristics across the returned result to try
            and determine the size of each bin (if data has been aggregated).
        """
        data = []
        tsdiff_dict = {}
        total_diffs = 0
        lastts = 0
        lastbin = 0
        perfect_bins = 0

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


        for r in result:
            # Collecting data for our binsize heuristics
            if lastts == 0:
                lastts = r['timestamp']
                lastbin = r[tscol]
            elif lastts != r['timestamp']:
                tsdiff = r['timestamp'] - lastts
                bindiff = r[tscol] - lastbin

                # Difference between bins matches our requested binsize
                if bindiff == size:
                    perfect_bins +=1

                if tsdiff in tsdiff_dict:
                    tsdiff_dict[tsdiff] += 1
                else:
                    tsdiff_dict[tsdiff] = 1

                total_diffs += 1
                lastts = r['timestamp']
                lastbin = r[tscol]

            # This is the bit that actually converts the result into a
            # dictionary and appends it to our data list
            foo = {}
            for i in range(0, len(selectcols)):
                foo[selectcols[i].name] = r[i]
            data.append(foo)

        if len(data) <= 1 or total_diffs == 0:
            if size < 300:
                binsize = 300
            else:
                binsize = size

            return data, binsize

        # If this check passes, we requested a binsize greater than the
        # measurement frequency (case 1, above)
        if perfect_bins / float(total_diffs) > 0.9:
            binsize = size
        else:
            # If we get here, then our binsize is more than likely smaller
            # than our measurement frequency. Now, we need to try and figure
            # out what that frequency was...

            # Try and set a sensible default frequency. In particular, let's
            # not set the frequency too low, otherwise our graphs will end up
            # lots of gaps if we get it wrong.
            if size < 300:
                binsize = 300
            else:
                binsize = size

            # Find a suitable mode in all the timestamp differences.
            # I require a strong mode, i.e. at least half the differences
            # must be equal to the mode, as there shouldn't be a lot of
            # variation in the time differences (unless your measurements are
            # very patchy).
            for td, count in tsdiff_dict.items():
                if count >= 0.5 * total_diffs:
                    binsize = td
                    break

        return data, binsize

    def _group_select(self, selectcols, wherecl, groupcols, tscol, size):
        query = select(selectcols).where(wherecl).group_by(*groupcols).order_by(tscol)
        result = query.execute()
        data, binsize = self._form_datadict(result, selectcols, tscol, size)
        result.close()
        return data, binsize

    def _select_binned(self, table, wherecl, selectors, groups, size, aggre):
        bts = label('binstart', table.c.timestamp - (table.c.timestamp % size))
        selectcols, groupcols = self._group_columns(table, selectors, groups,
                aggre, bts)
        return self._group_select(selectcols, wherecl, groupcols, bts, size)


    def _select_unbinned(self, table, wherecl, selectors, groups, size, aggre):
        selectcols, groupcols = self._group_columns(table, selectors, groups,
                aggre)

        # TODO are min_timestamp and max_timestamp actually used anywhere?
        # The other path through the select_binned function calls these
        # columns "binstart" and "timestamp".
        mints = label("min_timestamp", func.min(table.c.timestamp))
        selectcols.append(mints)
        selectcols.append(label("max_timestamp", func.max(table.c.timestamp)))
        # this extra timestamp column is the same as max_timestamp, but
        # is expected by later functions to have the name "timestamp"
        selectcols.append(label("timestamp", func.max(table.c.timestamp)))

        return self._group_select(selectcols, wherecl, groupcols, mints, size)



    """
        Get data from the database

        Both start_time and stop_time are inclusive values
    """
    def select_data(self, col, stream_ids, selectcols, start_time=None,
            stop_time=None):

        coll_t = self.metadata.tables['collections']
        res = select([coll_t.c.datatable]).select_from(coll_t).where(coll_t.c.id == col).execute()

        assert(res.rowcount == 1)

        datatable = res.fetchone()[0]
        table = self.metadata.tables[datatable]

        wherecl = self._where_clause(table, start_time, stop_time, stream_ids)

        return self._select_unmodified(table, wherecl, selectcols)


    def select_aggregated_data(self, collection, stream_ids, aggcols,
            start_time=None, stop_time=None, groupcols=None, binsize=0,
            aggregator="avg"):

        #print "SELECT AGGREGATED:", stream_ids
        coll_t = self.metadata.tables['collections']
        res = select([coll_t.c.datatable]).select_from(coll_t).where(coll_t.c.id == collection).execute()

        assert(res.rowcount == 1)

        datatable = res.fetchone()[0]
        table = self.metadata.tables[datatable]

        wherecl = self._where_clause(table, start_time, stop_time, stream_ids)

        if binsize == 0 and groupcols == None:
            return self._select_unmodified(table, wherecl, aggcols)

        if groupcols == None:
            groupcols = ['stream_id']
        elif 'stream_id' not in groupcols:
            groupcols.append('stream_id')

        # if binsize is not set, or if it is set to the same as the duration
        # then fetch unbinned data
        if binsize == 0 or binsize == (stop_time - start_time):
            return self._select_unbinned(table, wherecl, aggcols, groupcols,
                    binsize, aggregator)

        return self._select_binned(table, wherecl, aggcols, groupcols,
                binsize, aggregator)


    # The SQL generated here should end up looking something like this, fill
    # in DATATABLE, BINSIZE, STREAM_ID as appropriate:
    #
    #   SELECT max(recent) AS timestamp,
    #       binstart,
    #       array_agg(rtt_avg) AS values,
    #       avg(loss_avg) AS loss
    #   FROM (
    #       SELECT max(timestamp) AS recent,
    #           binstart,
    #           avg(rtt) AS rtt_avg,
    #           avg(loss) AS loss_avg
    #       FROM (
    #           SELECT timestamp,
    #               timestamp-timestamp%BINSIZE AS binstart,
    #               rtt,
    #               loss,
    #               ntile(20) OVER (
    #                   PARTITION BY timestamp-timestamp%BINSIZE
    #                   ORDER BY rtt
    #               )
    #           FROM DATATABLE
    #           WHERE stream_id=STREAM_ID
    #           ORDER BY binstart
    #       ) AS ntiles
    #       GROUP BY binstart, ntile ORDER BY binstart, rtt_avg
    #   ) AS agg
    #   GROUP BY binstart"
    #
    # TODO ideally this should properly use the aggcols, groupcols and
    # aggregator arguments to work with any data in the same manner that the
    # other data fetching functions do. Shouldn't be too difficult, but a few
    # assumptions are being made about which column is the one to get ntiles
    # for and to array_agg at the top level.
    def select_percentile_data(self, collection, stream_ids, aggcols,
            start_time=None, stop_time=None, groupcols=None, binsize=0,
            aggregator="avg"):

        coll_t = self.metadata.tables['collections']
        res = select([coll_t.c.datatable]).select_from(coll_t).where(coll_t.c.id == collection).execute()

        assert(res.rowcount == 1)

        datatable = res.fetchone()[0]
        table = self.metadata.tables[datatable]

        wherecl = self._where_clause(table, start_time, stop_time, stream_ids)
        bts = label("binstart", table.c.timestamp - (table.c.timestamp % binsize))

        # First, generate the ntiles column listing the ntile that each rtt
        # measurement falls within.
        ntiles = select([
                    table.c.timestamp,
                    bts,
                    table.c.rtt,
                    table.c.loss,
                    func.ntile(20).over(partition_by=bts, order_by="rtt").label("ntile")
                ]).where(wherecl).order_by(bts).alias("ntiles")

        # Second, aggregate across each ntile to find the average values within
        # each group. We should now have a timestamp for the start of each bin
        # that a measurement falls in, the most recent timestamp from that bin
        # that has been aggregated, and an average rtt/loss for every ntile
        # within each bin.
        agg = select([
                    func.max(ntiles.c.timestamp).label("recent"),
                    ntiles.c.binstart,
                    func.avg(ntiles.c.rtt).label("rtt_avg"),
                    func.avg(ntiles.c.loss).label("loss_avg")
                ]).select_from(ntiles).group_by(ntiles.c.binstart, ntiles.c.ntile).order_by("binstart", "rtt_avg").alias("agg")

        # Finally, collapse the multiple ntile measurements down into an array
        # to give one row per bin, containing everything we are after.
        query = select([
                func.max(agg.c.recent).label("timestamp"),
                agg.c.binstart,
                # array_agg() leaves NULLs in the array, so we now use
                # string_agg() which appears to ignore trailing NULLs (the
                # values are sorted, so they are at the end). It does take
                # a small amount longer to convert to string and back to array,
                # but it's probably still quicker than doing a tidy pass using
                # the format_data() callback.
                # TODO cast this back to floats before returning it
                func.coalesce(func.string_to_array(func.string_agg(func.cast(agg.c.rtt_avg, String), ','), ','), []).label("values"),
                func.avg(agg.c.loss_avg).label("loss")
                ]).select_from(agg).group_by(agg.c.binstart)

        result = query.execute()

        # query.c is a collection of the column objects, but needs to be a list
        data, binsize = self._form_datadict(result, list(query.c), "binstart",
                binsize)
        result.close()
        return data, binsize

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
