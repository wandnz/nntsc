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


import time
import psycopg2
import psycopg2.extras
from libnntscclient.logger import *
from libnntsc.dberrorcodes import *
from libnntsc.streamcache import StreamCache

class NNTSCCursor(object):
    def __init__(self, connstr, autocommit=False, name=None):
        self.cursorname = name
        self.connstr = connstr
        self.autocommit = autocommit

        self.conn = None
        self.cursor = None

    def destroy(self):
        if self.cursor is not None:
            #self.cursor.close()
            self.cursor = None

        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connect(self, retrywait):
        logmessage = False

        while self.conn is None:
            try:
                self.conn = psycopg2.connect(self.connstr)
            except psycopg2.DatabaseError as e:
                if not logmessage:
                    log("Error connecting to database: %s" % e)
                    log("Retrying every %d seconds" % retrywait)
                    logmessage = True
                self.conn = None
                time.sleep(retrywait)

        self.conn.autocommit = self.autocommit

        if logmessage:
            log("Successfully connected to NNTSC database")

        self.cursor = None
        return 0

    def reconnect(self):
        time.sleep(5)
        self.destroy()
        self.connect(5)

    def createcursor(self):

        try:
            if self.cursorname is not None:
                self.cursor = self.conn.cursor(self.cursorname,
                        cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                self.cursor = self.conn.cursor(
                        cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.OperationalError as e:
            log("Database disconnect while resetting cursor")
            self.cursor = None
            raise DBQueryException(DB_OPERATIONAL_ERROR)
        except psycopg2.DatabaseError as e:
            log("Failed to create cursor: %s" % e)
            self.cursor = None
            raise DBQueryException(DB_CODING_ERROR)

    def executequery(self, query, params):
        if self.cursor is None:
            self.createcursor()

        try:
            if params is not None:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            raise DBQueryException(DB_QUERY_TIMEOUT)
        except psycopg2.OperationalError:
            log("Database appears to have disappeared -- reconnecting")
            self.reconnect()
            raise DBQueryException(DB_OPERATIONAL_ERROR)
        except psycopg2.ProgrammingError as e:
            log(e)
            self.conn.rollback()
            raise DBQueryException(DB_CODING_ERROR)
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            if "duplicate key " in str(e):
                raise DBQueryException(DB_DUPLICATE_KEY)
            log(e)
            raise DBQueryException(DB_DATA_ERROR)
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            raise DBQueryException(DB_DATA_ERROR)
        except KeyboardInterrupt:
            raise DBQueryException(DB_INTERRUPTED)
        except psycopg2.Error as e:
            # Some disconnect cases seem to end up here :/
            if "server closed the connection unexpectedly" in str(e):
                log("Database appears to have disappeared, fell through to catch-all error case -- reconnecting")
                self.reconnect()
                raise DBQueryException(DB_OPERATIONAL_ERROR)

            log(e.pgerror)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            raise DBQueryException(DB_GENERIC_ERROR)

    def closecursor(self):
        if self.cursor is None:
            return

        if self.conn is None:
            self.cursor = None
            return
        err = DB_NO_ERROR

        try:
            self.cursor.close()
            err = DB_NO_ERROR
        except psycopg2.OperationalError:
            log("Database appears to have disappeared while closing cursor -- reconnecting")
            self.reconnect()
            # Don't return an error code, cursor will be successfully closed
            # even if we have to reconnect first!
            err = DB_NO_ERROR
        except psycopg2.ProgrammingError as e:
            log(e)
            err = DB_CODING_ERROR
        except psycopg2.IntegrityError as e:
            log(e)
            err = DB_DATA_ERROR
        except psycopg2.DataError as e:
            log(e)
            err = DB_DATA_ERROR
        except KeyboardInterrupt:
            err = DB_INTERRUPTED
        except psycopg2.Error as e:
            # Some disconnect cases seem to end up here :/
            if "server closed the connection unexpectedly" in e.str():
                log("Database appears to have disappeared, fell through to catch-all error case -- reconnecting")
                self.reconnect()
                err = DB_NO_ERROR
            log(e.pgerror)
            err = DB_GENERIC_ERROR

        if err != DB_NO_ERROR:
            log("Error while closing database cursor")
            raise DBQueryException(err)

        self.cursor = None


    def commit(self):
        if self.conn is None:
            raise DBQueryException(DB_NO_CURSOR)

        err = DB_NO_ERROR
        try:
            self.conn.commit()
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            err = DB_QUERY_TIMEOUT
        except psycopg2.OperationalError:
            log("Database appears to have disappeared during commit -- reconnecting")
            self.reconnect()
            err = DB_OPERATIONAL_ERROR
        except psycopg2.ProgrammingError as e:
            log(e)
            err = DB_CODING_ERROR
        except psycopg2.IntegrityError as e:
            log(e)
            err = DB_DATA_ERROR
        except psycopg2.DataError as e:
            log(e)
            err = DB_DATA_ERROR
        except KeyboardInterrupt:
            err = DB_INTERRUPTED
        except psycopg2.Error as e:
            # Some disconnect cases seem to end up here :/
            if "server closed the connection unexpectedly" in e.str():
                log("Database appears to have disappeared, fell through to catch-all error case -- reconnecting")
                self.reconnect()
                err = DB_OPERATIONAL_ERROR
            log(e.pgerror)
            err = DB_GENERIC_ERROR

        if err != DB_NO_ERROR:
            log("Error while committing to database")
            raise DBQueryException(err)


class DatabaseCore(object):
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost=None,
            timeout=0, cachetime=0):

        #no host means use the unix socket
        if dbhost == "":
            dbhost = None

        if dbpass == "":
            dbpass = None

        if dbuser == "":
            dbuser = None

        connstr = "dbname=%s" % (dbname)
        if dbuser != "" and dbuser != None:
            connstr += " user=%s" % (dbuser)
        if dbpass != "" and dbpass != None:
            connstr += " password=%s" % (dbpass)
        if dbhost != "" and dbhost != None:
            connstr += " host=%s" % (dbhost)

        if timeout != 0:
            connstr += " options='-c statement_timeout=%d'" % (timeout * 1000.0)

        self.conn = None
        self.connstr = connstr

        self.basic = NNTSCCursor(self.connstr, False, None)

        self.streamcache = StreamCache(dbname, cachetime)

    def connect_db(self, retrywait):
        return self.basic.connect(retrywait)

    def disconnect(self):
        self.basic.destroy()

    def __del__(self):
        try:
            self.basic.commit()
            self.basic.destroy()
        except DBQueryException as e:
            pass

    def _basicquery(self, query, params=None):
        while True:
            try:
                self.basic.executequery(query, params)
            except DBQueryException as e:
                if e.code == DB_OPERATIONAL_ERROR:
                    # Retry the query, as we just reconnected
                    continue
                else:
                    raise

            self.basic.commit()
            break

    def _releasebasic(self):
        self.basic.closecursor()

    def list_collections(self):
        collections = []

        self._basicquery("SELECT * from collections")
        while True:
            row = self.basic.cursor.fetchone()

            if row is None:
                break
            col = {}
            for k, v in row.items():
                col[k] = v
            collections.append(col)
        self._releasebasic()
        return collections

    def select_streams_by_module(self, mod):
        """ Fetches all streams that belong to collections that have a common
            parent module, e.g. amp or rrd.

            For example, passing "amp" into this function would give you
            all amp-icmp and amp-traceroute streams.

            Note that if you want the streams for a single collection, you
            should use select_streams_by_collection.

            Returns a list of streams, where each stream is a dictionary
            describing all of the stream parameters.
        """

        # Find all streams for a given parent collection, e.g. amp
        #
        # For each stream:
        #   Form a dictionary containing all the relevant information about
        #   that stream (this will require info from both the combined streams
        #   table and the module/subtype specific table
        # Put all the dictionaries into a list

        # Find the collections matching this module

        self._basicquery("SELECT * from collections where module=%s",
                    (mod,))
        streamtables = {}

        cols = self.basic.cursor.fetchall()
        for c in cols:
            streamtables[c["id"]] = (c["streamtable"], c["modsubtype"])

        self._releasebasic()

        streams = []
        for cid, (tname, sub) in streamtables.items():
            sql = """ SELECT * FROM %s """ % (tname)

            self._basicquery(sql, (cid,))

            while True:
                row = self.basic.cursor.fetchone()
                if row is None:
                    break
                row_dict = {"modsubtype":sub}
                for k, v in row.items():
                    if k == "id":
                        continue
                    row_dict[k] = v
                streams.append(row_dict)

        self._releasebasic()
        return streams




class DBInsert(DatabaseCore):
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost=None,
            cachetime=0):

        super(DBInsert, self).__init__(dbname, dbuser, dbpass, dbhost,
                cachetime=cachetime)

    def connect_db(self, retrywait):
        self.streams = NNTSCCursor(self.connstr, False, None)
        self.data = NNTSCCursor(self.connstr, False, None)

        if self.streams.connect(retrywait) == -1:
            return -1
        if self.data.connect(retrywait) == -1:
            return -1
        return super(DBInsert, self).connect_db(retrywait)

    def disconnect(self):
        self.streams.destroy()
        self.data.destroy()
        super(DBInsert, self).disconnect()

    def _streamsquery(self, query, params=None):
        self.streams.executequery(query, params)

    def _dataquery(self, query, params=None):
        self.data.executequery(query, params)

    def commit_streams(self):
        self.streams.commit()
        self.streams.closecursor()

    def commit_data(self):
        self.data.commit()
        self.data.closecursor()

    def create_aggregators(self):
        # Create a useful function to select a mode from any data
        # http://scottrbailey.wordpress.com/2009/05/22/postgres-adding-custom-aggregates-most/
        mostfunc = """
            CREATE OR REPLACE FUNCTION _final_most(anyarray)
                RETURNS anyelement AS
            $BODY$
                SELECT a
                FROM unnest($1) a
                GROUP BY 1 ORDER BY count(1) DESC
                LIMIT 1;
            $BODY$
                LANGUAGE 'sql' IMMUTABLE;"""

        self._basicquery(mostfunc)

        smokefunc = """
            CREATE OR REPLACE FUNCTION _final_smoke(anyarray)
                RETURNS numeric[] AS
            $BODY$
                SELECT array_agg(avg)::numeric[] FROM (
                    SELECT avg(foo), ntile FROM (
                        SELECT foo, ntile(20) OVER (PARTITION BY one ORDER BY foo) FROM (
                            SELECT 1 as one, unnest($1) as foo
                        ) as a WHERE foo IS NOT NULL
                    ) as b GROUP BY ntile ORDER BY ntile
                ) as c;
            $BODY$
                LANGUAGE 'sql' IMMUTABLE;"""

        self._basicquery(smokefunc)

        # we can't check IF EXISTS or use CREATE OR REPLACE, so just query it
        self._basicquery("""SELECT * from pg_proc WHERE proname='most';""")
        assert(self.basic.cursor.rowcount <= 1)

        # if it doesn't exist, create the aggregate function that applies
        # _final_most to multiple rows of data
        if self.basic.cursor.rowcount == 0:
            aggfunc = """
                CREATE AGGREGATE most(anyelement) (
                    SFUNC=array_append,
                    STYPE=anyarray,
                    FINALFUNC=_final_most,
                    INITCOND='{}'
                );"""
            self._basicquery(aggfunc)

        self._basicquery(
                """SELECT * from pg_proc WHERE proname='smokearray';""")

        if self.basic.cursor.rowcount == 0:
            aggfunc = """
                CREATE AGGREGATE smokearray(anyarray) (
                SFUNC=array_cat,
                STYPE=anyarray,
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );"""
            self._basicquery(aggfunc)

        self._basicquery(
                """SELECT * from pg_proc WHERE proname='smoke';""")

        if self.basic.cursor.rowcount == 0:
            aggfunc = """
                CREATE AGGREGATE smoke(numeric) (
                SFUNC=array_append,
                STYPE=numeric[],
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );"""
            self._basicquery(aggfunc)

        self._releasebasic()

    def create_index(self, name, table, columns):
        if len(columns) == 0:
            return DB_NO_ERROR

        if name == "":
            # XXX Not ideal but people should provide properly named
            # indexes anyway!
            name = "%s_%s_idx" % (table, columns[0])

        # Unfortunately we have to explicitly check if the index exists
        # before attempting this because there is no CREATE INDEX IF
        # NOT EXISTS
        indexcheck = """SELECT * FROM pg_class c JOIN pg_namespace n ON
                    n.oid = c.relnamespace WHERE
                    c.relname = %s AND
                    n.nspname = 'public'"""

        self._basicquery(indexcheck, (name,))

        if self.basic.cursor.rowcount != 0:
            # Already exists, no need to create
            return

        self._releasebasic()

        index = "CREATE INDEX %s ON %s"
        index += "(%s)" % (",".join(["%s"] * len(columns)))

        params = tuple([name] + [table] + columns)

        # Annoying that I can't parameterise this SQL nicely
        self._streamsquery(index % params)


    def build_databases(self, modules, new=False):
        if new:
            self.__delete_everything()

        self.create_aggregators()

        # Function to create a sequence if it doesn't already exist
        # Borrowed from an answer on stack overflow:
        # http://stackoverflow.com/questions/11905868/check-if-sequence-exists-postgres-plpgsql
        seqfunc = """
            CREATE OR REPLACE FUNCTION create_seq(_seq text,
                    _schema text = 'public') RETURNS void AS
            $func$
            DECLARE
               _fullname text := quote_ident(_seq);

            BEGIN
            -- If an object of the name exists, the first RAISE EXCEPTION
            -- raises a meaningful message immediately.
            -- Else, the failed cast to regclass raises its own exception
            -- "undefined_table".
            -- This particular error triggers sequence creation further down.

            RAISE EXCEPTION 'Object >>%<< of type "%" already exists.'
               ,_fullname
               ,(SELECT c.relkind FROM pg_class c
                 WHERE  c.oid = _fullname::regclass    -- error if non-existent
                ) USING ERRCODE = '23505';

            EXCEPTION WHEN undefined_table THEN -- SQLSTATE '42P01'
                EXECUTE 'CREATE SEQUENCE ' || _fullname;
                RAISE NOTICE 'New sequence >>%<< created.', _fullname;

            END
            $func$  LANGUAGE plpgsql STRICT;

            COMMENT ON FUNCTION create_seq(text, text) IS
                'Create new seq if name is free.
                 $1 _seq  .. name of sequence
                 $2 _name .. name of schema (optional; default is "public")';
        """
        self._basicquery(seqfunc)

        coltable = """CREATE TABLE IF NOT EXISTS collections (
                id SERIAL PRIMARY KEY,
                module varchar NOT NULL,
                modsubtype character varying,
                streamtable character varying NOT NULL,
                datatable character varying NOT NULL,
                UNIQUE (module, modsubtype))"""

        self._basicquery(coltable)

        createseq = """SELECT create_seq('streams_id_seq');"""
        try:
            self._basicquery(createseq)
        except DBQueryException as e:
            # Not the best error handling, but should stop us from falling
            # over if the sequence already exists
            if e.code != DB_DATA_ERROR:
                raise


        self._releasebasic()

        for mod in list(modules.values()):
            mod.tables(self)

    def get_collection_id(self, mod, subtype):

        # Find the appropriate collection id
        colcheck = """SELECT * FROM collections WHERE module=%s
                    AND modsubtype=%s"""

        self._basicquery(colcheck, (mod, subtype))

        if self.basic.cursor.rowcount > 1:
            log("Database Error: duplicate collections for %s:%s" % (mod, subtype))
            self._releasebasic()
            return -1

        if self.basic.cursor.rowcount == 0:
            col_id = 0
        else:
            col = self.basic.cursor.fetchone()
            col_id = col['id']

        self._releasebasic()
        return col_id

    def register_collection(self, mod, subtype, stable, dtable):

        # Check if the collection already exists
        colid = self.get_collection_id(mod, subtype)

        if colid != 0:
            # Collection already exists
            return

        insert = """INSERT INTO collections (module, modsubtype,
                streamtable, datatable) VALUES (%s, %s, %s, %s)"""

        self._basicquery(insert, (mod, subtype, stable, dtable))
        self._releasebasic()


    def clone_table(self, original, streamid, foreignkey=None):
        tablename = original + "_" + str(streamid)

        # LIKE can't copy foreign keys so we have to explicitly add the
        # one we really want
        query = "CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING INDEXES INCLUDING CONSTRAINTS" % (tablename, original)
        if foreignkey is not None:
            query += ", %s)" % (foreignkey)
        else:
            query += ")"

        self._streamsquery(query)

    def add_foreign_key(self, tablename, column, foreigntable, foreigncolumn):
        # XXX Only supports one column foreign keys for now

        # TODO, validate these table names and columns to make sure they
        # exist
        query = "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE" % (tablename, column, foreigntable, foreigncolumn)

        self._streamsquery(query)

    def __delete_everything(self):

        self._basicquery("""SELECT table_schema, table_name FROM
                    information_schema.tables WHERE table_schema='public'
                    ORDER BY table_schema, table_name""")

        # XXX This may use a lot of memory :0
        rows = self.basic.cursor.fetchall()
        for r in rows:
            self._basicquery("""DROP TABLE IF EXISTS %s CASCADE""" % (r['table_name'],))

        self._basicquery("""DROP SEQUENCE IF EXISTS "streams_id_seq" """)
        self._releasebasic()

    def update_timestamp(self, datatable, stream_ids, lasttimestamp, is_influx):

        if is_influx:
            return

        dbkey = "postgres"

        self.streamcache.update_timestamps(dbkey, datatable, stream_ids,
                lasttimestamp)
        #log("Updated timestamp for %d streams to %d" % (len(stream_ids),
        #            lasttimestamp))

        return

    def get_last_timestamp(self, table, streamid):
        lastdict = self.streamcache.fetch_all_last_timestamps("postgres",
                table)

        if streamid not in lastdict:
            # Nothing useful in cache, query data table for max timestamp
            # Warning, this isn't going to be fast so try to avoid doing
            # this wherever possible!
            query = "SELECT max(timestamp) FROM %s_" % (table)
            query += "%s"   # stream id goes in here

            self._basicquery(query, (streamid,))

            if self.basic.cursor.rowcount != 1:
                log("Unexpected number of results when querying for max timestamp: %d" % (self.basic.cursor.rowcount))
                raise DBQueryException(DB_CODING_ERROR)

            row = self.basic.cursor.fetchone()
            if row[0] is None:
                return 0

            lastts = int(row[0])
            lastdict[streamid] = lastts
            self.streamcache.set_last_timestamps("postgres", table, lastdict)

            self._releasebasic()
        else:
            lastts = lastdict[streamid]
        return lastts


    def find_existing_stream(self, st, props):
        # We know a stream already exists that matches the given properties
        # so we just need to find it

        wherecl = "WHERE "
        params = []
        keys = list(props.keys())
        for i in range(0, len(keys)):
            if i != 0:
                wherecl += " AND "
            wherecl += keys[i] + "=%s"
            params += [props[keys[i]]]

        query = "SELECT stream_id FROM %s " % (st)
        query += wherecl

        self._basicquery(query, tuple(params))

        if self.basic.cursor.rowcount != 1:
            log("Unexpected number of matches when searching for existing stream: %d" % (self.basic.cursor.rowcount))
            log(query % tuple(params))
            raise DBQueryException(DB_CODING_ERROR)

        row = self.basic.cursor.fetchone()

        self._releasebasic()
        return row[0]


    def insert_stream(self, tablename, datatable, timestamp, streamprops,
            createdatatable=True):

        # insert stream into our stream table
        colstr = "(stream_id"
        values = []
        for k, v in streamprops.items():
            colstr += ', "%s"' % (k)
            values.append(v)
        colstr += ") "

        params = tuple(values)
        insert = "INSERT INTO %s " % (tablename)
        insert += colstr
        insert += "VALUES (nextval('streams_id_seq'), %s)" % \
                (",".join(["%s"] * len(values)))
        insert += " RETURNING stream_id"

        try:
            self._streamsquery(insert, params)
        except DBQueryException as e:
            if e.code == DB_DUPLICATE_KEY:
                return self.find_existing_stream(tablename, streamprops)
            else:
                raise

        # Grab the new stream ID so we can return it
        newid = self.streams.cursor.fetchone()[0]

        if createdatatable:
            dbkey = "postgres"
        else:
            dbkey = "influx"

        # Cache the observed timestamp as the first timestamp
        if timestamp != 0 and dbkey == "postgres":
            self.streamcache.update_timestamps(dbkey, datatable, [newid],
                    timestamp, timestamp)

        # Create a new data table for this stream, using the "base" data
        # table as a template
        if createdatatable:
            self.clone_table(datatable, newid)

        # Resist the urge to commit streams or do any live exporting here!
        #
        # Some collections have additional steps that they
        # need to perform before the new tables etc should be committed.
        # For example, amp traceroute needs to create the corresponding
        # path table. If you commit here then get interrupted, there will be
        # a stream without a matching path table and NNTSC will error on
        # restart.
        #
        # Also, don't live export the stream until all the new stream stuff
        # has been committed otherwise you will get netevmon asking for
        # data for a stream that hasn't had its data table committed yet.
        return newid

    def custom_insert(self, customsql, values):
        self._dataquery(customsql, values)
        result = self.data.cursor.fetchone()
        return result

    def insert_data(self, tablename, collection, stream, ts, result,
            casts=None):

        colstr = "(stream_id, timestamp"
        valstr = "%s, %s"
        values = [stream, ts]

        if casts is None:
            casts = {}

        for k, v in result.items():
            colstr += ', "%s"' % (k)
            values.append(v)

            if k in casts:
                valstr += ", CAST(%s AS " + casts[k] + ")"
            else:
                valstr += ", %s"
        colstr += ") "

        params = tuple(values)
        insert = "INSERT INTO %s_%s " % (tablename, stream)
        insert += colstr
        insert += "VALUES (%s)" % (valstr)

        self._dataquery(insert, params)

    def _columns_sql(self, name, columns):

        basesql = ""

        for c in columns:
            if "name" not in c:
                log("Unnamed column when creating streams table %s -- skipping" % (name))
                continue

            if "type" not in c:
                log("Column %s has no type when creating streams table %s -- skipping" % (c["name"], name))
                continue

            basesql += ', "%s" %s ' % (c["name"], c["type"])

            if "null" in c and c["null"] is False:
                basesql += "NOT NULL "

            if "default" in c:
                basesql += "DEFAULT %s " % (c["default"])

            if "unique" in c and c["unique"] is True:
                basesql += "UNIQUE "

        return basesql

    def create_misc_table(self, name, columns, uniquecols=None):
        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)
        coltext = self._columns_sql(name, columns)

        # Remove the leading comma
        basesql += coltext[1:]

        if uniquecols:
            basesql += ", UNIQUE ("

            for i in range(0, len(uniquecols)):
                if i != 0:
                    basesql += ", "
                basesql += "%s" % (uniquecols[i])

            basesql += ")"
        basesql += ")"

        self._basicquery(basesql)
        self._releasebasic()

    def create_data_table(self, name, columns):

        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)

        basesql += "stream_id integer NOT NULL, timestamp integer NOT NULL"

        basesql += self._columns_sql(name, columns)
        basesql += ")"

        self._basicquery(basesql)
        self._releasebasic()

        # Automatically create an index on timestamp
        self.create_index("%s_%s_idx" % (name, "timestamp"), name,
                    ["timestamp"])

    def create_streams_table(self, name, columns, uniquecols=None):

        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)
        basesql += "stream_id integer PRIMARY KEY "
        basesql += "DEFAULT nextval('streams_id_seq')"
        basesql += self._columns_sql(name, columns)

        if uniquecols:
            basesql += ", UNIQUE ("

            for i in range(0, len(uniquecols)):
                if i != 0:
                    basesql += ", "
                basesql += '"%s"' % (uniquecols[i])

            basesql += ")"
        basesql += ")"

        self._basicquery(basesql)
        self._releasebasic()

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
