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


import time, sys
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

        while self.conn == None:
            try:
                self.conn = psycopg2.connect(self.connstr)
            except psycopg2.DatabaseError as e:
                if not logmessage:
                    log("Error connecting to database: %s" % e)
                    log("Retrying every %d seconds" % retrywait);
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
            return DB_OPERATIONAL_ERROR
        except psycopg2.DatabaseError as e:
            log("Failed to create cursor: %s" % e)
            self.cursor = None
            return DB_CODING_ERROR

        return DB_NO_ERROR

    def executequery(self, query, params):
        if self.cursor is None:
            err = self.createcursor()
            if err != DB_NO_ERROR:
                return err

        try:
            if params is not None:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            return DB_QUERY_TIMEOUT        
        except psycopg2.OperationalError:
            log("Database appears to have disappeared -- reconnecting")
            self.reconnect()
            return DB_OPERATIONAL_ERROR
        except psycopg2.ProgrammingError as e:
            log(e)
            self.conn.rollback()
            return DB_CODING_ERROR
        except psycopg2.IntegrityError as e:
            log(e)
            self.conn.rollback()
            if " duplicate " in str(e):
                return DB_DUPLICATE_KEY
            return DB_DATA_ERROR
        except psycopg2.DataError as e:
            log(e)
            self.conn.rollback()
            return DB_DATA_ERROR
        except KeyboardInterrupt:
            return DB_INTERRUPTED
        except psycopg2.Error as e:
            # Some disconnect cases seem to end up here :/
            if "server closed the connection unexpectedly" in str(e):
                log("Database appears to have disappeared, fell through to catch-all error case -- reconnecting")
                self.reconnect()
                return DB_OPERATIONAL_ERROR

            log(e.pgerror)
            try:
                self.conn.rollback()
            except InterfaceError as e:
                log(e)
            return DB_GENERIC_ERROR
    
        return DB_NO_ERROR

    def closecursor(self):
        if self.cursor is None:
            return DB_NO_ERROR

        if self.conn is None:
            self.cursor = None
            return DB_NO_ERROR

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

        self.cursor = None    
        return err


    def commit(self):
        if self.conn is None:
            return DB_NO_CURSOR

        try:
            self.conn.commit()
        except psycopg2.extensions.QueryCanceledError:
            self.conn.rollback()
            return DB_QUERY_TIMEOUT        
        except psycopg2.OperationalError:
            log("Database appears to have disappeared during commit -- reconnecting")
            self.reconnect()
            return DB_OPERATIONAL_ERROR
        except psycopg2.ProgrammingError as e:
            log(e)
            return DB_CODING_ERROR
        except psycopg2.IntegrityError as e:
            log(e)
            return DB_DATA_ERROR
        except psycopg2.DataError as e:
            log(e)
            return DB_DATA_ERROR
        except KeyboardInterrupt:
            return DB_INTERRUPTED
        except psycopg2.Error as e:
            # Some disconnect cases seem to end up here :/
            if "server closed the connection unexpectedly" in e.str():
                log("Database appears to have disappeared, fell through to catch-all error case -- reconnecting")
                self.reconnect()
                return DB_OPERATIONAL_ERROR
            log(e.pgerror)
            return DB_GENERIC_ERROR
    
        return DB_NO_ERROR
            

class DatabaseCore(object):
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost=None, \
            new=False, debug=False, timeout=0, cachetime=0):

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
        self.basic.commit()
        self.basic.destroy()

    def _basicquery(self, query, params=None):
        while 1:
            err = self.basic.executequery(query, params)
            if err == DB_OPERATIONAL_ERROR:
                # Retry the query, as we just reconnected
                continue

            err = self.basic.commit()
            break

        return err
    
    def _releasebasic(self):
        return self.basic.closecursor()

    def list_collections(self):
        collections = []

        err = self._basicquery("SELECT * from collections")
        if err == DB_NO_CURSOR:
            return []

        if err != DB_NO_ERROR:
            log("Failed to query for all collections")
            raise DBQueryException(err)

        while True:
            row = self.basic.cursor.fetchone()

            if row == None:
                break
            col = {}
            for k, v in row.items():
                col[k] = v
            collections.append(col)
        err = self._releasebasic()

        if err != DB_NO_ERROR:
            log("Error while tidying up after collections query")
            raise DBQueryException(err)
        return collections

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

        err = self._basicquery("SELECT * from collections where module=%s", 
                    (mod,))
        if err != DB_NO_ERROR:
            log("Failed to query collections that belong to parent module %s" \
                    % (mod))
            raise DBQueryException(err)

        streamtables = {}

        cols = self.basic.cursor.fetchall()
        for c in cols:
            streamtables[c["id"]] = (c["streamtable"], c["modsubtype"])

        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after querying collections for module %s" \
                    % (mod))
            raise DBQueryException(err)
            

        streams = []
        for cid, (tname, sub) in streamtables.items():
            sql = """ SELECT * FROM %s """ % (tname)
            
            err = self._basicquery(sql, (cid,))
            if err != DB_NO_ERROR:
                log("Failed to query streams that belong to module %s-%s" \
                        % (mod, sub))
                raise DBQueryException(err)

            while True:
                row = self.basic.cursor.fetchone()
                if row == None:
                    break
                row_dict = {"modsubtype":sub}
                for k, v in row.items():
                    if k == "id":
                        continue
                    row_dict[k] = v
                streams.append(row_dict)
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after querying streams for module %s" \
                    % (mod))
            raise DBQueryException(err)
        return streams
    
    


class DBInsert(DatabaseCore):
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost=None, \
            new=False, debug=False, cachetime=0):

        super(DBInsert, self).__init__(dbname, dbuser, dbpass, dbhost, \
                new, debug, cachetime=cachetime)

        self.streams = NNTSCCursor(self.connstr, False, None)
        self.data = NNTSCCursor(self.connstr, False, None)

    def connect_db(self, retrywait):
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
        err = self.streams.executequery(query, params)
        return err
    
    def _dataquery(self, query, params=None):
        err = self.data.executequery(query, params)
        return err

    def commit_streams(self):
        err = self.streams.commit()

        if err == DB_NO_ERROR:
            err = self.streams.closecursor()

        return err

    def commit_data(self):
        err = self.data.commit()
        if err == DB_NO_ERROR:
            err = self.data.closecursor()
        return err

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
       
        err = self._basicquery(mostfunc)
        if err != DB_NO_ERROR:
            return err
 
        smokefunc = """
            CREATE OR REPLACE FUNCTION _final_smoke(anyarray)
                RETURNS anyarray AS
            $BODY$
                SELECT array_agg(avg) FROM (
                    SELECT avg(foo), ntile FROM (
                        SELECT foo, ntile(20) OVER (PARTITION BY one ORDER BY foo) FROM (
                            SELECT 1 as one, unnest($1) as foo
                        ) as a WHERE foo IS NOT NULL
                    ) as b GROUP BY ntile ORDER BY ntile
                ) as c;
            $BODY$
                LANGUAGE 'sql' IMMUTABLE;"""
        
        err = self._basicquery(smokefunc)
        if err != DB_NO_ERROR:
            return err
        
        # we can't check IF EXISTS or use CREATE OR REPLACE, so just query it
        err = self._basicquery("""SELECT * from pg_proc WHERE proname='most';""")
        if err != DB_NO_ERROR:
            return err

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
            err = self._basicquery(aggfunc)
            if err != DB_NO_ERROR:
                return err

        err = self._basicquery(        
                """SELECT * from pg_proc WHERE proname='smokearray';""")
        if err != DB_NO_ERROR:
            return err

        if self.basic.cursor.rowcount == 0:
            aggfunc = """
                CREATE AGGREGATE smokearray(anyarray) (
                SFUNC=array_cat,
                STYPE=anyarray,
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );"""
            err = self._basicquery(aggfunc)
            if err != DB_NO_ERROR:
                return err
        
        err = self._basicquery(
                """SELECT * from pg_proc WHERE proname='smoke';""")
        if err != DB_NO_ERROR:
            return err
        
        if self.basic.cursor.rowcount == 0:
            aggfunc = """
                CREATE AGGREGATE smoke(numeric) (
                SFUNC=array_append,
                STYPE=numeric[],
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );"""
            err = self._basicquery(aggfunc)
            if err != DB_NO_ERROR:
                return err
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after creating aggregate functions")
            return err
        
        return DB_NO_ERROR

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

        err = self._basicquery(indexcheck, (name,))
        if err != DB_NO_ERROR:
            return err
        
        if self.basic.cursor.rowcount != 0:
            # Already exists, no need to create
            return DB_NO_ERROR

        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after checking for index existence")
            return err
        
        index = "CREATE INDEX %s ON %s" 
        index += "(%s)" % (",".join(["%s"] * len(columns)))

        params = tuple([name] + [table] + columns)
        
        # Annoying that I can't parameterise this SQL nicely
        err = self._streamsquery(index % params)
        return err        
 

    def build_databases(self, modules, new=False):
        if new:
            err = self.__delete_everything()
            if err != DB_NO_ERROR:
                return err

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
                );

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
        err = self._basicquery(seqfunc)       
        if err != DB_NO_ERROR:
            return err

        coltable = """CREATE TABLE IF NOT EXISTS collections (
                id SERIAL PRIMARY KEY,
                module varchar NOT NULL,
                modsubtype character varying,
                streamtable character varying NOT NULL,
                datatable character varying NOT NULL,
                UNIQUE (module, modsubtype))"""
        
        err = self._basicquery(coltable)       
        if err != DB_NO_ERROR:
            return err

        createseq = """SELECT create_seq('streams_id_seq');"""
        err = self._basicquery(createseq)       
        if err != DB_NO_ERROR:
            return err

        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after creating core tables")
            return err


        for base,mod in modules.items():
            mod.tables(self)

        return DB_NO_ERROR

    def register_collection(self, mod, subtype, stable, dtable):
       
        colcheck = """SELECT * FROM collections WHERE module=%s 
                    AND modsubtype=%s"""
       
        err = self._basicquery(colcheck, (mod, subtype))
        if err != DB_NO_ERROR:
            return err

        if self.basic.cursor.rowcount > 0:
            return DB_NO_ERROR

        insert = """INSERT INTO collections (module, modsubtype, 
                streamtable, datatable) VALUES (%s, %s, %s, %s)"""

        err = self._basicquery(insert, (mod, subtype, stable, dtable))
        if err != DB_NO_ERROR:
            return err

        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after registering collection")
            return err
        return DB_NO_ERROR

    def register_new_stream(self, mod, subtype, name, ts, basedata):

        # Find the appropriate collection id
        colcheck = """SELECT * FROM collections WHERE module=%s 
                    AND modsubtype=%s"""
                
        err = self._basicquery(colcheck, (mod, subtype))
        if err != DB_NO_ERROR:
            return err, -1

        if self.basic.cursor.rowcount == 0:
            log("Database Error: no collection for %s:%s" % (mod, subtype))
            return DB_DATA_ERROR, -1

        if self.basic.cursor.rowcount > 1:
            log("Database Error: duplicate collections for %s:%s" % (mod, subtype))
            return DB_DATA_ERROR, -1

        col = self.basic.cursor.fetchone()
        col_id = col['id']
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after finding collection for new stream")
            return err

        insert = """INSERT INTO streams (collection, name, lasttimestamp,
                    firsttimestamp) VALUES (%s, %s, %s, %s) RETURNING id
                """
        err = self._streamsquery(insert, (col_id, name, 0, ts))
        
        if err == DB_DUPLICATE_KEY:
            log("Attempted to register duplicate stream for %s:%s, name was %s" % (mod, subtype, name))

        if err != DB_NO_ERROR:
            return err, -1


        # Return the new stream id
        newid = self.streams.cursor.fetchone()[0]

        if ts != 0:        
            self.streamcache.store_firstts(newid, ts)

        return col_id, newid

    def clone_table(self, original, streamid, foreignkey=None):
        tablename = original + "_" + str(streamid)

        # LIKE can't copy foreign keys so we have to explicitly add the
        # one we really want
        query = "CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING INDEXES INCLUDING CONSTRAINTS" % (tablename, original)
        if foreignkey is not None:
            query += ", %s)" % (foreignkey)
        else:
            query += ")"

        err = self._streamsquery(query)
        
        if err != DB_NO_ERROR:
            log("Failed to clone table %s for new stream %s" % \
                    (original, str(streamid)))
            return err

        return DB_NO_ERROR

    def add_foreign_key(self, tablename, column, foreigntable, foreigncolumn):
        # XXX Only supports one column foreign keys for now

        # TODO, validate these table names and columns to make sure they
        # exist
        query = "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE" % (tablename, column, foreigntable, foreigncolumn)

        err = self._streamsquery(query)
        
        if err != DB_NO_ERROR:
            log("Failed to add foreign key to table %s" % \
                    (tablename))
            return err

        return DB_NO_ERROR

    def __delete_everything(self):

        err = self._basicquery("""SELECT table_schema, table_name FROM
                    information_schema.tables WHERE table_schema='public'
                    ORDER BY table_schema, table_name""")
        if err != DB_NO_ERROR:
            return err
       
        # XXX This may use a lot of memory :0
        rows = self.basic.cursor.fetchall()
        for r in rows:
            err = self._basicquery("""DROP TABLE IF EXISTS %s CASCADE""" % (r['table_name'],))
            if err != DB_NO_ERROR:
                return err            
       
        err = self._basicquery("""DROP SEQUENCE IF EXISTS "streams_id_seq" """)

        if err != DB_NO_ERROR:
            return err
 
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after dropping all tables")
            return err

        return DB_NO_ERROR
 
    def update_timestamp(self, datatable, stream_ids, lasttimestamp):
        
        for sid in stream_ids:
            self.streamcache.store_timestamps(datatable, sid, lasttimestamp)
            #log("Updated timestamp for %d to %d" % (sid, lasttimestamp))
        
        return DB_NO_ERROR

    def get_last_timestamp(self, table, streamid):
        firstts, lastts = self.streamcache.fetch_timestamps(table, streamid)

        if lastts is None:
            # Nothing useful in cache, query data table for max timestamp
            # Warning, this isn't going to be fast so try to avoid doing
            # this wherever possible!
            query = "SELECT max(timestamp) FROM %s_" % (table)
            query += "%s"   # stream id goes in here

            err = self._basicquery(query, (streamid,))
            if err != DB_NO_ERROR:
                return err, 0

            if self.basic.cursor.rowcount != 1:
                log("Unexpected number of results when querying for max timestamp: %d" % (self.basic.cursor.rowcount))
                return DB_CODING_ERROR, []

            row = self.basic.cursor.fetchone()
            if row[0] == None:
                return DB_NO_ERROR, 0

            lastts = int(row[0])
            self.streamcache.store_timestamps(table, streamid, lastts)
            
            err = self._releasebasic()
            if err != DB_NO_ERROR:
                return err, 0
    
        return DB_NO_ERROR, lastts


    def find_existing_stream(self, st, props):
        # We know a stream already exists that matches the given properties
        # so we just need to find it

        wherecl = "WHERE "
        params = []
        keys = props.keys()
        for i in range(0, len(keys)):
            if i != 0:
                wherecl += " AND "
            # XXX NOT PARAMETERISED
            wherecl += "%s = %s" 
            params += [keys[i], props[keys[i]]]

        query = "SELECT stream_id FROM %s " % (st)
        query += wherecl

        err = self._basicquery(query, tuple(params))
        if err != DB_NO_ERROR:
            return err

        if self.basic.cursor.rowcount != 1:
            log("Unexpected number of matches when searching for existing stream: %d" % (self.basic.cursor.rowcount))
            return DB_CODING_ERROR

        row = self.basic.cursor.fetchone()
        
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after finding existing stream")
            return err
        return row[0]


    def insert_stream(self, tablename, datatable, timestamp, streamprops):

        colid = 0

        # insert stream into our stream table
        colstr = "(stream_id"
        values = []
        for k,v in streamprops.iteritems():
            colstr += ', "%s"' % (k)
            values.append(v)    
        colstr += ") "
        
        params = tuple(values)
        insert = "INSERT INTO %s " % (tablename)
        insert += colstr
        insert += "VALUES (nextval('streams_id_seq'), %s)" % \
                (",".join(["%s"] * len(values)))
        insert += " RETURNING stream_id"
        err = self._streamsquery(insert, params)
        
        if err == DB_DUPLICATE_KEY:
            return colid, self.find_existing_stream(tablename, streamprops)
        elif err != DB_NO_ERROR:
            return colid, err

        # Grab the new stream ID so we can return it
        newid = self.streams.cursor.fetchone()[0]

        # Cache the observed timestamp as the first timestamp
        if timestamp != 0:        
            self.streamcache.store_timestamps(datatable, newid, timestamp, 
                    timestamp)

 
        # Create a new data table for this stream, using the "base" data
        # table as a template
        
        err = self.clone_table(datatable, newid)
        if err != DB_NO_ERROR:
            return colid, err       
 
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
        return colid, newid

    def custom_insert(self, customsql, values):
        err = self._dataquery(customsql, values)
        if err != DB_NO_ERROR:
            return err, None

        result = self.data.cursor.fetchone()
        return DB_NO_ERROR, result     



    def insert_data(self, tablename, collection, stream, ts, result,
            casts = {}):
   
        colstr = "(stream_id, timestamp"
        valstr = "%s, %s"
        values = [stream, ts]
        
        for k,v in result.iteritems():
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

        err = self._dataquery(insert, params)
        return err
    
    def _columns_sql(self, columns):

        basesql = ""
        
        for c in columns:
            if "name" not in c:
                log("Unnamed column when creating streams table %s -- skipping" % (name))
                continue

            if "type" not in c:
                log("Column %s has no type when creating streams table %s -- skipping" % (c["name"], name))
                continue

            basesql += ', "%s" %s ' % (c["name"], c["type"])

            if "null" in c and c["null"] == False:
                basesql += "NOT NULL "

            if "default" in c:
                basesql += "DEFAULT %s " % (c["default"])
            
            if "unique" in c and c["unique"] == True:
                basesql += "UNIQUE "

        return basesql

    def create_misc_table(self, name, columns, uniquecols=[]):
        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)
        coltext = self._columns_sql(columns)

        # Remove the leading comma
        basesql += coltext[1:]    
    
        if len(uniquecols) != 0:
            basesql += ", UNIQUE ("
    
            for i in range(0, len(uniquecols)):
                if i != 0:
                    basesql += ", "
                basesql += "%s" % (uniquecols[i])

            basesql += ")"
        basesql += ")"

        err = self._basicquery(basesql)
        if err != DB_NO_ERROR:
            log("Failed to create miscellaneous table %s" % (name))
            return err

        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after creating miscellaneous table")
            return err
        return DB_NO_ERROR

    def create_data_table(self, name, columns, indexes=[]):
 
        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)

        basesql += "stream_id integer NOT NULL, timestamp integer NOT NULL" 

        basesql += self._columns_sql(columns)
        basesql += ")"

        err = self._basicquery(basesql)
        if err != DB_NO_ERROR:
            log("Failed to create data table %s" % (name))
            return err
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after creating data table")
            return err
      
        err = self.create_index("%s_%s_idx" % (name, "timestamp"), name, \
                    ["timestamp"])
        if err != DB_NO_ERROR:
            log("Failed to create index while creating data table %s" % ( name))
            return err

 
        for ind in indexes:
            if "columns" not in ind or len(ind["columns"]) == 0:
                log("Index %s for data table %s has no columns -- skipping" \
                        % (indname, name))
                continue
            
            if "name" not in ind:
                indname = ""
            else:
                indname = ind["name"]
    
            err = self.create_index(indname, name, ind["columns"])
            if err != DB_NO_ERROR:
                log("Failed to create index while creating data table %s" % ( name))
                return err
        
        return DB_NO_ERROR

    def create_streams_table(self, name, columns, uniquecols=[]):

        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)

        basesql += "stream_id integer PRIMARY KEY DEFAULT nextval('streams_id_seq')"

        basesql += self._columns_sql(columns)

        if len(uniquecols) != 0:
            basesql += ", UNIQUE ("
    
            for i in range(0, len(uniquecols)):
                if i != 0:
                    basesql += ", "
                basesql += '"%s"' % (uniquecols[i])

            basesql += ")"
        basesql += ")"

        err = self._basicquery(basesql)
        if err != DB_NO_ERROR:
            log("Failed to create stream table %s" % (name))
            return err
        err = self._releasebasic()
        if err != DB_NO_ERROR:
            log("Failed to tidy up after creating streams table")
            return err

        return DB_NO_ERROR
    

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
