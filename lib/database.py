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

class Database:
    def __init__(self, dbname, dbuser=None, dbpass=None, dbhost=None, \
            new=False, debug=False):

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

        self.conn = None
        self.connstr = connstr

        self.basiccursor = None

    def connect_db(self, retrywait):
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

        if logmessage:
            log("Successfully connected to NNTSC database")

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

    def disconnect(self):
        if self.basiccursor is not None:
            self.basiccursor.close()
            self.basiccursor = None
        
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def reconnect(self):
        time.sleep(5)
        self.disconnect()
        self.connect_db(5)

    def __del__(self):

        if self.conn is not None:
            self.conn.commit()
        self.disconnect()

    def _basicquery(self, query, params=None):
        while 1:
            if self.basiccursor is None:
                log("Cannot execute query with a None cursor!")
                log("%s" % (query))
                return DB_NO_CURSOR
     
            try:
                if params is not None:
                    self.basiccursor.execute(query, params)
                else:
                    self.basiccursor.execute(query)

            except psycopg2.extensions.QueryCanceledError:
                self.conn.rollback()
                return DB_QUERY_TIMEOUT        
            except psycopg2.OperationalError:
                log("Database appears to have disappeared -- reconnecting")
                self.reconnect()
                continue
            except psycopg2.ProgrammingError as e:
                log(e)
                self.conn.rollback()
                return DB_CODING_ERROR
            except psycopg2.IntegrityError as e:
                self.conn.rollback()
                log(e)
                if " duplicate " in str(e):
                    return DB_DUPLICATE_KEY
                return DB_DATA_ERROR
            except psycopg2.DataError as e:
                self.conn.rollback()
                log(e)
                return DB_DATA_ERROR
            except KeyboardInterrupt:
                return DB_INTERRUPTED
            except psycopg2.Error as e:
                self.conn.rollback()
                log(e)
                return DB_GENERIC_ERROR
            
            break

        return DB_NO_ERROR

    def commit_transaction(self):
        self.conn.commit()

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

        assert(self.basiccursor.rowcount <= 1)

        # if it doesn't exist, create the aggregate function that applies
        # _final_most to multiple rows of data
        if self.basiccursor.rowcount == 0:
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

        if self.basiccursor.rowcount == 0:
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
        
        if self.basiccursor.rowcount == 0:
            aggfunc = text("""
                CREATE AGGREGATE smoke(numeric) (
                SFUNC=array_append,
                STYPE=numeric[],
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );""")
            err = self._basicquery(aggfunc)
            if err != DB_NO_ERROR:
                return err
        
        self.conn.commit()        
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
        
        if self.basiccursor.rowcount != 0:
            # Already exists, no need to create
            return DB_NO_ERROR

        
        index = "CREATE INDEX %s ON %s" 
        index += "(%s)" % (",".join(["%s"] * len(columns)))

        params = tuple([name] + [table] + columns)
        
        # Annoying that I can't parameterise this SQL nicely
        err = self._basicquery(index % params)
        return err        
 

    def build_databases(self, modules, new=False):
        if new:
            err = self.__delete_everything()
            if err != DB_NO_ERROR:
                return err

        self.create_aggregators()
        
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
        
        streamtable = """CREATE TABLE IF NOT EXISTS streams (
                id SERIAL PRIMARY KEY,
                collection integer NOT NULL 
                    REFERENCES collections (id) ON DELETE CASCADE,
                name character varying NOT NULL,
                lasttimestamp integer NOT NULL,
                firsttimestamp integer)"""

        err = self._basicquery(streamtable)
        if err != DB_NO_ERROR:
            return err
       
        err = self.create_index("index_streams_collection", "streams", 
                    ["collection"])
        if err != DB_NO_ERROR:
            return err

        err = self.create_index("index_streams_first", "streams", 
                    ["firsttimestamp"])
        if err != DB_NO_ERROR:
            return err
        
        err = self.create_index("index_streams_last", "streams", 
                    ["lasttimestamp"])
        if err != DB_NO_ERROR:
            return err

        for base,mod in modules.items():
            mod.tables(self)

        self.conn.commit()
        return DB_NO_ERROR

    def register_collection(self, mod, subtype, stable, dtable):
        
        colcheck = """SELECT * FROM collections WHERE module=%s 
                    AND modsubtype=%s"""
       
        err = self._basicquery(colcheck, (mod, subtype))
        if err != DB_NO_ERROR:
            return err

        if self.basiccursor.rowcount > 0:
            return DB_NO_ERROR

        insert = """INSERT INTO collections (module, modsubtype, 
                streamtable, datatable) VALUES (%s, %s, %s, %s)"""

        err = self._basicquery(insert, (mod, subtype, stable, dtable))
        if err != DB_NO_ERROR:
            return err

        self.conn.commit()
        return DB_NO_ERROR

    def register_new_stream(self, mod, subtype, name, ts, basedata):

        # Find the appropriate collection id
        colcheck = """SELECT * FROM collections WHERE module=%s 
                    AND modsubtype=%s"""
                
        err = self._basicquery(colcheck, (mod, subtype))
        if err != DB_NO_ERROR:
            return err, -1

        if self.basiccursor.rowcount == 0:
            log("Database Error: no collection for %s:%s" % (mod, subtype))
            return DB_DATA_ERROR, -1

        if self.basiccursor.rowcount > 1:
            log("Database Error: duplicate collections for %s:%s" % (mod, subtype))
            return DB_DATA_ERROR, -1

        col = self.basiccursor.fetchone()
        col_id = col['id']

        insert = """INSERT INTO streams (collection, name, lasttimestamp,
                    firsttimestamp) VALUES (%s, %s, %s, %s) RETURNING id
                """
        err = self._basicquery(insert, (col_id, name, 0, ts))
        
        if err == DB_DUPLICATE_KEY:
            log("Attempted to register duplicate stream for %s:%s, name was %s" % (mod, subtype, name))

        if err != DB_NO_ERROR:
            return err, -1

        # Return the new stream id
        newid = self.basiccursor.fetchone()[0]

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

        err = self._basicquery(query)
        
        if err != DB_NO_ERROR:
            log("Failed to clone table %s for new stream %s" % \
                    (original, str(streamid)))
            return err

        #self.conn.commit()
        return DB_NO_ERROR

    def add_foreign_key(self, tablename, column, foreigntable, foreigncolumn):
        # XXX Only supports one column foreign keys for now

        # TODO, validate these table names and columns to make sure they
        # exist
        query = "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE" % (tablename, column, foreigntable, foreigncolumn)

        err = self._basicquery(query)
        
        if err != DB_NO_ERROR:
            log("Failed to add foreign key to table %s" % \
                    (tablename))
            return err

        #self.conn.commit()
        return DB_NO_ERROR

    def __delete_everything(self):

        err = self._basicquery("""SELECT table_schema, table_name FROM
                    information_schema.tables WHERE table_schema='public'
                    ORDER BY table_schema, table_name""")
        if err != DB_NO_ERROR:
            return err
       
        # XXX This may use a lot of memory :0
        rows = self.basiccursor.fetchall()
        for r in rows:
            err = self._basicquery("""DROP TABLE IF EXISTS %s CASCADE""" % (r['table_name'],))
            if err != DB_NO_ERROR:
                return err            

            self.conn.commit()       
        return DB_NO_ERROR
 
    def update_timestamp(self, stream_ids, lasttimestamp):
        if len(stream_ids) == 0:
            return DB_NO_ERROR
        sql = "UPDATE streams SET lasttimestamp=%s "
        sql += "WHERE id IN (%s)" % (",".join(["%s"] * len(stream_ids)))

        err = self._basicquery(sql, tuple([lasttimestamp] + stream_ids))

        if err != DB_NO_ERROR:
            return err        

        #self.conn.commit()
        return DB_NO_ERROR

    def set_firsttimestamp(self, stream_id, ts):
       
        sql = "UPDATE streams SET firsttimestamp=%s WHERE id = %s"
 
        err = self._basicquery(sql, (ts, stream_id))

        if err != DB_NO_ERROR:
            return err        
        self.conn.commit()
        return DB_NO_ERROR

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

        if self.basiccursor.rowcount != 1:
            log("Unexpected number of matches when searching for existing stream: %d" % (self.basiccursor.rowcount))
            return DB_CODING_ERROR

        row = self.basiccursor.fetchone()
        return row[0]


    def insert_stream(self, liveexp, tablename, datatable, basecol, submodule, 
            name, timestamp, streamprops):

        colid, streamid = self.register_new_stream(basecol, submodule, 
                name, timestamp, datatable)
        
        if colid < 0:
            return colid

        # insert stream into our stream table
        colstr = "(stream_id"
        values = [streamid]
        for k,v in streamprops.iteritems():
            colstr += ', "%s"' % (k)
            values.append(v)    
        colstr += ") "
        
        params = tuple(values)
        insert = "INSERT INTO %s " % (tablename)
        insert += colstr
        insert += "VALUES (%s)" % (",".join(["%s"] * len(values)))

        err = self._basicquery(insert, params)
        
        if err == DB_DUPLICATE_KEY:
            return self.find_existing_stream(tablename, streamprops)
        elif err != DB_NO_ERROR:
            return err
 
        # Create a new data table for this stream, using the "base" data
        # table as a template
        
        err = self.clone_table(datatable, streamid)
        if err != DB_NO_ERROR:
            return err       
 
        self.conn.commit()
        if liveexp != None and streamid > 0:
            streamprops["name"] = name
            liveexp.publishStream(colid, basecol + "_" + submodule,
                    streamid, streamprops)

        return streamid

    def custom_insert(self, customsql, values):
        err = self._basicquery(customsql, values)
        if err != DB_NO_ERROR:
            return err, None

        result = self.basiccursor.fetchone()
        return DB_NO_ERROR, result     



    def insert_data(self, liveexp, tablename, collection, stream, ts, result,
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

        err = self._basicquery(insert, params)
        if err != DB_NO_ERROR:
            return err

        if liveexp != None:
            liveexp.publishLiveData(collection, stream, ts, result)

        return DB_NO_ERROR
    
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

        self.commit_transaction()
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
        
        self.commit_transaction()
        return DB_NO_ERROR

    def create_streams_table(self, name, columns, uniquecols=[]):

        basesql = "CREATE TABLE IF NOT EXISTS %s (" % (name)

        basesql += "stream_id integer PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE"

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

        self.commit_transaction()
        return DB_NO_ERROR
    
    def list_collections(self):
        collections = []

        err = self._basicquery("SELECT * from collections")
        if err == DB_NO_CURSOR:
            return []

        if err != DB_NO_ERROR:
            return err

        while True:
            row = self.basiccursor.fetchone()

            if row == None:
                break
            col = {}
            for k, v in row.items():
                col[k] = v
            collections.append(col)
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
            return err, []

        streamtables = {}

        cols = self.basiccursor.fetchall()
        for c in cols:
            streamtables[c["id"]] = (c["streamtable"], c["modsubtype"])

        streams = []
        for cid, (tname, sub) in streamtables.items():
            sql = """ SELECT * FROM streams, %s WHERE streams.collection=%s
                      AND streams.id = %s.stream_id """ % (tname, "%s", tname)
            
            err = self._basicquery(sql, (cid,))
            if err != DB_NO_ERROR:
                return err, []

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
        return DB_NO_ERROR, streams
    

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
