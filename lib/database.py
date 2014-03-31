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
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError,\
        ProgrammingError, DataError, InvalidRequestError, InterfaceError, \
        InternalError
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

DB_NO_ERROR = 0
DB_DATA_ERROR = -1
DB_GENERIC_ERROR = -2
DB_INTERRUPTED = -3
DB_OPERATIONAL_ERROR = -4

class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

class DropView(DDLElement):
    def __init__(self, name):
        self.name = name

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

        self.connect_string = URL('postgresql', username=dbuser, \
                password=dbpass, host=dbhost, database=dbname)

        self.dbname = dbname
        self.conn = None
        self.engine = None
        self.trans = None
        self.init_error = False

    def connect_db(self):

        if self.trans is not None:
            self.commit_transaction() 

        if self.conn is not None:
            self.conn.close()

        self.init_error = False
        
        self.engine = create_engine(self.connect_string, echo=False,
                implicit_returning = False)

        while self.__reflect_db() == -1:
           time.sleep(5) 

        self.conn = self.engine.connect()

        self.trans = self.conn.begin()
        return 0

    def __reflect_db(self):
        self.metadata = MetaData(self.engine)
        try:
            self.metadata.reflect(bind=self.engine)
        except OperationalError as e:
            if "does not exist" in e.args[0]:
                log("Error binding to database %s" % (self.dbname))
                log("Are you sure you've specified the right database name?")
                sys.exit(1)
            else:
                log(e) 
            return -1

        # reflect() is supposed to take a 'views' argument which will
        # force it to reflects views as well as tables, but our version of
        # sqlalchemy didn't like that. So fuck it, I'll just reflect the
        # views manually
        inspector = reflection.Inspector.from_engine(self.engine)
        views = inspector.get_view_names()
        for v in views:
            view_table = Table(v, self.metadata, autoload=True)

        return 0

    def reconnect(self):
        time.sleep(5)
        self.connect_db()

    def __del__(self):
        if self.conn is not None:
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

    def create_aggregators(self):
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

        smokefunc = text("""
            CREATE OR REPLACE FUNCTION _final_smoke(anyarray)
                RETURNS anyarray AS
            $BODY$
                SELECT array_agg(avg) FROM (
                    SELECT avg(foo), ntile FROM (
                        SELECT foo, ntile(20) OVER (PARTITION BY one ORDER BY foo) FROM (
                            SELECT 1 as one, unnest($1) as foo EXCEPT ALL SELECT 1,NULL
                        ) as a
                    ) as b GROUP BY ntile ORDER BY ntile
                ) as c;
            $BODY$
                LANGUAGE 'sql' IMMUTABLE;""")
        self.conn.execute(smokefunc)
        
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

        mostcount.close()
        smokearraycount = self.conn.execute(
                """SELECT * from pg_proc WHERE proname='smokearray';""")

        if smokearraycount.rowcount == 0:
            aggfunc = text("""
                CREATE AGGREGATE smokearray(anyarray) (
                SFUNC=array_cat,
                STYPE=anyarray,
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );""")
            self.conn.execute(aggfunc)

        smokearraycount.close()
        smokecount = self.conn.execute(
                """SELECT * from pg_proc WHERE proname='smoke';""")

        if smokecount.rowcount == 0:
            aggfunc = text("""
                CREATE AGGREGATE smoke(numeric) (
                SFUNC=array_append,
                STYPE=numeric[],
                FINALFUNC=_final_smoke,
                INITCOND='{}'
            );""")
            self.conn.execute(aggfunc)
        
        smokecount.close()
        self.commit_transaction()

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
            Index('index_streams_first', streams.c.firsttimestamp)
            Index('index_streams_last', streams.c.lasttimestamp)

        self.create_aggregators()

        for base, mod in modules.items():
            mod.tables(self)

        self.metadata.create_all()
        self.commit_transaction()

    def register_collection(self, mod, subtype, stable, dtable):
        table = self.metadata.tables['collections']

        # TODO Maybe check if the collection exists first -- this is
        # mostly just so we don't waste collection ids, which tends to
        # bug me for some reason

        try:
            self.conn.execute(table.insert(), module=mod, modsubtype=subtype,
                    streamtable=stable, datatable=dtable)
        except IntegrityError, e:
            self.rollback_transaction()
            log("Failed to register collection for %s:%s, probably already exists" % (mod, subtype))
            #print >> sys.stderr, e
            return -1

        self.commit_transaction()

    def register_new_stream(self, mod, subtype, name, ts, basedata):

        # Find the appropriate collection id
        coltable = self.metadata.tables['collections']

        sql = coltable.select().where(and_(coltable.c.module==mod,
                coltable.c.modsubtype==subtype))
        result = sql.execute()

        if result.rowcount == 0:
            log("Database Error: no collection for %s:%s" % (mod, subtype))
            return DB_DATA_ERROR, -1

        if result.rowcount > 1:
            log("Database Error: duplicate collections for %s:%s" % (mod, subtype))
            return DB_DATA_ERROR, -1

        col = result.fetchone()
        col_id = col['id']
        result.close()

        # Insert entry into the stream table
        sttable = self.metadata.tables['streams']

        while 1:

            try:
                result = self.conn.execute(sttable.insert(), collection=col_id,
                        name=name, lasttimestamp=0, firsttimestamp=ts)
            except (DataError, ProgrammingError, IntegrityError) as e:
                log(e)
                log("Failed to register stream %s for %s:%s, probably already exists" % (name, mod, subtype))
                #print >> sys.stderr, e
                return DB_DATA_ERROR, -1
            except OperationalError as e:
                log("Database became unavailable while registering stream")
                self.reconnect()
                continue
            except SQLAlchemyError as e:
                log(e)
                log("Failed to register stream %s for %s:%s due to SQLAlchemy error" % (name, mod, subtype))
                return DB_GENERIC_ERROR, -1

            break

        # Return the new stream id
        newid = result.inserted_primary_key
        result.close()

        self.commit_transaction()
        # Create a new data table for this stream, using the "base" data
        # table as a template
        fkey = "FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE"
        ret = self.clone_table(basedata, newid[0], fkey)
        if ret != DB_NO_ERROR:
            return ret, -1

        return col_id, newid[0]

    def clone_table(self, original, streamid, foreignkey=None):
        tablename = original + "_" + str(streamid)

        while 1:
            # LIKE can't copy foreign keys so we have to explicitly add the
            # one we really want
            query = "CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING INDEXES INCLUDING CONSTRAINTS" % (tablename, original)
            if foreignkey is not None:
                query += ", %s)" % (foreignkey)
            else:
                query += ")"

            try:
                result = self.conn.execute(query)
            except OperationalError as e:
                log("Database became unavailable while cloning table")
                self.reconnect()
                continue
            except (DataError, ProgrammingError, IntegrityError) as e:
                log(e)
                log("Failed to clone table %s for new stream %s" % \
                        (original, str(streamid)))
                return DB_DATA_ERROR
            except SQLAlchemyError as e:
                #if "TransactionRollbackError" in str(e):
                #    log("Retrying clone table")
                #    continue
                log(e)
                log("Failed to clone table %s for new stream %s" % \
                        (original, str(streamid)))
                return DB_GENERIC_ERROR
            break

        self.commit_transaction()

        try:
            self.metadata.reflect(bind=self.engine)
        except OperationalError as e:
            log("Database became unavailable while binding new data table")
            self.reconnect()
        
        return DB_NO_ERROR

    def add_foreign_key(self, tablename, column, foreigntable, foreigncolumn):
        # XXX Only supports one column foreign keys for now

        while 1:
            # TODO, validate these table names and columns to make sure they
            # exist
            query = "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE" % (tablename, column, foreigntable, foreigncolumn)

            try:
                result = self.conn.execute(query)
            except OperationalError as e:
                log("Database became unavailable while adding foreign key")
                self.reconnect()
                continue
            except (DataError, ProgrammingError, IntegrityError) as e:
                log(e)
                log("Failed to add foreign key to table %s" % \
                        (tablename))
                return DB_DATA_ERROR
            except SQLAlchemyError as e:
                log(e)
                log("Failed to add foreign key to table %s" % \
                        (tablename))
                return DB_GENERIC_ERROR
            break

        self.commit_transaction()
            
        return DB_NO_ERROR

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
                    ForeignKeyConstraint((), (), name=fk['name'])
                    )
            t = Table(table_name, newmeta, *fks)
            if table_name[0:5] == "part_":
                partitions.append(t)
            else:
                tbs.append(t)
            all_fks.extend(fks)

        for v in inspector.get_view_names():
            self.conn.execute(DropView(v))
            self.commit_transaction()

        for fkc in all_fks:
            self.conn.execute(DropConstraint(fkc))
            self.commit_transaction()

        for table in partitions:
            self.conn.execute(DropTable(table))
            self.commit_transaction()

        for table in tbs:
            self.conn.execute("DROP TABLE %s CASCADE" % (table))
            self.commit_transaction()

        self.commit_transaction()

    def list_collections(self):
        collections = []

        table = self.metadata.tables['collections']

        result = table.select().execute()
        for row in result:

            col = {}
            for k, v in row.items():
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
                for k, v in row.items():
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

        sql = select([colstrtable, streams_t]).select_from(colstrtable.join(streams_t, streams_t.c.id == colstrtable.c.stream_id)).where(colstrtable.c.stream_id > minid)
        result = sql.execute()

        for row in result:
            stream_dict = {}
            for k, v in row.items():
                if k == "id":
                    continue
                stream_dict[k] = v
            selected.append(stream_dict)
        result.close()
        return selected

    def commit_transaction(self):
        # TODO: Better error handling!
        if self.trans is None:
            return

        try:
            self.trans.commit()
        except InvalidRequestError as e:
            self.trans.rollback()
            self.trans = None
            return
        except:
            raise
        self.trans = self.conn.begin()

    def rollback_transaction(self):
        try:
            self.trans.rollback()
            self.trans = self.conn.begin()
        except (OperationalError, InterfaceError) as e:
            # DB has gone away, can't rollback
            self.trans = None
            return

    def update_timestamp(self, stream_ids, lasttimestamp):
        if len(stream_ids) == 0:
            return DB_NO_ERROR
        sql = "UPDATE streams SET lasttimestamp=%s "
        sql += "WHERE id IN (%s)" % (",".join(["%s"] * len(stream_ids)))

        while 1:    
            try:
                self.conn.execute(sql, tuple([lasttimestamp] + stream_ids))
            except OperationalError as e:
                self.reconnect()
                continue
            except (IntegrityError, DataError, ProgrammingError) as e:
                self.rollback_transaction()
                log(e)
                return DB_DATA_ERROR
            except KeyboardInterrupt as e:
                self.rollback_transaction()
                return DB_INTERRUPTED
            except SQLAlchemyError as e:
                self.rollback_transaction()
                log(e)
                return DB_GENERIC_ERROR

            return DB_NO_ERROR

    def set_firsttimestamp(self, stream_id, ts):
        table = self.metadata.tables['streams']
        
        while 1:
            try:
                result = self.conn.execute(table.update().where( \
                    table.c.id==stream_id).values( \
                    firsttimestamp=ts))
                break
            except OperationalError as e:
                self.reconnect()
                continue
            except (IntegrityError, DataError, ProgrammingError) as e:
                self.rollback_transaction()
                log(e)
                return DB_DATA_ERROR
            except KeyboardInterrupt as e:
                self.rollback_transaction()
                return DB_INTERRUPTED
            except SQLAlchemyError as e:
                self.rollback_transaction()
                log(e)
                return DB_GENERIC_ERROR

        result.close()
        return DB_NO_ERROR

    def insert_stream(self, liveexp, tablename, datatable, basecol, submodule, 
            name, timestamp, streamprops):

        colid, streamid = self.register_new_stream(basecol, submodule, 
                name, timestamp, datatable)

        if colid < 0:
            return colid

        # insert stream into our stream table
        st = self.metadata.tables[tablename]

        while 1:

            try:
                result = self.conn.execute(st.insert(), stream_id=streamid,
                    **streamprops)
            except (IntegrityError, DataError, ProgrammingError) as e:
                self.rollback_transaction()
                log(e)
                return DB_DATA_ERROR
            except OperationalError as e:
                # Don't rollback as the database has probably gone away
                log("Operational Error while inserting stream")
                self.reconnect()
                continue
            except SQLAlchemyError as e:
                self.rollback_transaction()
                log(e)
                return DB_GENERIC_ERROR
            except KeyboardInterrupt as e:
                self.rollback_transaction()
                return DB_INTERRUPTED
            break

        if liveexp != None and streamid > 0:
            streamprops["name"] = name
            liveexp.publishStream(colid, basecol + "_" + submodule,
                    streamid, streamprops)

        return streamid

    def custom_insert(self, customsql, values):
        while 1:
            try:
                result = self.conn.execute(customsql, values)
            except (DataError, IntegrityError, ProgrammingError) as e:
                self.rollback_transaction()
                log(e)
                return DB_DATA_ERROR, None
            except OperationalError as e:
                # Don't rollback as the database has probably gone away
                log("Operational Error while inserting data")
                self.reconnect()
                continue
            except SQLAlchemyError as e:
                self.rollback_transaction()
                log(e)
                return DB_GENERIC_ERROR, None
            except KeyboardInterrupt as e:
                self.rollback_transaction()
                return DB_INTERRUPTED, None

            break

        return DB_NO_ERROR, result


    def insert_data(self, liveexp, tablename, collection, stream, ts, result,
            insertfunc=None):
   
        # insertfunc allows callers to provide custom SQL to insert their
        # data -- required if casts are required (e.g. amp-traceroute)
    
        # If no insertfunc is provided, use the generic sqlalchemy insert 
        if insertfunc == None:
            dt = self.metadata.tables[tablename + "_" + str(stream)]
            insertfunc = dt.insert()

        while 1:

            try:
                self.conn.execute(insertfunc, stream_id=stream, timestamp=ts,
                        **result)
            except (DataError, IntegrityError, ProgrammingError) as e:
                self.rollback_transaction()
                log(e)
                return DB_DATA_ERROR
            except OperationalError as e:
                # Don't rollback as the database has probably gone away
                log("Operational Error while inserting data")
                self.reconnect()
                continue
            except SQLAlchemyError as e:
                self.rollback_transaction()
                log(e)
                return DB_GENERIC_ERROR
            except KeyboardInterrupt as e:
                self.rollback_transaction()
                return DB_INTERRUPTED

            break

        if liveexp != None:
            liveexp.publishLiveData(collection, stream, ts, result)

        return DB_NO_ERROR
     

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
