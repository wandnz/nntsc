from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint, event, DDL
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

from libnntsc.logger import *

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
        self.engine = create_engine(connect_string, echo=debug)

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
                Column('name', String, nullable=False, unique=True),
                Column('lasttimestamp', Integer, nullable=False),
            )
      
            streams.create()
       
        #self.metadata.create_all()
        #self.commit_transaction()

        #print self.meta.tables.keys()

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

    def register_new_stream(self, mod, subtype, name):

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
                    name=name, lasttimestamp=0)
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
            tbs.append(t)
            all_fks.extend(fks)

        for v in inspector.get_view_names():
            self.conn.execute(DropView(v))

        for fkc in all_fks:
            self.conn.execute(DropConstraint(fkc))

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

    def select_streams_by_collection(self, coll):

        coll_t = self.metadata.tables['collections']
        streams_t = self.metadata.tables['streams']

        selected = []

        sql = select([streams_t.c.id, coll_t.c.streamtable, streams_t.c.name]).select_from(coll_t.join(streams_t, streams_t.c.collection == coll_t.c.id)).where(coll_t.c.id == coll)
        result = sql.execute()

        for row in result:
            s_id = row[0]
            table = self.metadata.tables[row[1]]
            name = row[2]
            
            stream_data = table.select().where(table.c.stream_id == s_id).execute()

            assert(stream_data.rowcount == 1)
            stream = stream_data.fetchone()

            stream_dict = {}
            for k,v in stream.items():
                stream_dict[k] = v
            stream_data.close()
            stream_dict['name'] = name
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

        data = []
        for r in result:
            foo = {}
            for i in range(0, len(tablecols)):
                foo[tablecols[i].name] = r[i]
            data.append(foo)

        result.close()

        return data


    def _group_columns(self, table, selectors, groups, aggregator, bts=None):
        aggfunc = self._get_aggregator(aggregator)
        if aggfunc == None:
            return []

        if groups == None:
            groups = []

        aggcols = []
        groupcols = []

        for col in table.columns:
            if col.name in selectors:
                labelstr = col.name
                newcol = label(labelstr, aggfunc(col))
                aggcols.append(newcol)
            if col.name in groups:
                groupcols.append(col)

        # If we are binning, put the timestamp column into the group list to
        # ensure the bins are of appropriate size
        # However, we also want to create a special aggregator column that
        # contains the maximum timestamp from the bin -- this helps a lot
        # when graphing ts data because the last data point will have the
        # timestamp of the most recent data.
        if bts is not None:
            aggts = label('timestamp', func.max(table.c.timestamp))
            aggcols.append(aggts)
            selectcols = aggcols + groupcols
            groupcols.append(bts)
        else:
            selectcols = aggcols + groupcols


        return selectcols, groupcols
    
    def _group_select(self, selectcols, wherecl, groupcols, tscol):
        result = select(selectcols).where(wherecl).group_by(*groupcols).order_by(tscol).execute()
                
        data = []
        for r in result:
            foo = {}
            for i in range(0, len(selectcols)):
                foo[selectcols[i].name] = r[i]
            data.append(foo)
        
        result.close()
        return data 
       
    def _select_binned(self, table, wherecl, selectors, groups, size, aggre):
        bts = label('timestamp', table.c.timestamp / size * size)
        selectcols, groupcols = self._group_columns(table, selectors, groups, 
                aggre, bts)
        return self._group_select(selectcols, wherecl, groupcols, bts)


    def _select_unbinned(self, table, wherecl, selectors, groups, 
            aggregator):

        selectcols, groupcols = self._group_columns(table, selectors, groups, 
                aggregator)

        mints = label("min_timestamp", func.min(table.c.timestamp))
        selectcols.append(mints)
        selectcols.append(label("max_timestamp", func.max(table.c.timestamp)))


        return self._group_select(selectcols, wherecl, groupcols, mints)
        
        

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

        if binsize == 0:
            return self._select_unbinned(table, wherecl, aggcols, groupcols,
                    aggregator)

        return self._select_binned(table, wherecl, aggcols, groupcols,
                binsize, aggregator)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
