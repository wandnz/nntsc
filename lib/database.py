from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.sql.expression import select, outerjoin, func
from sqlalchemy.engine.url import URL

import time, sys

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
            print 'Connecting to db using "%s"' % connect_string

        self.init_error = False

        self.engine = create_engine(connect_string, echo=debug)

        self.meta = MetaData(self.engine)
        try:
            self.meta.reflect(bind=self.engine)
        except OperationalError, e:
            print >> sys.stderr, "Error binding to database %s" % (dbname)
            print >> sys.stderr, "Are you sure you've specified the right database name?"
            self.init_error = True
            sys.exit(1)
        self.conn = self.engine.connect()
        
        self.stream_tables = {}
        self.data_tables = {}

        for name, tab in self.meta.tables.items():
            if name[0:5] == "data_":
                self.data_tables[name] = tab
            if name[0:8] == "streams_":
                self.stream_tables[name] = tab

        self.trans = self.conn.begin()
        self.pending = 0

    def __del__(self):
        if not self.init_error:
            self.commit_transaction()
            self.conn.close()

    def build_databases(self, modules, new=False):
        if new:
            self.__delete_everything(self.engine)

        self.metadata = MetaData(self.engine)

        self.streams = Table('streams', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('module', String, nullable=False),
            Column('modsubtype', String, nullable=True),
            Column('name', String, nullable=False, unique=True),
        )

        for base,mod in modules.items():
            table_dict = mod.tables()
            for name, tables in table_dict.items():
                try:
                    t = self.__build_mod_table("streams_" + name, \
                            tables[0], tables[1])
                    self.stream_tables[name] = t
                except:
                    raise   # XXX

                try:
                    t = self.__build_data_table("data_" + name, tables[2])
                    self.data_tables[name] = t
                except:
                    raise   # XXX

        self.metadata.create_all()
        self.commit_transaction()

    def __delete_everything(self, engine):
        self.meta.drop_all(bind=engine)

    def __build_mod_table(self, name, columns, constraint):
        id_col = Column('stream_id', Integer, ForeignKey("streams.id"), \
                    nullable = False, primary_key=True)
        constraint = UniqueConstraint(*(constraint))
        columns = [id_col] + columns + [constraint]
        t = Table(name, self.metadata, *columns)
        
        return t
    
    def __build_data_table(self, name, columns):
        id_col = Column('stream_id', Integer, ForeignKey("streams.id"), \
                    nullable = False)
        ts_col = Column('timestamp', Integer, nullable=False)
        columns = [id_col, ts_col] + columns
        t = Table(name, self.metadata, *columns)
        
        return t

    """ Find the correct module table for the specified stream_id """
    def __get_mod_table(self, stream_id):
        # XXX This seems kinda slow....
        for i in self.meta.tables.keys():
            if i.find('streams_') != -1:
                mod = self.meta.tables[i]
                sql = mod.select().where(mod.c.stream_id==stream_id)
                result = sql.execute()
                if result.rowcount == 1:
                    return mod
    
    def __get_data_table(self, stream_id):
        # XXX This seems kinda slow....
        for i in self.meta.tables.keys():
            if i.find('data_') != -1:
                mod = self.meta.tables[i]
                sql = mod.select().where(mod.c.stream_id==stream_id)
                result = sql.execute()
                if result.rowcount == 1:
                    return mod

    def list_collections(self):
        collections = []

        for i in self.meta.tables.keys():
            if i[0:5] == 'data_':
                collections.append(i[5:])
        return collections

    def get_stream_schema(self, name):
        key = 'streams_' + name

        if not self.stream_tables.has_key(key):
            return []

        schema = []
        table = self.stream_tables[key]

        return table.columns
    
    def get_data_schema(self, name):
        key = 'data_' + name
        if not self.data_tables.has_key(key):
            return []

        schema = []
        table = self.data_tables[key]

        return table.columns

    def select_streams_by_module(self, mod):

        # Find all streams matching a given module type
        
        # For each stream:
        #   Form a dictionary containing all the relevant information about
        #   that stream (this will require info from both the combined streams
        #   table and the module/subtype specific table

        # Put all the dictionaries into a list

        streams_t = self.meta.tables['streams']
        streams = []

        for name, table in self.stream_tables.items():
            sql = table.join(streams_t, streams_t.c.id == table.c.stream_id).select().where(streams_t.c.module==mod)
            result = sql.execute()

            for row in result:
                row_dict = {}
                for k,v in row.items():
                    if k == 'id':
                        continue
                    row_dict[k] = v
                streams.append(row_dict)
            result.close()
        return streams

    def select_streams_by_collection(self, coll):
        key = 'streams_' + coll
        
        if not self.stream_tables.has_key(key):
            return []

        table = self.stream_tables[key]
        streams_t = self.meta.tables['streams']

        streams = []

        sql = table.join(streams_t, streams_t.c.id == table.c.stream_id).select()
        result = sql.execute()

        for row in result:
            row_dict = {}
            for k,v in row.items():
                if k == 'id':
                    continue
                row_dict[k] = v
            streams.append(row_dict)
        result.close()

        return streams

    def select_stream_by_id(self, stream_id):
        # find the mod table this id is in
        mod = self.__get_stream_table(stream_id)

        result = outerjoin(self.meta.tables['streams'], mod).select( \
                mod.c.stream_id==stream_id).execute()

        stream = dict(result.fetchone())

        result.close()

        return stream

    def commit_transaction(self):
        # TODO: Better error handling!

        if self.pending == 0:
            return
       
        #print "Committing %d statements (%s)" % (self.pending, \
        #        time.strftime("%d %b %Y %H:%M:%S", time.localtime())) 
        try:
            self.trans.commit()
        except:
            self.trans.rollback()
            raise
        self.trans = self.conn.begin()
        self.pending = 0

    def rollback_transaction(self):
        #if self.pending == 0:
        #    return
        self.trans.rollback()
        self.trans = self.conn.begin()
        self.pending = 0

    """
        This function inserts an entry into both the global streams table and
        the streams table for the specific module.

        stream_id - unique id for this stream
        mod - module name (e.g. rrd)
        name - give this stream a human readable name

        additional arguments must be those specified by the module.
        See the modules documentation for this.

        An example line for the rrd module is shown below.

        insert_stream( \
                stream_id=1, \
                mod='rrd', \
                name='Lightwire Gateway Traffic', \
                filename='/tmp/smokeping/Lightwire/lwgateway.rrd', \
                substream=0, \
                lasttimestamp=0)

    """
    def insert_stream(self, mod, modsubtype, name, **kwargs):
        self.commit_transaction()
        table = self.meta.tables['streams']
        
        result = select([func.max(table.c.id)]).execute()
        assert(result.rowcount == 1)
        
        max_tuple = result.fetchone()
        if max_tuple == (None,):
            next_stream_id = 0
        else:
            next_stream_id = max_tuple[0] + 1
        result.close()
       
        try: 
            result = self.conn.execute(table.insert(), \
                    id=next_stream_id, module=mod, modsubtype=modsubtype, \
                    name=name)
        except IntegrityError, e:
            self.rollback_transaction()
            print >> sys.stderr, e
            return -1

        stream_id = next_stream_id
        result.close()
        self.pending += 1

        # Don't insert duplicate streams! If we are a duplicate, we need
        # to rollback the previous insert as well
        table = self.meta.tables['streams_'+mod+"_"+modsubtype]
        try:
            result = self.conn.execute(table.insert(), stream_id=stream_id, \
                    **kwargs)
        except IntegrityError, e:
            self.rollback_transaction()
            print >> sys.stderr, e
            return -1
        else:
            result.close()

        self.pending += 1
        self.commit_transaction()
        return stream_id

    def insert_data(self, mod, modsubtype, stream_id, timestamp, **kwargs):
        table = self.meta.tables['data_' + mod + "_" + modsubtype]
        result = self.conn.execute(table.insert(), stream_id = stream_id, \
                timestamp=timestamp, **kwargs)
                
        # TODO check result for errors
        result.close()
        
        self.pending += 1

    def update_timestamp(self, stream_id, lasttimestamp):
        table = self.__get_mod_table(stream_id)
        result = self.conn.execute(table.update().where( \
                table.c.stream_id==stream_id).values( \
                lasttimestamp=lasttimestamp))
        result.close()
        self.pending += 1

    """
        Get data from the database

        Both start_time and stop_time are inclusive values
    """
    def select_data(self, mod, modsubtype, stream_ids, columns, 
            start_time=None, stop_time=None):

        tablekey = 'data_' + mod + '_' + modsubtype

        if not self.data_tables.has_key(tablekey):
            return []

        table = self.data_tables[tablekey]

        tablecols = filter(lambda a: a.name in columns, table.columns)

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

        # Run the query and convert the results into something we can use
        result = select(tablecols).where(query).order_by(
                table.c.timestamp).execute()

        data = list(result)
        result.close()

        return data

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
