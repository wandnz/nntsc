from libnntsc.dberrorcodes import *
import libnntscclient.logger as logger
import time

class NNTSCParser(object):
    def __init__(self, db):
        self.db = db
        self.streamtable = None
        self.datatable = None
        self.colname = "Unspecified"
        self.source = None
        self.module = None

        self.streams = {}
        self.streamcolumns = []
        self.uniquecolumns = []
        self.streamindexes = []
        self.datacolumns = []
        self.dataindexes = []
        self.exporter = None

    def add_exporter(self, exp):
        self.exporter = exp

    def get_data_table_name(self):
        return self.datatable

    def get_streams_table_name(self):
        return self.streamtable

    def get_last_timestamp(self, stream):
        err, lastts = self.db.get_last_timestamp(self.datatable, stream)
        if err != DB_NO_ERROR:
            return time.time()
        return lastts

    def _create_indexes(self, table, indexes):
        for ind in indexes:
            if "columns" not in ind or len(ind["columns"]) == 0:
                logger.log("Index for %s has no columns -- skipping" % (table))
                continue

            if "name" not in ind:
                indname = ""
            else:
                indname = ind["name"]

            err = self.db.create_index(indname, table, ind["columns"])
            if err != DB_NO_ERROR:
                logger.log("Failed to create index for %s" % (table))
                return err

        return DB_NO_ERROR

    def stream_table(self):
        err = self.db.create_streams_table(self.streamtable, 
                self.streamcolumns, self.uniquecolumns)
        
        if err != DB_NO_ERROR:
            logger.log("Failed to create streams table for %s" % (self.colname))
            return err
        
        err = self._create_indexes(self.streamtable, self.streamindexes)
        if err != DB_NO_ERROR:
            return err

        err = self.db.commit_streams()
        if err != DB_NO_ERROR:
            return err

        return DB_NO_ERROR

    def data_table(self):
        err = self.db.create_data_table(self.datatable, self.datacolumns)
        if err != DB_NO_ERROR:
            logger.log("Failed to create data table for %s" % (self.colname))
            return err

        err = self._create_indexes(self.datatable, self.dataindexes)
        if err != DB_NO_ERROR:
            return err

        err = self.db.commit_streams()
        if err != DB_NO_ERROR:
            return err
        return DB_NO_ERROR


    def register(self):
        err = self.stream_table()
        if err != DB_NO_ERROR:
            return err

        err = self.data_table()
        if err != DB_NO_ERROR:
            return err

        return self.db.register_collection(self.source, self.module, 
                self.streamtable, self.datatable)

    def create_new_stream(self, result, timestamp):

        streamprops = {}
        for col in self.streamcolumns:
            if col['name'] in result:
                streamprops[col['name']] = result[col['name']]
            else:
                streamprops[col['name']] = None

        while 1:
            errorcode = DB_NO_ERROR
            colid, streamid = self.db.insert_stream(self.streamtable, 
                self.datatable, timestamp, streamprops)

            if colid < 0:
                errorcode = streamid

            if streamid < 0:
                errorcode = streamid
    
            if errorcode == DB_QUERY_TIMEOUT:
                continue
            if errorcode != DB_NO_ERROR:
                return errorcode
        
            err = self.db.commit_streams()
            if err == DB_QUERY_TIMEOUT:
                continue
            if err != DB_NO_ERROR:
                return err
            break
        
        if self.exporter == None:
            return streamid


        # TODO get this once rather than every time we add a stream
        err, colid = self.db.get_collection_id(self.source, self.module)
        if err != DB_NO_ERROR:
            return err
        if colid == 0:
            return streamid

        self.exporter.publishStream(colid, self.colname, streamid, streamprops)
        return streamid

    def insert_data(self, stream, ts, result, casts = {}):
        filtered = {}
        for col in self.datacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        err = self.db.insert_data(self.datatable, self.colname, stream, ts, 
                filtered, casts)
        if err != DB_NO_ERROR:
            return err

        # NOTE colname is actually unused by the exporter, so don't panic
        # that we export a collection id number for streams and a string
        # for live data.
        # TODO get rid of this to avoid confusion
        if self.exporter != None:
            self.exporter.publishLiveData(self.colname, stream, ts, filtered)

        return DB_NO_ERROR



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
