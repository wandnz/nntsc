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

        self.collectionid = None

    def add_exporter(self, exp):
        self.exporter = exp

    def get_data_table_name(self):
        return self.datatable

    def get_streams_table_name(self):
        return self.streamtable

    def get_last_timestamp(self, stream):
        lastts = self.db.get_last_timestamp(self.datatable, stream)
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

            try:
                self.db.create_index(indname, table, ind["columns"])
            except DBQueryException as e:
                logger.log("Failed to create index for %s" % (table))
                logger.log("Error was: %s" % (str(e)))
                raise

    def stream_table(self):
        self.db.create_streams_table(self.streamtable, 
                self.streamcolumns, self.uniquecolumns)
        
        self._create_indexes(self.streamtable, self.streamindexes)
        self.db.commit_streams()

    def data_table(self):
        self.db.create_data_table(self.datatable, self.datacolumns)
        self._create_indexes(self.datatable, self.dataindexes)
        self.db.commit_streams()

    def register(self):
        try:
            self.stream_table()
        except DBQueryException as e:
            logger.log("Failed to create streams table for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            self.data_table()
        except DBQueryException as e:
            logger.log("Failed to create data table for %s" % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            self.db.register_collection(self.source, self.module, 
                self.streamtable, self.datatable)
        except DBQueryException as e:
            logger.log("Failed to register new collection %s in database" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

    def _get_collection_id(self):
        if self.collectionid == None:
            try:
                colid = self.db.get_collection_id(self.source, self.module)
            except DBQueryException as e:
                logger.log("Failed to get collection id for %s" % \
                        (self.colname))
                logger.log("Error was: %s" % (str(e)))
                raise

            if colid > 0:
                self.collectionid = colid
            else:
                return -1

        return self.collectionid

    def create_new_stream(self, result, timestamp):

        streamprops = {}
        for col in self.streamcolumns:
            if col['name'] in result:
                streamprops[col['name']] = result[col['name']]
            else:
                streamprops[col['name']] = None

        try:
            streamid = self.db.insert_stream(self.streamtable, 
                    self.datatable, timestamp, streamprops)
        except DBQueryException as e:
            logger.log("Failed to insert new stream into database for %s" \
                    % (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise

        try:
            self.db.commit_streams()
        except DBQueryException as e:
            logger.log("Failed to commit new stream for %s" % \
                    (self.colname))
            logger.log("Error was: %s" % (str(e)))
            raise
        
        if self.exporter == None:
            return streamid

        colid = self._get_collection_id()
        if colid <= 0:
                # Not sure what we should do if we get a bad collection id,
                # but for now I'm going to go with not exporting the new
                # stream
                return streamid

        self.exporter.publishStream(colid, self.colname, streamid, 
                streamprops)
        return streamid

    def insert_data(self, stream, ts, result, casts = {}):
        filtered = {}
        for col in self.datacolumns:
            if col["name"] in result:
                filtered[col["name"]] = result[col["name"]]
            else:
                filtered[col["name"]] = None

        try:
            self.db.insert_data(self.datatable, self.colname, stream, ts, 
                    filtered, casts)
        except DBQueryException as e:
            logger.log("Failed to insert new data for %s stream %d" % \
                    (self.colname, stream))
            logger.log("Error was: %s" % (str(e)))
            raise

        # NOTE colname is actually unused by the exporter, so don't panic
        # that we export a collection id number for streams and a string
        # for live data.
        # TODO get rid of this to avoid confusion

        colid = self._get_collection_id()

        if self.exporter != None and colid > 0:
            self.exporter.publishLiveData(colid, stream, ts, filtered)


    def _find_median(self, datapoints):
        if len(datapoints) == 0:
            return None

        half = int(len(datapoints) / 2)
        if (len(datapoints) % 2) == 1:
            return datapoints[half]

        return (datapoints[half] + datapoints[half - 1]) / 2


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
