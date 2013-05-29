from libnntsc.database import Database
from libnntsc.configurator import *
from libnntsc.parsers import lpi_bytes, lpi_common

from socket import *
import sys, struct

class LPIModule:
    def __init__(self, existing, nntsc_conf, exp):

        self.exporter = exp

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        lpiserver = get_nntsc_config(nntsc_conf, 'lpi', 'server')
        if lpiserver == "NNTSCConfigError":
            sys.exit(1)
        
        if lpiserver == "":
            print >> sys.stderr, "No LPI Server specified, disabling module"
            sys.exit(0)

        lpiport = get_nntsc_config(nntsc_conf, 'lpi', 'port')
        if lpiport == "NNTSCConfigError":
            sys.exit(1)
        if lpiport == "":
            lpiport = 3678

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])

        for s in existing:
            
            if s['modsubtype'] == "bytes":
                lpi_bytes.create_existing_stream(s)
     
        self.server_fd = lpi_common.connect_lpi_server(lpiserver, int(lpiport))
        if self.server_fd == -1:
            sys.exit(1)

        self.protocol_map = {}    


    def process_stats(self, data):
        if data == {}:
            print >> sys.stderr, "LPIModule: Empty Stats Dict"
            return -1

        if data['metric'] == "bytes":
            return lpi_bytes.process_data(self.db, self.exporter, \
                    self.protocol_map, data)
        

        return 0

    def run(self):
        while True:
            rec_type, data = lpi_common.read_lpicp(self.server_fd)

            if rec_type == 3:
                self.db.commit_transaction()

            if rec_type == 4:
                self.protocol_map = data

            if rec_type == 0:
                if self.process_stats(data) == -1:
                    print >> sys.stderr, "LPIModule: Invalid Statistics Data"
                    break

            if rec_type == -1:
                break

        self.server_fd.close()

def run_module(existing, config, exp):
    lpi = LPIModule(existing, config, exp)
    lpi.run()

def tables(db):

    st_name = lpi_bytes.stream_table(db)
    dt_name = lpi_bytes.data_table(db)

    db.register_collection("lpi", "bytes", st_name, dt_name)



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
