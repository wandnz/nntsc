from libnntsc.database import Database
from libnntsc.configurator import *

from socket import *
import sys, struct

lpicp_header_fmt = "!BBHHH"
lpicp_stats_fmt = "!LLLBBHHH"
lpicp_cnt_fmt = "!Q"
lpicp_stats_rec_fmt = "!LQ"
lpicp_proto_subhdr_fmt = "!L"
lpicp_proto_rec_fmt = "!LH"

dirnames = ["out", "in"]
metnames = ["pkts", "bytes", "new_flows", "curr_flows", "peak_flows", "active_ips", "observed_ips"]



def lpi_stat_stream_table():
    from sqlalchemy import Column
    from sqlalchemy.types import String, Integer

    return [
        Column('source', String, nullable=False),
        Column('user', String, nullable=False),
        Column('dir', String, nullable=False),
        Column('freq', Integer, nullable=False),
        Column('metric', String, nullable=False),
        Column('protocol', String, nullable=False)
    ]

def lpi_stat_stream_constraints():
    return ['source', 'user', 'dir', 'freq', 'metric', 'protocol']

def lpi_stat_parse_db_row():
    pass

def lpi_stat_data_table():
    from sqlalchemy import Column
    from sqlalchemy.types import String, Integer

    return [
        Column('value', Integer, nullable=False)
    ]

def lpi_stat_insert_stream(db, source, user, direct, freq, metric, protocol,
        name):
    
    return db.insert_stream(mod="lpi", modsubtype="stat", name=name, 
            source=source, 
            user=user, dir=direct, freq=freq, metric=metric, protocol=protocol)

def lpi_stat_insert_data(db, stream, ts, result):

    kwargs = {}
    kwargs['value'] = result

    db.insert_data(mod="lpi", modsubtype="stat", stream_id=stream, 
            timestamp=ts, **kwargs)

def connect_lpi_server(host, port):
    try:
        s = socket(AF_INET, SOCK_STREAM)
    except error, msg:
        sys.stderr.write("Failed to create socket: %s\n" % (msg[1]))
        return -1
    try:
        s.connect((host, port))
    except error, msg:
        sys.stderr.write("Failed to connect to %s on port %u: %s\n" %
                (host, port, msg[1]))
        return -1

    return s

def parse_protocols_record(msg_buf, name_len):
    proto_record = {}
    buf_read = len(msg_buf)
    buf_parsed = name_len

    #getting the number of records from the subheader
    low = buf_parsed
    upp = buf_parsed + struct.calcsize(lpicp_proto_subhdr_fmt)
    proto_subhdr = struct.unpack(lpicp_proto_subhdr_fmt, str(msg_buf[low:upp]))
    buf_parsed += struct.calcsize(lpicp_proto_subhdr_fmt)
    num_records = proto_subhdr[0]


    for i in range(0, num_records):
        low = buf_parsed
        upp = buf_parsed + struct.calcsize(lpicp_proto_rec_fmt)
        proto_rec = struct.unpack(lpicp_proto_rec_fmt, str(msg_buf[low:upp]))
        buf_parsed += struct.calcsize(lpicp_proto_rec_fmt)

        id = proto_rec[0]
        string_len = proto_rec[1]

        proto_name = msg_buf[buf_parsed:buf_parsed + string_len]
        proto_record[id] = proto_name

        buf_parsed += string_len

    return proto_record

def parse_stat_record(msg_buf, name_len):
    stat_record = {}
    buf_read = len(msg_buf)
    buf_parsed = name_len

    low = buf_parsed
    upp = buf_parsed + struct.calcsize(lpicp_stats_fmt)
    stats_hdr = struct.unpack(lpicp_stats_fmt, str(msg_buf[low:upp]))
    buf_parsed += struct.calcsize(lpicp_stats_fmt)

    user_len = int(stats_hdr[6])
    stat_record["user"] = msg_buf[buf_parsed:buf_parsed + user_len]
    buf_parsed += user_len;

    stat_record["id"] = msg_buf[0:name_len]
    stat_record["ts"] = stats_hdr[0]
    stat_record["freq"] = int(stats_hdr[2])
    stat_record["dir"] = dirnames[int(stats_hdr[3])]
    stat_record["metric"] = metnames[int(stats_hdr[4])]
    stat_record["results"] = []

    for i in range(0, int(stats_hdr[5])):
# getting the protocol ID
        low = buf_parsed
        upp = buf_parsed + struct.calcsize(lpicp_stats_rec_fmt)
        proto_rec = struct.unpack(lpicp_stats_rec_fmt, str(msg_buf[low:upp]))
        buf_parsed += struct.calcsize(lpicp_stats_rec_fmt)
        stat_record["results"].append((proto_rec[0], proto_rec[1]))

        assert(buf_parsed <= buf_read)

    return stat_record


def read_lpicp_hdr(s):
    try:
        msg_buf = s.recv(struct.calcsize(lpicp_header_fmt))
    except error, msg:
        sys.stderr.write("Error receiving header: %s\n" %
                (msg[1]))
        return {}, 0

    if not msg_buf:
        return {}, 0

    lpicp_hdr = struct.unpack(lpicp_header_fmt, msg_buf)
    to_read = int(lpicp_hdr[2]) - struct.calcsize(lpicp_header_fmt)

    return lpicp_hdr, to_read


def receive_lpicp_message(s, to_read):

    received = 0
    msg_buf = ""
    while received != to_read:
        try:
            foo = s.recv(to_read - received)
        except error, msg:
            sys.stderr.write("Error receiving body: %s\n" %
                    (msg[1]))
            return ""
        msg_buf += foo
        received = len(msg_buf)

    return msg_buf


def read_lpicp(s):
    
    lpicp_hdr, to_read = read_lpicp_hdr(s)
    if lpicp_hdr == {}:
        print >> sys.stderr, "Failed to read LPICP header"
        return -1, {}

    if int(lpicp_hdr[0]) != 1:
        print >> sys.stderr, "Invalid LPICP version number: %u" % (int(lpicp_hdr[0]))
        return -1, {}

    msg_buf = receive_lpicp_message(s, to_read)
    if msg_buf == []:
        return -1, {}

    name_len = int(lpicp_hdr[3])

    data = {}

    if int(lpicp_hdr[1]) == 0:
        data = parse_stat_record(msg_buf, name_len)

    if int(lpicp_hdr[1]) == 4:
        data = parse_protocols_record(msg_buf, name_len)

    return int(lpicp_hdr[1]), data


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
 
        self.streams = {}

        for s in existing:
            self.streams[(s['source'], s['user'], s['dir'], s['freq'], 
                    s['metric'], s['protocol'])] = s['stream_id']

        self.server_fd = connect_lpi_server(lpiserver, int(lpiport))
        if self.server_fd == -1:
            sys.exit(1)

        self.protocol_map = {}    

    def find_stream(self, mon, user, dir, freq, metric, proto):
        k = (mon, user, dir, freq, metric, self.protocol_map[proto])

        if self.streams.has_key(k):
            return self.streams[k]
        return -1

    def add_new_stream(self, mon, user, dir, freq, metric, proto):
        k = (mon, user, dir, freq, metric, self.protocol_map[proto])

        namestr = "%s %s_%s_%s (%s:%s)" % (self.protocol_map[proto], dir, 
                        metric, freq, mon, user) 

        stream_id = lpi_stat_insert_stream(self.db, mon, user, dir, freq, 
                metric, self.protocol_map[proto], namestr)
        self.streams[k] = stream_id
        return stream_id

    def process_stats(self, data):
        if data == {}:
            print >> sys.stderr, "LPIModule: Empty Stats Dict"
            return -1
        
        mon = data['id']
        user = data['user']
        dir = data['dir']
        freq = data['freq']
        metric = data['metric']

        for n in data['results']:
       
            if n[0] not in self.protocol_map.keys():
                print >> sys.stderr, "LPIModule: Unknown protocol id %u" % (n[0])
                return -1
     
            stream_id = self.find_stream(mon, user, dir, freq, metric, n[0])
            if stream_id == -1:
                stream_id = self.add_new_stream(mon, user, dir, freq, metric, n[0])
            if stream_id == -1:
                print >> sys.stderr, "LPIModule: Cannot create a stream for this data"
                print >> sys.stderr, "LPIModule: %s:%s %s_%s_%s %s\n" % (mon, user, dir, metric, freq, n[0])
                return -1
            
            lpi_stat_insert_data(self.db, stream_id, data['ts'], n[1])

    def run(self):
        while True:
            rec_type, data = read_lpicp(self.server_fd)

            if rec_type == 3:
                self.db.commit_transaction()

            if rec_type == 4:
                self.protocol_map = data

            if rec_type == 0:
                if self.process_stats(data) == -1:
                    print >> sys.stderr, "LPIModule: Invalid Statistics Data"
                    break

        self.server_fd.close()

def run_module(existing, config, exp):
    lpi = LPIModule(existing, config, exp)
    lpi.run()

def tables():
    res = {}
    res["lpi_stat"] = (lpi_stat_stream_table(), lpi_stat_stream_constraints(),
            lpi_stat_data_table())

    return res



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
