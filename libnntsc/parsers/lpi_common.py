#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
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


import sys, struct
from socket import *
import libnntscclient.logger as logger

lpicp_header_fmt = "!BBHHH"
lpicp_stats_fmt = "!LLLBBHHH"
lpicp_cnt_fmt = "!Q"
lpicp_stats_rec_fmt = "!LQ"
lpicp_proto_subhdr_fmt = "!L"
lpicp_proto_rec_fmt = "!LH"

dirnames = ["out", "in"]
metnames = ["packets", "bytes", "newflows", "currflows", "peakflows", "activeusers", "observedusers"]

def get_metric_name(metid):

    if metid >= len(metnames):
        return "unknown"
    if metid < 0:
        return "unknown"

    return metnames[metid]

def connect_lpi_server(host, port):
    try:
        s = socket(AF_INET, SOCK_STREAM)
    except error, msg:
        logger.log("Failed to create socket: %s\n" % (msg[1]))
        return -1
    try:
        s.connect((host, port))
    except error, msg:
        logger.log("Failed to connect to %s on port %u: %s\n" %
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
    buf_parsed += user_len

    stat_record["id"] = msg_buf[0:name_len]
    stat_record["ts"] = stats_hdr[0]
    stat_record["freq"] = int(stats_hdr[2])
    stat_record["dir"] = dirnames[int(stats_hdr[3])]
    stat_record["metric"] = metnames[int(stats_hdr[4])]
    stat_record["results"] = {}

    for i in range(0, int(stats_hdr[5])):
# getting the protocol ID
        low = buf_parsed
        upp = buf_parsed + struct.calcsize(lpicp_stats_rec_fmt)
        proto_rec = struct.unpack(lpicp_stats_rec_fmt, str(msg_buf[low:upp]))
        buf_parsed += struct.calcsize(lpicp_stats_rec_fmt)
        stat_record["results"][proto_rec[0]] = proto_rec[1]

        assert(buf_parsed <= buf_read)

    return stat_record


def read_lpicp_hdr(s):
    try:
        msg_buf = s.recv(struct.calcsize(lpicp_header_fmt))
    except error, msg:
        logger.log("Error receiving header: %s\n" % (msg[1]))
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
            logger.log("Error receiving body: %s\n" % (msg[1]))
            return ""

        if foo is None or len(foo) == 0:
            return ""

        msg_buf += foo
        received = len(msg_buf)

    return msg_buf


def read_lpicp(s):

    lpicp_hdr, to_read = read_lpicp_hdr(s)
    if lpicp_hdr == {}:
        logger.log("Failed to read LPICP header")
        return -1, {}

    if int(lpicp_hdr[0]) != 1:
        logger.log("Invalid LPICP version number: %u" % (int(lpicp_hdr[0])))
        return -1, {}

    msg_buf = receive_lpicp_message(s, to_read)
    if msg_buf == "":
        return -1, {}

    name_len = int(lpicp_hdr[3])

    data = {}

    if int(lpicp_hdr[1]) == 0:
        data = parse_stat_record(msg_buf, name_len)

    if int(lpicp_hdr[1]) == 4:
        data = parse_protocols_record(msg_buf, name_len)

    return int(lpicp_hdr[1]), data


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
