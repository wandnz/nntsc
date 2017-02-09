#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#          Andy Bell
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

from libnntsc.parsers.amp_icmp import AmpIcmpParser
from libnntsc.parsers.amp_dns import AmpDnsParser
from libnntsc.parsers.amp_http import AmpHttpParser
from libnntsc.parsers.amp_throughput import AmpThroughputParser
from libnntsc.parsers.amp_tcpping import AmpTcppingParser
from libnntsc.parsers.amp_udpstream import AmpUdpstreamParser
from libnntsc.parsers.amp_traceroute_pathlen import AmpTraceroutePathlenParser


def get_parser(table_name):
    """Returns a parser for the given table"""
    if table_name == "data_amp_icmp":
        parser = AmpIcmpParser(None)
    elif table_name == "data_amp_tcpping":
        parser = AmpTcppingParser(None)
    elif table_name == "data_amp_dns":
        parser = AmpDnsParser(None)
    elif table_name == "data_amp_throughput":
        parser = AmpThroughputParser(None)
    elif table_name == "data_amp_http":
        parser = AmpHttpParser(None)
    elif table_name == "data_amp_udpstream":
        parser = AmpUdpstreamParser(None)
    elif table_name == "data_amp_traceroute_pathlen":
        parser = AmpTraceroutePathlenParser(None)
    else:
        parser = None

    return parser

def getMatrixCQ(table_name):
    parser = get_parser(table_name)
    return parser.get_matrix_cq()

def get_cqs(table_name, bin_size=None):
    """Gets continuous queries for given table at given bin size,
    or all cqs if no bin size given"""
    parser = get_parser(table_name)
    if parser is None:
        return []

    cqs = parser.get_cqs()
    if bin_size is None:
        return cqs

    for times, cols in cqs:
        if bin_size in [x[0] for x in times]:
            return cols

    return []

def build_cqs(influxdb, retention_policy="default"):
    """Builds continuous queries on influxdb"""
    parsers = []

    parsers.append(AmpIcmpParser(None, influxdb))
    parsers.append(AmpTcppingParser(None, influxdb))
    parsers.append(AmpDnsParser(None, influxdb))
    parsers.append(AmpThroughputParser(None, influxdb))
    parsers.append(AmpHttpParser(None, influxdb))
    parsers.append(AmpUdpstreamParser(None, influxdb))
    parsers.append(AmpTraceroutePathlenParser(None, influxdb))

    for parser in parsers:
        parser.build_cqs(retention_policy)
