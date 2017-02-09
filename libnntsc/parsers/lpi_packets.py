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


import libnntscclient.logger as logger
import sys, string
from libnntsc.dberrorcodes import *
from libnntsc.parsers.lpi_bytes import LPIBytesParser

# This is really similar to the LPI bytes parser, so we'll inherit from
# that instead and just make a few minor changes

class LPIPacketsParser(LPIBytesParser):
    def __init__(self, db):
        super(LPIPacketsParser, self).__init__(db)

        self.streamtable = "streams_lpi_packets"
        self.datatable = "data_lpi_packets"
        self.colname = "lpi_packets"
        self.module = "packets"

        self.datacolumns = [
            {"name":"packets", "type":"bigint"}
        ]

    def _result_dict(self, val):
        return {'packets':val}


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
