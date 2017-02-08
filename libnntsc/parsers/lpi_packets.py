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
