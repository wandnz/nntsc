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

DB_NO_ERROR = 0
DB_DATA_ERROR = -1
DB_GENERIC_ERROR = -2
DB_INTERRUPTED = -3
DB_OPERATIONAL_ERROR = -4
DB_CODING_ERROR = -5
DB_DUPLICATE_KEY = -6
DB_NO_CURSOR = -7
DB_QUERY_TIMEOUT = -8
DB_CQ_ERROR = -9

class DBQueryException(Exception):
    def __init__(self, code):
        self.code = code
    def __str__(self):
        if self.code == DB_DATA_ERROR:
            return "Data error detected during NNTSC query"
        if self.code == DB_GENERIC_ERROR:
            return "Generic database error detected during NNTSC query"
        if self.code == DB_INTERRUPTED:
            return "Keyboard interrupt detected during NNTSC query"
        if self.code == DB_CODING_ERROR:
            return "Bad database code encountered during NNTSC query"
        if self.code == DB_DUPLICATE_KEY:
            return "Duplicate key error while performing NNTSC query"
        if self.code == DB_QUERY_TIMEOUT:
            return "NNTSC database query timed out"
        if self.code == DB_OPERATIONAL_ERROR:
            return "Connection to NNTSC database was lost"
        if self.code == DB_NO_CURSOR:
            return "Could not execute query as had no valid cursor"
        if self.code == DB_CQ_ERROR:
            return "Attempted to modify a continuous query that doesn't exist"
        if self.code == DB_NO_ERROR:
            return "No error occurred, why are we getting this exception?"
        return "Unknown error code for DBQueryException: %d" % (self.code)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
