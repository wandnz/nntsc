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


nntsc_hdr_fmt = "!BBL"
nntsc_req_fmt = "!LLQ"

NNTSC_CLIENTAPI_VERSION="1.7.0"

NNTSC_REQ_COLLECTION = 0
NNTSC_REQ_STREAMS = 1
NNTSC_REQ_SCHEMA = 2
NNTSC_REQ_ACTIVE_STREAMS = 3

NNTSC_REQUEST = 0
NNTSC_COLLECTIONS = 1
NNTSC_SCHEMAS = 2
NNTSC_STREAMS = 3
NNTSC_SUBSCRIBE = 4
NNTSC_HISTORY = 5
NNTSC_LIVE = 6
NNTSC_AGGREGATE = 7
NNTSC_PERCENTILE = 8
NNTSC_PUSH = 9
NNTSC_ACTIVE_STREAMS = 10
NNTSC_REGISTER_COLLECTION = 11
NNTSC_VERSION_CHECK = 12
NNTSC_HISTORY_DONE = 13
NNTSC_QUERY_CANCELLED = 14
NNTSC_UNSUBSCRIBE = 15
NNTSC_MATRIX = 16
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
