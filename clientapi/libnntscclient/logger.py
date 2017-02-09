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


import sys
import logging
import time

backgrounded = False
logger = None

def createLogger(flag, logfile, name):
    global backgrounded, logger
    backgrounded = flag

    if backgrounded:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.FileHandler(logfile)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        #logging.basicConfig(filename="/tmp/nntsc.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

def log(msg):

    if not backgrounded:
        print >> sys.stderr, time.strftime("%d %b %Y %H:%M:%S"), msg
    else:
        logger.debug(msg)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
