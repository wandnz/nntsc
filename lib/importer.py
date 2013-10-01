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


import os, sys

import libnntsc.parsers

def import_parsers(enabled):
    modules = {}

    for module in enabled:
        __import__('libnntsc.parsers.%s' % module)
        modules[module] = sys.modules['libnntsc.parsers.' + module]
    return modules

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
