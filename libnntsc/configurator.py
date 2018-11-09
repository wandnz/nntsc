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


import ConfigParser
import libnntscclient.logger as logger

def load_nntsc_config(filename):
    # load in config file with database settings
    nntsc_config = ConfigParser.SafeConfigParser()

    # add some default values
    nntsc_config.add_section('multicast')
    nntsc_config.set('multicast', 'enabled', 'False')
    nntsc_config.set('multicast', 'group', '224.1.1.1')
    nntsc_config.set('multicast', 'port', '5007')

    if nntsc_config.read([filename]) == []:
        logger.log("Failed to load config file: %s" % (filename))
        return 0

    return nntsc_config


def get_nntsc_config_bool(nntsc_config, section, option):

    if nntsc_config == 0:
        log("Attempted to get a config option after loading failed!")
        return "NNTSCConfigError"

    try:
        result = nntsc_config.getboolean(section, option)
    except ConfigParser.NoSectionError:
        #logger.log("The section '%s' does not exist in the config file" % (section))
        return "NNTSCConfigMissing"
    except ConfigParser.NoOptionError:
        #logger.log("The option '%s' does not exist in section '%s' from the config file" % (option, section))
        return "NNTSCConfigMissing"
    except ValueError:
        logger.log("The option '%s' in section '%s' does not have a boolean value" % (option, section))
        logger.log("Suitable values are 'true', '1', 'on', 'yes', 'false', 0, 'off', or 'no'")
        return "NNTSCConfigError"

    return result

def get_nntsc_config_integer(nntsc_config, section, option):
    if nntsc_config == 0:
        log("Attempted to get a config option after loading failed!")
        return "NNTSCConfigError"

    try:
        result = nntsc_config.getint(section, option)
    except ConfigParser.NoSectionError:
        return "NNTSCConfigMissing"
    except ConfigParser.NoOptionError:
        return "NNTSCConfigMissing"
    except ValueError:
        logger.log("Option '%s' in section '%s' must be an integer" % (
                    option, section))
        return "NNTSCConfigError"

    return result

def get_nntsc_config(nntsc_config, section, option):

    if nntsc_config == 0:
        logger.log("Attempted to get a config option before loading was complete!")
        return "NNTSCConfigError"

    try:
        result = nntsc_config.get(section, option)
    except ConfigParser.NoSectionError:
        #logger.log("The section '%s' does not exist in the config file" % (section))
        return "NNTSCConfigMissing"
    except ConfigParser.NoOptionError:
        #logger.log("The option '%s' does not exist in section '%s' from the config file" % (option, section))
        return "NNTSCConfigMissing"

    return result

def get_influx_config(nntsc_config):

    useinflux = get_nntsc_config_bool(nntsc_config, 'influx', 'useinflux')
    if useinflux == "NNTSCConfigMissing":
        useinflux = False
    dbname = get_nntsc_config(nntsc_config, 'influx', 'database')
    if dbname == "NNTSCConfigMissing":
        dbname = "nntsc"
    dbuser = get_nntsc_config(nntsc_config, 'influx', 'username')
    if dbuser == "NNTSCConfigMissing":
        dbuser = None
    dbpass = get_nntsc_config(nntsc_config, 'influx', 'password')
    if dbpass == "NNTSCConfigMissing":
        dbpass = None
    dbhost = get_nntsc_config(nntsc_config, 'influx', 'host')
    if dbhost == "NNTSCConfigMissing":
        dbhost = "localhost"
    dbport = get_nntsc_config(nntsc_config, 'influx', 'port')
    if dbport == "NNTSCConfigMissing":
        dbport = 8086
    else:
        try:
            dbport = int(dbport)
        except ValueError:
            logger.log("Invalid port number in influx config: {}".format(dbport))
            dbport = "NNTSCConfigError"
    keepdata = get_nntsc_config(nntsc_config, 'influx', 'keepdata')
    if keepdata == "NNTSCConfigMissing":
        keepdata = "inf"
    keeprollups = get_nntsc_config(nntsc_config, 'influx', 'keeprollups')
    if keeprollups == "NNTSCConfigMissing":
        keeprollups = "inf"


    if "NNTSCConfigError" in [useinflux, dbname, dbuser, dbpass, dbhost,
                              dbport, keepdata, keeprollups]:
        return {}

    return {"host":dbhost, "name":dbname, "user":dbuser, "pass":dbpass,
            "port":dbport, "useinflux":useinflux, "keepdata":keepdata,
            "keeprollups":keeprollups}


def get_nntsc_db_config(nntsc_config):

    dbhost = get_nntsc_config(nntsc_config, 'database', 'host')
    if dbhost == "NNTSCConfigMissing":
        dbhost = None
    dbname = get_nntsc_config(nntsc_config, 'database', 'database')
    if dbname == "NNTSCConfigMissing":
        dbname = "nntsc"
    dbuser = get_nntsc_config(nntsc_config, 'database', 'username')
    if dbuser == "NNTSCConfigMissing":
        dbuser = None
    dbpass = get_nntsc_config(nntsc_config, 'database', 'password')
    if dbpass == "NNTSCConfigMissing":
        dbpass = None
    cachetime = get_nntsc_config(nntsc_config, 'database', 'streamcachetime')
    if cachetime == "NNTSCConfigMissing":
        cachetime = 0

    if "NNTSCConfigError" in [dbhost, dbname, dbuser, dbpass]:
        return {}

    return {"host":dbhost, "name":dbname, "user":dbuser, "pass":dbpass,
            "cachetime":int(cachetime)}


def get_nntsc_net_config(nntsc_config):
    address = get_nntsc_config(nntsc_config, 'nntsc', 'address')
    if address == "NNTSCConfigMissing":
        address = ""
    port = get_nntsc_config_integer(nntsc_config, 'nntsc', 'port')
    if port == "NNTSCConfigMissing":
        port = 61234

    if "NNTSCConfigError" in [address, port]:
        return {}

    return {"address": address, "port": port}

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
