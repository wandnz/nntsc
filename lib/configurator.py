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


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
