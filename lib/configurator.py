import ConfigParser, sys


def load_nntsc_config(filename):
    # load in config file with database settings
    nntsc_config = ConfigParser.SafeConfigParser()

    # add some default values
    nntsc_config.add_section('multicast')
    nntsc_config.set('multicast','enabled','False')
    nntsc_config.set('multicast','group','224.1.1.1')
    nntsc_config.set('multicast','port','5007')

    if nntsc_config.read([filename]) == []:
	    print >> sys.stderr, "Failed to load config file: %s" % (filename)
	    return 0

    return nntsc_config 


def get_nntsc_config_bool(nntsc_config, section, option):

    if nntsc_config == 0:
        print >> sys.stderr, "Attempted to get a config option after loading failed!"
        return "NNTSCConfigError"

    try:
        result = nntsc_config.getboolean(section, option)
    except ConfigParser.NoSectionError:
        print >> sys.stderr, "The section '%s' does not exist in the config file" % (section)
        return "NNTSCConfigError"
    except ConfigParser.NoOptionError:
        print >> sys.stderr, "The option '%s' does not exist in section '%s' from the config file" % (option, section)
        return "NNTSCConfigError"
    except ConfigParser.ValueError:
        print >> sys.stderr, "The option '%s' in section '%s' does not have a boolean value" % (option, section)
        print >> sys.stderr, "Suitable values are 'true', '1', 'on', 'yes', 'false', 0, 'off', or 'no'"
        return "NNTSCConfigError"

    return result

def get_nntsc_config(nntsc_config, section, option):

    if nntsc_config == 0:
        print >> sys.stderr, "Attempted to get a config option before loading was complete!"
        return "NNTSCConfigError"

    try:
        result = nntsc_config.get(section, option)
    except ConfigParser.NoSectionError:
        print >> sys.stderr, "The section '%s' does not exist in the config file" % (section)
        return "NNTSCConfigError"
    except ConfigParser.NoOptionError:
        print >> sys.stderr, "The option '%s' does not exist in section '%s' from the config file" % (option, section)
        return "NNTSCConfigError"

    return result

def get_nntsc_db_config(nntsc_config):
	
    dbhost = get_nntsc_config(nntsc_config, 'database', 'host')
    if dbhost == "NNTSCConfigError":
        return {}
    dbname = get_nntsc_config(nntsc_config, 'database', 'database')
    if dbname == "NNTSCConfigError":
        return {}
    dbuser = get_nntsc_config(nntsc_config, 'database', 'username')
    if dbuser == "NNTSCConfigError":
        return {}
    dbpass = get_nntsc_config(nntsc_config, 'database', 'password')
    if dbpass == "NNTSCConfigError":
        return {}
	
    return {"host":dbhost, "name":dbname, "user":dbuser, "pass":dbpass}


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
