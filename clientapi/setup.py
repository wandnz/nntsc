#!/usr/bin/env python

import sys

from pkg_resources import Requirement, resource_filename

try:
        from setuptools import setup
except ImportError:
        from distutils.core import setup

setup(name="libnntsc-client",
	version="1.6",
	description="Client API for Nathan\'s Network Time Series Collector",
	author = "Shane Alcock",
	author_email = "contact@wand.net.nz",
	url="http://www.wand.net.nz",
	packages=['libnntscclient'],
	package_dir = {
		'libnntscclient':'libnntscclient'
	}
)
