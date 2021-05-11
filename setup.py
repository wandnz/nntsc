#!/usr/bin/env python3
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

try:
        from setuptools import setup
except ImportError:
        from distutils.core import setup

requires = [
        'rrdtool', 'psycopg2>=2.5', 'pika>=0.9.12,<0.11.0', 'daemon',
	'libnntsc-client', 'pylibmc', 'influxdb>=2.12.0', 'requests'
]

if sys.version_info < (2, 7):
        requires.append('argparse')

setup(name="nntsc",
	version="2.27",
	description='Nathan\'s Network Time Series Collector',
        author='Shane Alcock, Brendon Jones',
        author_email='contact@wand.net.nz',
        url='http://www.wand.net.nz',
	scripts=['build_nntsc_db', 'nntsc'],
	packages=['libnntsc', 'libnntsc.parsers'],
	install_requires = requires,
        tests_require = ["mock"],
        test_suite="tests",
	package_dir = {
		'libnntsc':'libnntsc',
		'libnntsc.parsers':'libnntsc/parsers',
	},
)
