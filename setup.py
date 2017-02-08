#!/usr/bin/env python

import sys

from pkg_resources import Requirement, resource_filename

try:
        from setuptools import setup
except ImportError:
        from distutils.core import setup

requires = [
        'python-rrdtool', 'psycopg2>=2.5', 'pika>=0.9.12', 'python-daemon',
	'libnntsc-client', 'pylibmc', 'influxdb'
]

if sys.version_info < (2, 7):
        requires.append('argparse')

setup(name="nntsc",
	version="2.11",
	description='Nathan\'s Network Time Series Collector',
        author='Nathan Overall, Shane Alcock',
        author_email='contact@wand.net.nz',
        url='http://www.wand.net.nz',
	scripts=['build_nntsc_db', 'nntsc'],
	packages=['libnntsc', 'libnntsc.parsers'],
	install_requires = requires,
	package_dir = {
		'libnntsc':'libnntsc',
		'libnntsc.parsers':'libnntsc/parsers',
	},
)
