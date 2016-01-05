#!/usr/bin/env python

from setuptools import setup, find_packages
version = 'v1.1 Beta'
setup(
	name = 'rs_query',
	version = version,
	description = 'The rs_query module is a simple "psycopg2" wrapper for quering and interacting with Amazons Redshift.',
	author = 'kod5kod Datuman',
	url = 'https://github.com/kod5kod',
	include_package_data = True,
	packages = find_packages(),
	install_requires = [
		]
)





