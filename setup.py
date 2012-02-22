#!/usr/bin/env python
#
# 	 dwhwrapper - cli wrapper for Teradata data warehouse utilities (BTEQ,etc..)
#    Copyright (C) 2012 Felix Barbalet, Corporate Analytics, Australian Taxation Office, Commonwealth of Australia
#	 
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#    setup.py - customed distutil install/setup script for dwhwrapper

from distutils.core import setup,Command
from distutils.command.build_py import build_py as _build_py
from distutils.command.install_scripts import install_scripts as _install_scripts
from distutils.command.install_lib import install_lib as _install_lib
from distutils.command.sdist import sdist as _sdist


# header for dwhwrapper/dbcarea.py which for the generated dbcarea.py file

dbcarea_header = """
#!/usr/bin/env python
#
# 	 dwhwrapper - cli wrapper for Teradata data warehouse utilities (BTEQ,etc..)
#    Copyright (C) 2012 Felix Barbalet, Corporate Analytics, Australian Taxation Office, Commonwealth of Australia
#	 
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#    dbcarea.py autogenerated by setup.py upon testing/build
#    python ctypes version of dbcarea C structure

import ctypes

class dbc_area(ctypes.Structure):
	"Pythonified dbcarea from dbcarea.h"
	
	# these fields are generated upon install
	# the structure will differ on different architecture
	
	_fields_ =[
"""

class gen_dbcarea():
	user_options=[]
	
	def initialize_options(self):
		pass
	
	def finalize_options(self):
		pass
			  
	


class dwhwrapper_custom_setup(_build_py):
	
	def gen_struct(self):
	
		import os,sys
		dbcarea_file = os.path.join(os.getcwd(),'dwhwrapper/dbcarea.py')
		print 'Generating {0}'.format(dbcarea_file)
		from cstruct_from_c import generate_struct
		
		struct = generate_struct('dbcarea.h','DBCAREA')
		
		
		
		with file(dbcarea_file,'w') as dbcarea:
		
			dbcarea.write(dbcarea_header)
			for s in struct:
				dbcarea.write("\t\t{0}\n".format(s))
			
			dbcarea.write("\t]\n\n")
			
	def run(self):
		import sys,subprocess,os
		self.gen_struct()
		print 'Initiating unit tests...'
		sys.path.append(os.path.join(sys.path[0],'dwhwrapper'))
		import unittest
		import tests.test_tdcli
		
		suite = unittest.TestLoader().loadTestsFromModule(tests.test_tdcli)
		
		unittest.TextTestRunner(verbosity=2).run(suite)
		
		_build_py.run(self)
		
class custom_install(_install_scripts):
	
	def run(self):
		
		_install_scripts.run(self)
		
		import os
		basedir = self.install_dir
		
		dwh_script = os.path.join(basedir,'dwh')
		
		for n in ['dwhget','dwhput','dwhtable']:
			sym = os.path.join(basedir,n)
			if os.path.exists(sym):
				os.unlink(sym)
			print 'linking {0} -> {1}'.format(sym,dwh_script)
			os.symlink(dwh_script,sym)
			
class custom_install_lib(_install_lib):
	
	def install(self):
		#overwrite to chmod modules before moving them
		#code copied from distutils install_scripts
		import os
		from stat import ST_MODE
		from distutils import log
		
		outfiles = _install_lib.install(self)
		if os.name == 'posix':
		# Set the executable bits (owner, group, and world) on
			# all the scripts we just installed.
			for file in outfiles:
				if self.dry_run:
					log.info("changing mode of %s", file)
				else:
					mode = ((os.stat(file)[ST_MODE]) | 0555) & 07777
					log.info("changing mode of %s to %o", file, mode)
					os.chmod(file, mode)


class custom_sdist(_sdist):
	
	def run(self):
		self.formats = ['zip']
		_sdist.run(self)

description = "cli wrapper for Teradata data warehouse utilities (BTEQ,etc..)"

s = setup(
	name = "dwhwrapper",
	version = "1.0a",
	author = "Felix Barbalet",
	author_email = "felixb@gmail.com",
	description = description,
	long_description = description,
	license = "GNU General Public License version 3 (GPLv3)",
	platforms = ("linux-x86"),
	keywords = "teradata bteq fastexport multiload csv sql",
	url = "https://github.com/xlfe/dwhwrapper",
	scripts= ['dwhwrapper/dwh'],
	py_modules=['tdcli','dbcarea'],
	package_dir={'':'dwhwrapper'},
	classifiers=[
		"Development Status :: 3 - Alpha",
		"License :: OSI Approved :: GNU General Public License (GPL)",
#		"License :: OSI Approved :: GNU General Public License v3 (GPLv3)",  - Not yet in trove?
		"Environment :: Console",
		"Operating System :: POSIX :: Linux",
		"Programming Language :: Python :: 2.6",
		"Topic :: Database :: Front-Ends",
		"Topic :: Utilities"
	],
	requires=['python (==2.6)','cliv2'],
	cmdclass = {'build_py': dwhwrapper_custom_setup
				,'install_scripts':custom_install
				,'install_lib':custom_install_lib
				,'sdist':custom_sdist
			}
)

