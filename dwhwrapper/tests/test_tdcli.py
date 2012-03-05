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
#    test_tdcli.py - unit testing for tdcli.py

import random
import struct
import unittest
import tdcli
import os
import ctypes


class TestDBCArea(unittest.TestCase):
	"""unit testing of TestDBCArea"""

	def setUp(self):
		self.dbc = tdcli.dbc_connection()

	def test_dbc_init(self):
		"""cliv2 lib"""
		
		self.assertEqual(self.dbc.dbcarea.total_len,640)
		
		self.assertEqual(self.dbc.result.value,self.dbc.EM_OK)
		
		#Check a few options that are setup as defaults
		self.assertEqual(self.dbc.dbcarea.req_proc_opt,'P')
		self.assertEqual(self.dbc.dbcarea.parcel_mode,'Y')
		self.assertEqual(self.dbc.dbcarea.resp_mode,'I')
		
		self.assertNotEqual(self.dbc.dbcarea.tell_about_crash,'N')

class TestPrepParcelHandling(unittest.TestCase):
	"""test the PrepInfoColumn class"""
	
	def create_invalid_prepinfocolumn(self,type,length):
		
		data = struct.pack('HH',type,length)
		self.assertEqual(len(data),4)
		
		tdcli.PrepInfoColumn(data)
	
	def test_unknown_type(self):
		"""Disallow unknown types"""		
		#123 is a made up type
		self.assertRaises(TypeError,self.create_invalid_prepinfocolumn,123,0)
		#696 is LONGVARBYTE which is real, but not supported
		self.assertRaises(ValueError,self.create_invalid_prepinfocolumn,696,0)
		
	def test_varchar_decode(self):
		"""PrepInfoColumn"""
		
		data_type=449		#Varchar
		data_len=256
		col_name='TestingColumn this is a test'
		column_format='TestingFormat a very simple test'
		column_title='TestingTitle let us hope it passes!'
		
		data = struct.pack('=HHH{0}sH{1}sH{2}s'. \
						   format(len(col_name),
								len(column_format),
								len(column_title)
								),
							data_type,data_len,
							len(col_name),col_name,
							len(column_format),column_format,
							len(column_title),column_title
							)
		
		#raise Exception('\n'.join(d for d in data))
		pic = tdcli.PrepInfoColumn(data)
		
		self.assertEqual(pic.data_type_name,'VARCHAR')
		self.assertNotEqual(pic.data_type_allows_nulls,False)
		self.assertEqual(pic.pic_length,10 + len(col_name)+len(column_format)+len(column_title))
		self.assertEqual(pic.col_name,col_name)
		self.assertEqual(pic.column_format,column_format)
		self.assertEqual(pic.column_title,column_title)

def ISO8859(length):
	string = ''
	characters = []
	for x in xrange(32,127):
		characters.append(x)
	for x in xrange(160,256):
		characters.append(x)
	
	while length > 0 :
		string = "{0}{1}".format(string,chr(random.choice(characters)))
		length -=1
		
	return string


class TestIndicData(unittest.TestCase):
	
	def setUp(self):
		
		self.columns = random.randint(0,320)
		self.indic_data = tdcli.indic_data(self.columns)
		
		self.nulls = []
		
		for i in range(0,self.columns):
			
			if bool(random.randint(0,1))is True:
				self.nulls.append(True)
			else:
				self.nulls.append(False)
				
		self.assertEqual(self.columns,len(self.nulls))
		
	def test_pack_unpack(self):
		"""IndicData"""
		
		bytes = self.indic_data.pack(self.nulls)
		
		unpacker = tdcli.indic_data(len(self.nulls))
		unpacked_nulls = unpacker.unpack(bytes)
		
		self.assertEqual(len(self.nulls),len(unpacked_nulls))
		
		self.assertEqual(unpacked_nulls,self.nulls)
		

class TestTDTypes(unittest.TestCase):
	
	
	def pack_unpack(self,ddf,input):
		
		type_class = getattr(tdcli,'type_{0}'.format(ddf['Type'].lower()))(ddf)
		
		rph = tdcli.row_pack_handler()
		
		type_class.pack(rph,input)
		
		rph.define_null(False)
		
		data = rph.pack_row(1)
		
		# format is
		# 2 bytes - row length
		# .. bytes - row data
		
		row_length = struct.unpack('H',data[:2])[0]
		ruh = tdcli.row_unpack_handler(data[2:],1)
		
		td_types = []
		unpack_type = getattr(tdcli,'type_{0}'.format(ddf['Type'].lower()))(ddf)
		td_types.append(unpack_type)
		
		row_items = ruh.unpack_row(td_types,data[2:],row_length)
		
		self.assertEqual(len(row_items),1)
		output = row_items[0]
		self.assertNotEqual(output,None)
		self.assertEqual(input,output)
	
	def dummy_ddf(self,type,length):
		
		return {'Name': 'TestVC', 'Format': 'X({0})'.format(length),
			   'Nulls': True, 'Len': length,
			   'Title': 'TestVC', 'Type': type}
		
	def test_xtra(self):
		"""extra type testing for edge cases"""
		
		for type in tdcli.SUPPORTED_TYPES:
			type_class = getattr(tdcli,'type_{0}'.format(type.lower()))
			for i in range(0,random.randint(10,30)):
				getattr(self,'test_{0}'.format(type.lower()))()
	
	def gen_varchar(self):
		#
		# WARNING - we assume we're working with LATIN character set ISO8859-1
		#
		#
		length = random.randint(0,64000)		
		string = ISO8859(length)
		
		ddf = self.dummy_ddf('VARCHAR',length)
		
		return ddf,string
		
			
	def test_varchar(self):
		"""varchar"""
		self.pack_unpack(*self.gen_varchar())
	
	def gen_char(self):
		
		length = random.randint(0,64000)
		string = ISO8859(length-random.randint(0,length)).ljust(length)
		
		ddf = self.dummy_ddf('CHAR',length)
		
		return ddf,string
		
	def test_char(self):
		"""char"""
		self.pack_unpack(*self.gen_char())
		
	def gen_decimal(self):
		#lh digits	bytes
		#	1 - 2 : 1 	byte
		#	3 - 4 : 2 	bytes
		#	5 - 9 : 4 	bytes
		#	10- 18: 8 	bytes
		#	19- 38: 16	bytes
		
		precision = random.randint(1,18)
		if precision > 1:
			scale = precision - random.randint(0,precision)
		else:
			scale = 0
		
		ddf = self.dummy_ddf('DECIMAL'.format(precision,scale),(precision,scale))
		if random.randint(0,1) == 1:
			neg = '-'
		else:
			neg = ''
		lhs = str(random.randint(100000000000000000,900000000000000000))[:precision-scale]
		rhs = str(random.randint(100000000000000000,900000000000000000))[:scale]
		
		if scale >0:
			dec = '{2}{0}.{1}'.format(lhs,rhs,neg)
		else:
			dec = '{1}{0}'.format(lhs,neg)
		
		return ddf,dec
	def test_decimal(self):
		"""decimal"""
		self.pack_unpack(*self.gen_decimal())
	
	def gen_floats(self):
		"""float"""
		
		float_size = 8
		
		ddf = self.dummy_ddf('FLOAT',float_size)
		
		floats = { 'min':float('4.9406564584124654E-324')
		 ,'max':float('1.7976931348623157E+308')
		 ,'zero':float('0')
		 ,'neg_z':float('-0')
		 ,'inf':float('inf')
		 ,'neginf':float('-inf')
		 #,'nan':float('nan')
		 
		 }
		
		floats['random'] = random.uniform(floats['min'],floats['max'])
		
		return ddf,floats
	
	def gen_float(self):
		
		ddf,floats = self.gen_floats()
		options=[]
		for f in floats:
			options.append(f)
		return ddf,floats[random.choice(options)]
		
	def test_float(self):
		"""floats"""
		
		ddf,floats = self.gen_floats()
		for f in floats:
			self.pack_unpack(ddf,floats[f])
		
	
	def int_range(self,ddf,min_size,max_size):
		
		self.pack_unpack(ddf,random.randint(min_size,max_size))
		self.pack_unpack(ddf,min_size)
		self.pack_unpack(ddf,max_size)
	
	def gen_integer(self):
		
		ddf = self.dummy_ddf('INTEGER',4)
		return ddf,random.randint(-2147483648,2147483647)
	
	def test_integer(self):
		"""integer"""
		ddf = self.dummy_ddf('INTEGER',4)
		self.int_range(ddf,-2147483648,2147483647)
	
	def gen_smallint(self):
		
		ddf=self.dummy_ddf('SMALLINT',2)
		return ddf,random.randint(-32768,32767)
	
	def test_smallint(self):
		"""smallint"""
		ddf = self.dummy_ddf('SMALLINT',2)
		self.int_range(ddf,-32768,32767)
	
	def gen_byteint(self):
		ddf=self.dummy_ddf('BYTEINT',1)
		return ddf,random.randint(-128,127)
		
	def test_byteint(self):
		"""byteint"""
		ddf = self.dummy_ddf('BYTEINT',1)
		self.int_range(ddf,-128,127)
	
	def gen_date(self):
		ddf = self.dummy_ddf('DATE',None)
		
		year = random.randint(1,9999)
		month = random.randint(1,12)
		day = random.randint(1,31)
		
		return ddf,'{0:04}-{1:02}-{2:02}'.format(year,month,day)
		
	def test_date(self):
		"""date"""
		
		self.pack_unpack(*self.gen_date())
		
	def test_multirow(self):
		"""pack/unpack multiple rows and columns"""
		
		n_rows = random.randint(1,10)
		n_columns = random.randint(1,100)
		i=0
		cols=[]
		prev_type= type=None
		
		for c in range(0,n_columns):
			
			while type == prev_type:
				type = random.choice(tdcli.SUPPORTED_TYPES)
			
			cols.append(type)
			prev_type = type
			
		
		while i < n_rows:
			
			rph = tdcli.row_pack_handler()
			td_types=[]
			items=[]
			row_size = 0
			
			for c in range(0,n_columns):
				
				ddf,data = getattr(self,'gen_{0}'.format(cols[c].lower()))()
				
				td_type = getattr(tdcli,'type_{0}'.format(cols[c].lower()))
				null = bool(random.randint(0,1))
				instance = td_type(ddf)
				
				if ddf['Type'] in ['CHAR','VARCHAR']:
					l = len(data)
				else:
					l = 10
				
				if rph.row_len + l + instance.data_length + len(items) > 65534:
					self.assertRaises(OverflowError,rph.pack,instance,data)
				else:
					rph.pack(instance,data)
					td_types.append(instance)
					rph.define_null(null)
					
					if null is True:
						items.append(None)
					else:
						items.append(data)
				
			row_data = rph.pack_row(len(items))
				
			row_length = struct.unpack('H',row_data[:2])[0]
			
			self.assertTrue(row_length < 65535)
			
			ruh = tdcli.row_unpack_handler(row_data[2:],len(items))
			row_items = ruh.unpack_row(td_types,row_data[2:],row_length)
			
			for input in items:
				output = row_items.pop(0)
				self.assertEqual(input,output)
				
			i += 1
			
			

if __name__ == '__main__':
	unittest.main()
