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
#    tdcli.py - teradata cli interface for python
# 
#    * queries teradata to retreive datatypes returned by a query
#    * packs/unpacks common sql types from teradata binary format used by fexp

import ctypes
import struct
import csv
import string
import re

#Only the following data types are supported currently
SUPPORTED_TYPES = ('VARCHAR','CHAR','DECIMAL','FLOAT',
				   'INTEGER','SMALLINT','DATE','BYTEINT')

#dbcarea is generated upon install because it is architecture dependent
from dbcarea import dbc_area

class cli_failure(ctypes.Structure):
	_fields_ = [
			("StatementNo",ctypes.c_ushort)
		,	("Info",ctypes.c_ushort)
		,	("Code",ctypes.c_ushort)
		,	("Length",ctypes.c_ushort)
		,	("Msg",ctypes.c_char * 255)
	]

class dbc_connection:
	"""Connects to the dbc using libcliv2.so
	Code based on the Teradata sample.c and heavily modified"""
	
	EM_OK=0
	
	DBFCON=1
	DBFDSC=2
	DBFIRQ=4
	DBFFET=5
	DBFERQ=8
	
	PclSUCCESS=8
	PclFAILURE=9
	PclRECORD=10
	PclFIELD=18
	PclOK=17
	PclTITLEEND=21
	PclERROR=49
	PclDATAINFO=71
	PclPREPINFO=86
	
	REQEXHAUST=307
	
	def __init__(self):
		self.dbcarea = dbc_area()
		self.dbcarea.total_len = ctypes.sizeof(self.dbcarea)
		self.result = ctypes.c_int(self.EM_OK)
		
		self.cnta = ctypes.c_int32(0)
		self.cli = ctypes.cdll.LoadLibrary("libcliv2.so")
		
		self.cli.DBCHINI(ctypes.byref(self.result),ctypes.byref(self.cnta),ctypes.byref(self.dbcarea))
	
		if self.result.value is not self.EM_OK:
			raise Exception("Fatal error: unable to init dbcarea {0}".format(self.result))
		
		#default dbc options
		self.dbcarea.change_opts 		= 'Y'
		self.dbcarea.resp_mode 			= 'I'
		self.dbcarea.use_presence_bits 	= 'N'
		self.dbcarea.keep_resp 			= 'N'
		self.dbcarea.wait_across_crash 	= 'N'
		self.dbcarea.tell_about_crash 	= 'Y'
		self.dbcarea.loc_mode 			= 'Y'
		self.dbcarea.var_len_req 		= 'N'
		self.dbcarea.var_len_fetch 		= 'N'
		self.dbcarea.save_resp_buf 		= 'Y'
		self.dbcarea.two_resp_bufs 		= 'N'
		self.dbcarea.ret_time 			= 'N'
		self.dbcarea.parcel_mode 		= 'Y'
		self.dbcarea.wait_for_resp 		= 'Y'
		self.dbcarea.req_proc_opt 		= 'P'
		
		#support for extra-large response parcels
		self.dbcarea.maximum_parcel		= 'H'

	def dbchcl(self,type):
		"""wrapper to call the DBCHCL function -
		don't need to pass much because it's all setup in dbcarea"""
		
		self.result = ctypes.c_int(self.EM_OK)
		self.dbcarea.func=type
		
		self.cli.DBCHCL(ctypes.byref(self.result), ctypes.byref(self.cnta), ctypes.byref(self.dbcarea))

		return self.result.value

	def logon(self,dbc_name,uid,password):
		"""logon to the dbc -
		this function should be called before any other functions"""
		
		lstr = '{0}/{1},{2}\n '.format(dbc_name,uid,password)
		self.dbcarea.logon_ptr = ctypes.cast(ctypes.c_char_p(lstr),ctypes.POINTER(ctypes.c_char))
		self.dbcarea.logon_len = len(lstr)

		self.result = self.dbchcl(self.DBFCON)
		
		if self.result is not self.EM_OK:
			raise Exception("Fatal error: unable to logon to DBC {0}".format(self.dbcarea.msg_text))
		
		self.fetch_request()		
		self.close_request()
		
		return True
	
	def fetch_request(self,return_on_parcels=[]):
		"""fetches responses from the dbc -
		continues until one of the parcels in return_on_parcels is seen"""
		
		while True:
			
			self.result = self.dbchcl(self.DBFFET)
			
			if self.result == self.REQEXHAUST:
				return self.REQEXHAUST
			
			elif self.result != self.EM_OK:
				raise Exception("Fetch failed {0}".format(self.dbcarea.msg_text))
				
			if self.dbcarea.fet_parcel_flavor in return_on_parcels:
				return self.dbcarea.fet_parcel_flavor
			
			elif self.dbcarea.fet_parcel_flavor == self.PclFAILURE or \
						self.dbcarea.fet_parcel_flavor == self.PclERROR:						
				
				cf = ctypes.cast(self.dbcarea.fet_data_ptr,ctypes.POINTER(cli_failure))[0]
				raise Exception("STATEMENT:{0} ERR:{1} {2}".format(cf.StatementNo,cf.Code,cf.Msg[:cf.Length]))
			
			#print 'Parcel Flavour {0}'.format(self.dbcarea.fet_parcel_flavor)
				
	def close_request(self):
		
		self.result = self.dbchcl(self.DBFERQ)
		
		if self.result != self.EM_OK:
			raise Exception("End req. failed {0}".format(dbc.msg_text))
	
	def submit_sql_request(self,sql):
	
		self.dbcarea.req_ptr = ctypes.cast(ctypes.c_char_p(sql),ctypes.POINTER(ctypes.c_char))
		self.dbcarea.req_len = len(sql)
		
		self.result = self.dbchcl(self.DBFIRQ)
		
		if self.result != self.EM_OK:
			raise Exception("Init. request failed {0}".format(self.dbcarea.msg_text))
			
	
	def get_prepinfo_parcel(self,sql):
		
		self.submit_sql_request(sql)
		
		self.result = self.fetch_request([self.PclPREPINFO])
		
		if self.result != self.PclPREPINFO:
			raise Exception("PclPrepInfo not returned {0}".format(self.dbcarea.msg_text))
		
		rlen = self.dbcarea.fet_ret_data_len
		parcel = ctypes.create_string_buffer(rlen)
		ctypes.memmove(parcel,ctypes.cast(self.dbcarea.fet_data_ptr,ctypes.c_void_p),rlen)
		
		self.fetch_request()		
		self.close_request()
		
		return (parcel,rlen)

	def logout(self):
		
		self.result = self.dbchcl(self.DBFDSC)
		
		if self.result != self.EM_OK:
			raise Exception("Disconnect failed {0}".format(self.dbcarea.msg_text))
			
		self.result = ctypes.c_int(self.EM_OK)
		
		self.cli.DBCHCLN(ctypes.byref(self.result),ctypes.byref(self.cnta))
		
		if self.result.value != self.EM_OK:
			raise Exception("Cleanup failed {0}".format(self.dbcarea.msg_text))
			
class PrepInfoColumn:
	"""Parses a PrepInfo data column"""
	data_type=None
	data_len=None
	data_type_name=None
	data_type_allows_nulls=None
	col_name=None
	column_format=None
	column_title=None
	pic_length=None
	
	
	#Types as defined by teradata, and whether they allow null values
	#Note we don't support all of these teradata types
	#- we check SUPPORTED_TYPES later..
	
	types = {448:["VARCHAR",False]
			,449:["VARCHAR",True]
			,452:["CHAR",False]
			,453:["CHAR",True]
			
			#TIME,TIMESTAMP and INTERVAL are all represented as CHAR...
			#,453:["TIME",True]
			#,453:["TIMESTAMP",True]
			#,453:["INTERVAL",True]
			
			,456:["LONGVARCHAR",False]
			,457:["LONGVARCHAR",True]
			,464:["VARGRAPHIC",False]
			,465:["VARGRAPHIC",True]
			,468:["FixedGRAPHIC",False]
			,469:["FixedGRAPHIC",True]
			,472:["LONGVARGRAPHIC",False]
			,473:["LONGVARGRAPHIC",True]
			,484:["DECIMAL",False]
			,485:["DECIMAL",True]
			,480:["FLOAT",False]
			,481:["FLOAT",True]
			,496:["INTEGER",False]
			,497:["INTEGER",True]
			,500:["SMALLINT",False]
			,501:["SMALLINT",True]
			,752:["DATE",False]
			,753:["DATE",True]
			,756:["BYTEINT",False]
			,757:["BYTEINT",True]
			,688:["VARBYTE",False]
			,689:["VARBYTE",True]
			,692:["BYTE",False]
			,693:["BYTE",True]
			,696:["LONGVARBYTE",False]
			,697:["LONGVARBYTE",True]}
	
	def __init__(self,data):
		
		self.data_type,self.data_len = struct.unpack('HH',data[:4])
		
		for k,v in self.types.iteritems():
			if k == self.data_type:
				self.data_type_name = v[0]
				self.data_type_allows_nulls = v[1]
				break
		
		if self.data_type_name is None:
			raise TypeError('Unknown data type {0}'.format(self.data_type))
		
		if self.data_type_name not in SUPPORTED_TYPES:
			raise ValueError('Unsuppported data type {0}'.format(self.data_type_name))
			
		if self.data_type in [484,485]: #Decimals
			dec = struct.unpack('BB',data[2:4])
			self.data_len = [dec[1],dec[0]]
			
		cn_len = struct.unpack_from('H',data,4)[0]
		self.col_name = data[6:6+cn_len]
		
		cf_len = struct.unpack_from('H',data,6+cn_len)[0]
		self.column_format = data[8+cn_len:8+cn_len+cf_len]
		
		ct_len = struct.unpack_from('H',data,8+cn_len+cf_len)[0]
		self.column_title = data[10+cn_len+cf_len:10+cn_len+cf_len+ct_len]
		
		self.pic_length = 4 + 6 + cn_len + cf_len + ct_len					
		
		return		
				
def get_ddf(c_sql,dbc,uid,pw,args):
	"""Connects to 'dbc' using credentials 'uid' and 'pw', and runs a
	PrepInfoQuery using the sql query supplied
	
	returns a dictionary containing:
	- sql_query : the query
	- cost_est : the cost estimate
	- ddf : data definition field(s)
			These define the data types, etc of the columns.
			Each DDF is a dict containing:
			- Title		: Column title (if defined)
			- Type 		: Data type
			- Len 		: Data type length
			- Nulls		: Allows Nulls
			- Name		: Column name
			- Format 	: SQL format description
			"""

	if args.verbose:
		print '--- opening connection to retreive PrepInfoParcel'
		
	dbcc = dbc_connection()
	if args.verbose:
		print '--- logon'
	dbcc.logon(dbc,uid,pw)
	if args.verbose is True:
		print "SQL: '{0}'".format(c_sql)
	parcel,plen = dbcc.get_prepinfo_parcel(c_sql)
	if args.verbose:
		print '--- logout'
	dbcc.logout()	
	del dbcc

	if plen == 0:
		raise Exception('Unable to retreive PrepInfo parcel')
	
	cost_estimate,summary_count,column_count = struct.unpack('dHH',parcel[:12])
	
	if args.verbose is True:
		print 'Query cost-estimate: {0} seconds'.format(cost_estimate)
		print 'Query summary-count: {0}'.format(summary_count)
		print 'Query column_count : {0}'.format(column_count)

	if column_count == 0:
		raise Exception("No columns found")
	
	#header is 12 bytes
	offset = 12
	ddf=[]
	
	for i in range(0,column_count):

		pic = PrepInfoColumn(parcel[offset:])
		offset = offset + pic.pic_length
		
		dat = {
				'Title'	:pic.column_title
			,	'Type'	:pic.data_type_name
			,	'Len'	:pic.data_len
			,	'Nulls'	:pic.data_type_allows_nulls
			,	'Name'	:pic.col_name
			,	'Format':pic.column_format					
			}
		
		if args.verbose is True:
			print dat
		
		ddf.append(dat)
		
	return {
		'sql_query'	:c_sql
		,'cost_est'	:cost_estimate
		,'ddf'		:ddf
	}

class td_type:
	"""base class for teradata binary types"""
	
	fd = None					#stores the field description
	data_type = None			#stores the field type for cstruct calls
	data_length = None			#stores the field length for cstruct calls
	
	def __init__(self,field_defintion):
		self.fd = field_defintion
		self.data_type = self.data_length = None
		
	def unpack(self,row_data,offset):
		data = struct.unpack_from(self.data_type,row_data,offset)[0]
		return data,self.data_length
		
class type_float(td_type):
	
	def __init__(self,field_definition):
		td_type.__init__(self,field_definition)
		
		self.data_length = self.fd['Len']
		
		dlmap = {4:'f',8:'d'}
		assert(self.data_length == 8) #Teradata stores floats as 8bytes only?
		self.data_type = dlmap[self.data_length]
		
	def pack(self,rph,r):
		rph.add_data(td_type=self,data=float(r))
	
class type_integer(td_type):
	
	def __init__(self,field_definition):
		td_type.__init__(self,field_definition)
		self.data_type = 'i'
		self.data_length = 4
		
	def pack(self,rph,r):
		rph.add_data(td_type=self,data=int(r))
		
class type_smallint(type_integer):
	"""like an integer, but half the size"""
	
	def __init__(self,field_definition):
		type_integer.__init__(self,field_definition)
		self.data_type = 'h'
		self.data_length = 2
	
class type_byteint(type_integer):
	"""like an integer, but a quarter of the size"""
	
	def __init__(self,field_definition):
		type_integer.__init__(self,field_definition)
		self.data_type = 'b'
		self.data_length = 1

class type_date(type_integer):
	"""dates are stored as integers - we overwrite the pack/unpack function"""
	
	def pack(self,rph,r):
		
		try:
			(yr,month,day)=re.compile('^[\s]*([0-9]{2,4})(?:-|/)([0-9]{1,2})(?:-|/)([0-9]{1,2})[\s]*$').match(r).groups()
		except AttributeError:
			raise AttributeError("Dates from column '{0}' are not in ISO Standard YYYY-MM-DD format (or other recognizable form); '{1}'".format(self.fd['Name'],r))
			
		if int(month) > 12:
			raise AttributeError("Dates from column '{0}' must be in ISO Standard YYYY-MM-DD format, not '{1}'".format(self.fd['Name'],r))
		
			
			
		if yr < 100:
			yr = yr + 2000 #assume it's this century?
			
		ri = (int(yr) - 1900) * 10000 + (int(month) * 100) + int(day)
		
		rph.add_data(td_type=self,data=ri)
	
	def unpack(self,row_data,offset):
		
		row_item,new_offset = td_type.unpack(self,row_data,offset)
		
		#(YEAR - 1900) * 10000 + (MONTH * 100) + DAY
		yr =   (row_item / 10000) + 1900
		month = row_item / 100 - (row_item / 10000) * 100
		day = 	row_item -		 (row_item / 100) 	* 100
		
		#ANSI format YYYY-MM-DD
		return '{0:04}-{1:02}-{2:02}'.format(yr,month,day),new_offset

class type_varchar(td_type):
	
	def __init__(self,field_definition):
		td_type.__init__(self,field_definition)
		self.data_type = 'H'
		self.data_length = 2
	
	def pack(self,rph,r):
				
		if len(r) > self.fd['Len']:
			raise ValueError("Column '{0}' contains a string longer than {1} characters".format(
				self.fd['Name'],self.fd['Len']))
		
		rph.add_custom_data(self.data_type,self.data_length,len(r))
		
		for i in range(0,len(r)):
			rph.add_custom_data('c',1,r[i])
	
	def unpack(self,row_data,offset):
		
		var_len,new_offset = td_type.unpack(self,row_data,offset)
		
		string_start = offset + new_offset		
		
		varchar = row_data[string_start : string_start + var_len]
		assert len(varchar) == var_len		
		return varchar,new_offset + var_len

class type_char(td_type):	
	
	def __init__(self,field_definition):
		td_type.__init__(self,field_definition)
		#self.data_type = 'c'
		self.data_type = '{0}s'.format(self.fd['Len'])
		self.data_length = self.fd['Len']
		
	def pack(self,rph,r):
				
		if len(r) > self.fd['Len']:
			raise ValueError("Column '{0}' contains a string longer than {1} characters".format(d['Name'],d['Len']))
			
		rph.add_data(self,r)
	
	def unpack(self,row_data,offset):
		
		row_item = row_data[offset : offset + self.data_length].rstrip('\0')
		return row_item,self.data_length

class type_decimal(td_type):
	"""decimal type"""
	
	def __init__(self,field_definition):
		td_type.__init__(self,field_definition)

		self.data_type,self.data_length = self.length()
	
	def length(self):
		#lh digits	bytes
		#	1 - 2 : 1 	byte
		#	3 - 4 : 2 	bytes
		#	5 - 9 : 4 	bytes
		#	10- 18: 8 	bytes
		#	19- 38: 16	bytes
		
		lhs = self.fd['Len'][0]
		
		if lhs <=2:
			return 'b',1
		
		elif lhs <=4:
			return 'h',2
		
		elif lhs <=9:
			return 'i',4
		
		elif lhs <=18:
			return 'q',8
		
		raise Exception('Decimals with precision > 18 not supported - col {0}'.format(self.fd['Name']))
		
	def pack(self,rph,r):
				
		precis = self.fd['Len'][0]
		scale = self.fd['Len'][1]
		
		#match the sign, integer and decimal portions...
		digits=False
		for c in r:
			if c in string.digits:
				digits=True
				break
		
		m=r.split('.')
		
		if len(m) > 2 or digits is False:
			raise ValueError("Unable to convert {0} to DECIMAL({1},{2}) in column '{3}'".format(r,precis,scale,self.fd['Name']))
		
		if len(m)>1:
			decp = m[1].strip()
		else:
			decp=''
			
		intp = m[0].strip()
		
		#check we dont have too many digits (ie overflow)
		dec_padding = scale - len(decp)
		int_padding = precis - scale - len(intp.translate(None,'-+'))
		
		if dec_padding < 0 or int_padding < 0:
			raise ValueError ("Overflow error converting {0} to DECIMAL({1},{2}) in column '{3}'".format(r,precis,scale,self.fd['Name']))
		
		dec_padding = string.zfill('',dec_padding)
		
		o=int(''.join(v for v in [intp,decp,dec_padding]))
		
		rph.add_data(td_type=self,data=o)
		
		
	def unpack(self,row_data,offset):
		
		row_item,new_offset = td_type.unpack(self,row_data,offset)
		
		scale = self.fd['Len'][1]
				
		if str(row_item)[:1]== '-':
			negative = '-'
		else:
			negative = ''
		
		if scale > 0:
			
			if negative == '-':
				row_item = row_item * -1
			
			lhs = str(row_item)[:- scale]
			rhs = str(row_item)[-scale:]
			
			return '{2}{0}.{1}'.format(lhs,string.zfill(rhs,scale),negative),new_offset
		else:
			return str(row_item),new_offset

class indic_data:
	"""pack/unpacker for teradata binary indicator data
	- the nth-bit indicates whether the nth column is null or not"""
	
	def __init__(self,column_count):
		self.column_count= column_count
		#indicator data length in bytes is 8bit aligned		
		if column_count % 8 == 0:
			self.indic_data_len = column_count/8
		else:
			self.indic_data_len = column_count/8 + 1
	
	def pack(self,nulls):
		"""returns a byte string in indicdata format"""
		
		n = 0
		bytes = []
		
		for i in range(0,self.indic_data_len):
			
			byte = 0
			
			for octet in range( 0, min(8,len(nulls)-n) ):
				octet = 7 - octet
				byte = byte + (nulls[n] << (octet))
				n += 1
				
			bytes.append(byte)
			
		return b''.join(chr(b) for b in bytes)
	
	def unpack(self,indic_data):
		"""Unpack a byte-string of indicator format data
		and return an array of boolean values"""
			
		assert isinstance(indic_data,type(b''))
		nulls = []
			
		for col in range(0,self.column_count):
			
			byte = ord(indic_data[col / 8])
			bit = 7 - (col % 8)
			
			is_null = (byte >> bit) & 1
			
			nulls.append(bool(is_null))
		
		return nulls

class row_pack_handler:
	"""collects the items for a row"""
	
	def __init__(self):
		self.row_len=0
		(self.format,self.data,self.nulls) = ([],[],[])
		
	def add_data(self,td_type,data):
		"""add data in format specified by data_type and data_length in td_type"""
		self.format.append(td_type.data_type)
		self.data.append(data)
		self.row_len += td_type.data_length
		
	def add_custom_data(self,data_type,data_length,data):
		"""add data in a format different from that specified in td_type"""
		self.format.append(data_type)
		self.data.append(data)
		self.row_len += data_length
	
	def insert_byte(self,offset,byte):
		"""take a single byte character and insert it at offset"""
		assert isinstance(byte,type(b''))
		assert len(byte) == 1
		self.format.insert(offset,'B')
		self.data.insert(offset,ord(byte))
		self.row_len += 1
	
	def define_null(self,null):
		self.nulls.append(bool(null))
		
	def pack(self,td_type,data):
		
		if td_type.fd['Type'] in ['CHAR','VARCHAR']:
			l = len(data)
		else:
			l = 10
		
		if self.row_len + l + td_type.data_length + len(self.nulls) > 65534:
			raise OverflowError('Maximum parcel length is 65k')
		else:
			td_type.pack(self,data)
			
	
	def pack_row(self,column_count):
		"""packs a completed row and returns the binary data"""
		
		id = indic_data(column_count)
		id_bytes = id.pack(nulls=self.nulls)
		
		assert(self.row_len < 65535)
		
		for id_byte_n in range(0,len(id_bytes)):
			self.insert_byte(id_byte_n,id_bytes[id_byte_n])
			
		assert(self.row_len < 65535)
		
		self.format.insert(0,'H')
		self.data.insert(0,self.row_len)
		
		self.format.append('B')
		self.data.append(ord('\n'))
		
		#Specify native byte order and no alignment
		self.format.insert(0,'=')		
		
		fmt = ''.join(f for f in self.format)
		return struct.pack(fmt,*self.data)

class row_unpack_handler:
	
	def __init__(self,row_data,columns):
		self.columns = columns
		id = indic_data(columns)
		self.nulls = id.unpack(row_data)
		self.offset = id.indic_data_len
	
	def unpack_row(self,td_types,row_data,row_len):
		
		row_items = []
		offset = self.offset
		
		for td_type in td_types:
			
			null = self.nulls.pop(0)
			
			row_item,new_offset = td_type.unpack(row_data,offset)
			
			offset = offset + new_offset
			if bool(null) is True:
				row_items.append(None)
			else:
				row_items.append(row_item)
			
			if offset > row_len:
				raise Exception('End of row overflow')
			
		return row_items
			
			
	
def fexp_to_csv(ddf,fexp_file,args):
	"""binary safe conversion from binary fast-export format to csv
	
	ddf - contains definitions of all fields required for conversion
	fexp_file - filename of binary fastexp file we read from
	args - arguments passed by the user (eg verbosity, output file)
	"""

	if args.use_column_titles is True:
		header_nm = 'Title'
	else:
		header_nm = 'Name'
	
	cols=[]
	td_types = []

	for fd in ddf:

		cols.append(fd[header_nm])
		try:
			#define each td_type with appropriate type_TYPE handler
			td_types.append(
				globals()['type_{0}'.format(fd['Type'].lower())](fd)
			)
		except KeyError:
			raise Exception("Unable to find handler class '{0}' for '{1}'".format(
				fd['Type'],fd['Title']))
	
	with file(args.output,'w') as out_file:
		out = csv.writer(out_file,quoting=csv.QUOTE_MINIMAL)
		
		out.writerow(cols)
		
		with open(fexp_file,'rb') as input:
			
			while True:
				
				header = input.read(2)
				if len(header) < 2:
					#EOF
					break
				
				row_len = struct.unpack('H',header)[0]
				row_data = input.read(row_len)
				
				ruh = row_unpack_handler(row_data,len(td_types))
				
				row_items = ruh.unpack_row(td_types,row_data,row_len) #fexpb.row_unpack(row_data,row_len[0])
					
				out.writerow(row_items)
				input.read(1) #End of record indicator is a single newline char..
	
def csv_to_fexp(ddf,csv_file,fexp_file,args):
	"""binary safe conversion from csv to fast-export binary format
	
	ddf - contains definitions of all fields in the csv (and potential fields not in the csv)
	csv_file - filename of csv we read from
	fexp_file - filename of fexp file we write to
	args - arguments passed by the user (eg verbosity)
	
	
	returns the actual ddfs used
	"""
	
	
	with open(csv_file,'r') as f:
		
		dict_reader = csv.DictReader(f)
		
		if args.use_column_titles is True:
			column = 'Title'
		else:
			column = 'Name'
		
		#td_types for use
		td_types=[]
		
		#available field definitions
		defined_fields = [fd[column] for fd in ddf]
		
		#compare available_fields with the headers from the csv file being read
		for field in dict_reader.fieldnames:
			if field not in defined_fields:
				raise Exception("'{0}' not defined in '{1}' - unable to continue".format(
					field,args.dest))
			else:
				for fd in ddf:
					if fd[column] == field:
						try:
							#define each td_type with appropriate type_TYPE handler
							td_types.append(
								globals()['type_{0}'.format(fd['Type'].lower())](fd)
								)
						except KeyError:
							raise Exception("Unable to find handler class '{0}' for '{1}'".format(
								fd['Type'],fd['Title']))
								
						
		with open(fexp_file,'wb') as out:
		
			for row in dict_reader:
				
				if len(row) < len(td_types):
					raise Exception('Row is missing columns')
					
				elif len(row) > len(td_types):
					raise Exception('Row has too many columns')
					
				rph = row_pack_handler()
				
				for td_type in td_types:
					
					r = row[td_type.fd[column]]
					
					if r is None or len(r) == 0:
						if td_type.fd['Type'] in ['CHAR','VARCHAR']:
							
							r=''
							rph.define_null(False)
						else:
							if td_type.fd['Nulls'] is False:
								raise ValueError('{0} has an empty value, but is defined as NON NULL'.format(
									td_type.fd['Name']))
							
							rph.add_data(td_type=td_type,data=0)
							rph.define_null(True)
							continue
						
					else:
						rph.define_null(False)
						
					#depending on the type, this calls type_TYPE.pack()
					rph.pack(td_type=td_type,data=r)
				
				out.write(rph.pack_row(len(td_types)))
			
	return [td_type.fd for td_type in td_types]
