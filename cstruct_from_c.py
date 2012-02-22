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
#    cstruct_from_c.py - conversion from a C STRUCT to a ptyhon cstruct

def generate_struct(header_file,struct_name):
    from commands import getoutput
    from sys import argv
    from sys import stderr
    from string import split
    import re

    infile = getoutput("cat /usr/include/{0} | cpp | sed '/^#/d'".format(header_file))
    lines = infile.split('\n')
    
    struct = []
    in_struct = False
    typedefs={}
    
    td = re.compile('^typedef ([a-z0-9\ \*\_]+) ([a-z0-9\_]+)[\s]*;')
    structure = re.search('typedef struct {0}(.*){0};'.format(struct_name),infile,re.DOTALL|re.MULTILINE)
    
    if structure is None:
        raise Exception('Unable to find typedef struct {0}'.format(struct_name))
    
    for l in lines:
        
        if l.strip()[:7] == "typedef":
            
            typ = td.match(l.strip().lower())
            
            if typ is not None:
                typedefs[typ.group(2)] = typ.group(1)
                #print '{0} -> {1}={2}'.format(typ.group(0),typ.group(1),typ.group(2))
                
    
    for l in structure.group(1).split('\n'):
        struct.append(l.strip())
    
    
    #make sure we reference the root type
    for t in typedefs:
        while typedefs[t] in typedefs:
            typedefs[t] = typedefs[typedefs[t]]
    
    type_mappings = {
            'long double'		:'c_longdouble'
        ,   'signed long long'	:'c_longlong'
        ,   'long long'         :'c_longlong'       #Implied
        ,	'signed long'		:'c_long'
        ,   'long'              :'c_long'           #Implied
        ,	'signed int'		:'c_int'
        ,   'int'               :'c_int'            #Implied
        ,	'signed short'		:'c_short'
        ,   'short'             :'c_short'          #Implied
        ,	'signed char'		:'c_byte'
        ,	'unsigned long long':'c_ulonglong'
        ,	'unsigned long' 	:'c_ulong'
        ,	'unsigned short'	:'c_ushort'
        ,	'unsigned char' 	:'c_ubyte'
        ,	'unsigned int'		:'c_uint'
        ,	'char *'    		:'POINTER(ctypes.c_char)'
        ,	'char'	    		:'c_char'
        ,	'double'    		:'c_double'
        ,	'float'	       		:'c_float'
        ,	'size_t'    		:'c_size_t'
        ,	'ssize_t'     		:'c_ssize_t'
        ,	'void *'		    :'c_void_p'
        ,   'int *'             :'c_void_p'         #Implied
        ,	'wchar_t *' 		:'c_wchar_p'
        ,	'wchar_t'		    :'c_wchar'
        ,	'bool'			    :'c_bool'
    }    
    
    
    for t in typedefs:
        try:
            typedefs[t] = type_mappings[typedefs[t]]
        except KeyError:
            stderr.write("No valid ctype for '{0}' (defined as '{1}')... continuing...\n".format(t,typedefs[t]))
            typedefs[t]=None
            
    
    variable_name = re.compile('[\s]*([^\s\*\[\]]+)(?:\[([\d]+)\])?;$')
    
    outp=[]
    
    for s in struct:
        
        s=s.strip()
        
        if len(s) <2:
            continue
        
        v =variable_name.search(s)
        var_name = v.group(1)
        var_len = v.group(2)
    
        
        try:
            var_type = type_mappings[s[:len(s)-len(v.group(0))].lower()]
        except:
            var_type = typedefs[s[:len(s)-len(v.group(0))].lower()]
        
        if var_len is None:    
            outp.append('("{0}",ctypes.{1}),'.format(var_name,var_type))
        else:
            outp.append('("{0}",ctypes.{1} * {2}),'.format(var_name,var_type,int(var_len)))
    
    return outp