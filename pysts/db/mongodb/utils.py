from pymongo.cursor import Cursor
from pymongo import MongoClient
import io
import pickle
import os

def get_client(url='l3-37:27017/',username=None,password=None,srv=False):
    prefix='mongodb'
    if srv: prefix+='+srv'
    if username and password:
        conn_str= f'{prefix}://{username}:{password}@{url}'
    else:
        conn_str=f'{prefix}://{url}'
    return MongoClient(conn_str)

def to_list(objs,field=None,query=None):
    if not isinstance(objs,Cursor):
        if query is None: query={}
        objs=objs.find(query)
    if field is None:
        return list(objs)
    return [getattr(x,field) if hasattr(x,field) else x[field] for x in objs]

def to_bytesio(obj):
    buffer=io.BytesIO()
    pos=buffer.tell()
    pickle.dump(obj,buffer,pickle.HIGHEST_PROTOCOL)
    buffer.seek(pos)
    return buffer

class BytesIOWrapper(io.BufferedReader):
    """Wrap a buffered bytes stream over TextIOBase string stream."""

    def __init__(self, text_io_buffer, encoding=None, errors=None, **kwargs):
        super(BytesIOWrapper, self).__init__(text_io_buffer, **kwargs)
        self.encoding = encoding or (text_io_buffer.encoding if hasattr(text_io_buffer,'encoding') else None) or 'utf-8'
        self.errors = errors or (text_io_buffer.errors if hasattr(text_io_buffer,'errors') else None) or 'strict'

    def _encoding_call(self, method_name, *args, **kwargs):
        raw_method = getattr(self.raw, method_name)
        val = raw_method(*args, **kwargs)
        return val.encode(self.encoding, errors=self.errors)

    def read(self, size=-1):
        return self._encoding_call('read', size)

    def read1(self, size=-1):
        return self._encoding_call('read1', size)

    def peek(self, size=-1):
        return self._encoding_call('peek', size)

class StringIOWrapper(BytesIOWrapper):
    """Wrap a buffered text stream over BytesIOBase stream."""

    def _encoding_call(self, method_name, *args, **kwargs):
        raw_method = getattr(self.raw, method_name)
        val = raw_method(*args, **kwargs)
        try:
            res=val.decode(self.encoding, errors=self.errors)
        except:
            res=val.decode('latin1', errors=self.errors)
        return res

def obj_to_buffer(obj):
    buffer=None
    info={}
    if isinstance(obj,str) and os.path.exists(obj):
        with open(obj,'rb') as f:
            buffer = io.BytesIO(f.read())
        filename=os.path.basename(obj)
        modified=os.path.getmtime(obj)
        info={'filename':filename,'modified':modified,'obj_type':'file'}
    elif isinstance(obj,io.BytesIO):
        obj.seek(0)
        buffer=obj
        info['obj_type']='bytesio'
    elif isinstance(obj,io.StringIO):
        obj.seek(0)
        buffer=BytesIOWrapper(obj)
        info['obj_type']='stringio'
    else:
        buffer=to_bytesio(obj)
        info['obj_type']='obj'
    return buffer,info

def buffer_to_obj(buffer,*args,**kwargs):
    obj_type=kwargs['obj_type'] if 'obj_type' in kwargs else (args[0] if len(args)>0 else None)
    elif obj_type=='stringio':
        obj=StringIOWrapper(buffer)
    elif obj_type=='obj':
        obj=pickle.load(buffer)
    else:
        obj=buffer

    return obj
