from pymongo.cursor import Cursor
from pymongo import MongoClient
import gridfs

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
