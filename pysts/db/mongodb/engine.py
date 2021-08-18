import pandas as pd
import numpy as np
import json
from pymongo import UpdateMany
import mongoengine
from mongoengine.queryset.queryset import QuerySet
from mongoengine.base.document import BaseDocument
from pysts.utils.utils import to_json_serializable

from pysts.utils.utils import create_logger
logger = create_logger('MONGOENGINE')

connect=mongoengine.connect
Document=mongoengine.DynamicDocument

#Add to_df to querysets (also for property .file)
def to_df(self,exclude=None):
    if exclude is None: exclude=['_id']
    return pd.DataFrame.from_records(json.loads(self.to_json()),exclude=exclude)

def _get_updates(self,*args,**kwargs):
    kwargs.update(dict(pair for d in args if isinstance(d,dict) for pair in d.items()))
    updates={}
    for key,val in kwargs.items():
        if not key.startswith('$'):
            if '$set' not in updates:
                updates['$set']={}
            updates['$set'][key]=val
        else:
            updates[key]=val
    return updates

def update_or_create(self,query=None,*args,files=None,unique_keys=None,**kwargs):
    if query is None: query=[]
    if not isinstance(query,list):
        query=[query]

    #get updates
    updates=self._get_updates(*args,**kwargs)

    #Process dataframe tables (files)
    if files is not None:
        if type(files) not in [list,tuple]:
            files=[files]
        for file in files:
            cur_meta={}
            if type(file) in [list,tuple] and len(file)==2 and isinstance(file[0],pd.DataFrame) and isinstance(file[1],dict):
                cur_meta=file[1]
                file=file[0]
            assert isinstance(file,pd.DataFrame), "Files should either be a list of dataframes or a list of [(DataFrame,{metadata}),...]"
            rows=self.df_to_records(file,**cur_meta)
            query.extend(rows)

    #Create operation
    db_collection=self._document._get_collection()

    if len(query)==0 and len(updates)>0:
        if '$set' in updates and len(updates)==1: #Only insert one
            result = db_collection.insert_one(updates['$set'])
            ids = [result.inserted_id]
        else:
            q=updates.pop('$set') if '$set' in updates else {} #Update many based on $set as the query or update everything
            db_collection.update_many(q,updates,upsert=True);
            ids = [x['_id'] for x in db_collection.find(q,projection='_id')]

    elif len(query)>0 and len(updates)==0: #Insert many
        result = db_collection.insert_many(query)
        ids = result.inserted_ids

    elif len(query)>0 and len(updates)>0: #Bulk write update many
        ops=[];combined_query={'$or':[]}
        if unique_keys is None:
            unique_keys=[key for key,val in query[0].items() if type(val) not in [list,tuple,np.ndarray]]
        for cur_query in query:
            ops.append(UpdateMany(cur_query, updates,upsert=True))
            combined_query['$or'].append({key:val for key,val in cur_query.items() if key in unique_keys})

        result=db_collection.bulk_write(ops)
        logger.info(f'update_or_create: performing bulk_write with {len(ops)} ops produced result = {str(result.bulk_api_result)[:50]}')

        ids=[x['_id'] for x in db_collection.find(combined_query,projection='_id')]
    else:
        raise Exception(f'Nothing to update or create: query={query}; and updates={updates}')

    res=self._document.objects(id__in=ids)
    logger.info(f'update_or_create: after running returning {len(res)} results')
    return res

def df_to_records(self,df,**metadata):
    df=df.reset_index()
    rows=df.to_dict(orient='records')
    meta_rows=[{**metadata,**data} for data in rows]
    return to_json_serializable(meta_rows)

def store_df(self,df,**metadata):
    instances = [self._document(**x) for x in self.df_to_records(df,**metadata)]
    res=self._document.objects.insert(instances)
    return res

QuerySet.to_df=QuerySet.file=to_df
QuerySet._get_updates = _get_updates
QuerySet.update_or_create = update_or_create
QuerySet.df_to_records = df_to_records
QuerySet.store_df = store_df

#Convinient class for all fields
class Fields(object):
    def __init__(self):
        for prop in dir(mongoengine):
            if prop.endswith('Field'):
                setattr(self,prop,getattr(mongoengine,prop))
fields=Fields()

@property
def properties(self):
    props=json.loads(self.to_json())
    props['_id']=props['_id']['$oid']
    return props

@property
def get_id(self):
    return self.id

BaseDocument.properties=properties
BaseDocument._id = get_id
