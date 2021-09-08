import pandas as pd
import numpy as np
import json
from pymongo import UpdateMany, ReplaceOne
from pymongo.command_cursor import CommandCursor
import mongoengine
from mongoengine.queryset.queryset import QuerySet
from mongoengine.base.document import BaseDocument
from pysts.utils.utils import to_json_serializable
from datetime import datetime

from pysts.utils.utils import create_logger
logger = create_logger(__name__) #pysts.db.mongodb.engine
connect=mongoengine.connect
Document=mongoengine.DynamicDocument

def delete_dups(doc,unique_keys,keep_ids=None):
    if keep_ids is None:
        keep_ids=[]
    if isinstance(unique_keys,str):
        unique_keys=[unique_keys]
    pipeline=[
        {
            "$group": {
                "_id": {x: f"${x}" for x in unique_keys},
                "uniqueIds": { "$addToSet": "$_id" },
                "count": { "$sum": 1 }
            }
        },
        { "$match": { "count": { "$gt": 1 } } },
        {"$project": {"name" : "$uniqueIds", "_id" : 0} }
    ]
    duplicates=doc.objects.aggregate(pipeline,allowDiskUse=True)
    get_del_ids=lambda ids: del_ids[:-1] if len(del_ids:=[x for x in ids if x not in keep_ids])>1 else del_ids
    ids=[x for duplicate in duplicates for x in get_del_ids(duplicate['name'])]
    q=doc.objects(id__in=ids)
    return q.delete()

#Add to_df to querysets (also for property .file)
def command_cursor_to_df(self,**kwargs):
    return pd.DataFrame.from_records(self,**kwargs)
CommandCursor.to_df=command_cursor_to_df

#Add to_df to querysets (also for property .file)
def to_df(self,**kwargs):
    return pd.DataFrame.from_records(json.loads(self.to_json()),**kwargs)

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

def update_or_create(self,query=None,*args,files=None,unique_keys=None,max_queries=1000,return_only_ids=False,**kwargs):
    start_t = datetime.now()
    ids=[]
    if query is None: query=[]
    if not isinstance(query,list):
        query=[query]

    #get updates
    updates=self._get_updates(*args,**kwargs)
    logger.debug(f'update_or_create: Starting with query of length {len(query)}, {len(updates)} updates, and {"no" if files is None else (len(files) if type(files) in [list,tuple] else 1)} files')

    #Process dataframe tables (files)
    last_diff_t=0
    if files is not None:
        if type(files) not in [list,tuple]:
            files=[files]
        prev_seconds=0
        for file in files:
            cur_meta={}
            if type(file) in [list,tuple] and len(file)==2 and isinstance(file[0],pd.DataFrame) and isinstance(file[1],dict):
                cur_meta=file[1]
                file=file[0]
            assert isinstance(file,pd.DataFrame), "Files should either be a list of dataframes or a list of [(DataFrame,{metadata}),...]"

            total_rows=file.shape[0]
            while file.shape[0]>0:
                num_records_allowed=max_queries-len(query)
                rows=self.df_to_records(file.iloc[:num_records_allowed],**cur_meta)
                query.extend(rows)

                file=file.iloc[num_records_allowed:]
                if len(query)>=max_queries:
                    ids.extend(self.update_or_create(query=query,unique_keys=unique_keys,max_queries=max_queries,return_only_ids=True,**updates))
                    diff_seconds = (datetime.now() - start_t).total_seconds()
                    seconds_per_row=(diff_seconds-prev_seconds)/len(query)
                    logger.debug(f'update_or_create: Calling update_or_create on {len(rows)} rows out of {total_rows}: {((total_rows-file.shape[0])/total_rows):.1%} complete with {seconds_per_row*file.shape[0]:.1f} seconds remaining...')
                    prev_seconds=diff_seconds
                    del rows
                    query.clear()
        del files
        #If all query items done, then return
        if len(query)==0:
            res=ids if return_only_ids else self._document.objects(id__in=ids)
            return res

    #Create operation
    db_collection=self._document._get_collection()

    if len(query)==0 and len(updates)>0:
        if '$set' in updates and len(updates)==1: #Only insert one
            result = db_collection.insert_one(updates['$set'])
            ids.append(result.inserted_id)
            msg='insert_one'
        else:
            q=updates.pop('$set') if '$set' in updates else {} #Update many based on $set as the query or update everything
            db_collection.update_many(q,updates,upsert=True);
            ids.extend([x['_id'] for x in db_collection.find(q,projection='_id')])
            msg='update_many'

    elif len(query)>0 and len(updates)==0: #Insert many
        result = db_collection.insert_many(query)
        ids.extend(result.inserted_ids)
        msg='insert_many'

    elif len(query)>0 and len(updates)==1 and '$set' in updates: #Bulk insert many
        setq=updates['$set']
        if unique_keys is None:
            unique_keys=[key for key,val in query[0].items() if type(val) not in [list,tuple,np.ndarray]]

        ops=[]; combined_query={"$or":[]}
        for cur_query in query:
            cur_query.update(setq)
            q={}
            combined_set=False
            for key in unique_keys:
                if key in cur_query:
                    if not combined_set:
                        combined_query['$or'].append({})
                        combined_set=True
                    val=cur_query.pop(key)
                    combined_query['$or'][-1][key]=val
                    q[key]=val
            ops.append(ReplaceOne(q,cur_query,upsert=True))

        if len(ops)>0:
            result=db_collection.bulk_write(ops, ordered=False)
            cursor=self.objects.aggregate([
                {'$match':{'$or':[{'table_name':'Traces'},{'table_name':'tedmp_table'}]}},
                { '$group': { '_id': None, 'ids': { '$addToSet': "$_id" } } },
                {'$project':{'_id':0}},
            ])
            ids.extend(cursor[0]['ids'])
            #Insert new docs
            # result=db_collection.insert_many(query)
            # ids.extend(result.inserted_ids)
            # #delete duplicates
            # if len(unique_keys)>0:
            #     deleted_count=delete_dups(self._document,unique_keys,keep_ids=ids)
            #     if deleted_count>0:
            #         logger.debug(f'update_or_create: During insert_many (with updates) - deleted {deleted_count} duplicate documents from {self._document} based on the keys: {unique_keys}')

        msg='bulk ReplaceOne'
    else:
        raise Exception(f'Nothing to update or create: query={query}; and updates={updates}')

    res=ids if return_only_ids else self._document.objects(id__in=ids)
    diff_t = int((datetime.now() - start_t).total_seconds())
    logger.debug(f'update_or_create: Performing {msg} on {self._document} with {len(query)} queries and {len(updates)} updates took {diff_t-last_diff_t} seconds, and returned {len(ids)} results')
    return res

def df_to_records(self,df,keep_index=False,**metadata):
    start_t = datetime.now()
    if not 'index' in df.columns:
        df=df.reset_index(drop=not keep_index)
    rows=df.to_dict(orient='records')
    for row in rows:
        row.update(metadata)
    diff_t = int((datetime.now() - start_t).total_seconds())
    logger.debug(f'df_to_records: Converting df with shape ({df.shape}) to records took {diff_t} seconds')
    return to_json_serializable(rows)

def store_df(self,df,keep_index=False,**metadata):
    start_t = datetime.now()
    instances = [self._document(**x) for x in self.df_to_records(df,keep_index=keep_index,**metadata)]
    res=self._document.objects.insert(instances)
    diff_t = int((datetime.now() - start_t).total_seconds())
    logger.debug(f'store_df: Storing df with shape ({df.shape}) took {diff_t} seconds')
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
