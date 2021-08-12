import pandas as pd
import json
from pymongo import InsertOne, InsertMany, DeleteMany, ReplaceOne, UpdateOne, UpdateMany, IndexModel
import mongoengine
from mongoengine.queryset.queryset import QuerySet

#Add to_df to querysets (also for property .file)
def to_df(self,exclude=None):
    if exclude is None: exclude=['_id']
    return pd.DataFrame.from_records(json.loads(self.to_json()),exclude=exclude)
QuerySet.to_df=QuerySet.file=to_df

#Convinient class for all fields
class Fields(object):
    def __init__(self):
        for prop in dir(mongoengine):
            if prop.endswith('Field'):
                setattr(self,prop,getattr(mongoengine,prop))
fields=fields()



def Document(mongoengine.DynamicDocument):
    @property
    def _db_collection(self):
        return self._get_collection()

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

    def update_or_create(self,query=None,*args,files=None,**kwargs):
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
                rows=self.df_to_records(file,cur_meta)
                query.extend(rows)

        #Create operation
        db_collection=self._db_collection

        if len(query)==0 and len(updates)>0:
            if '$set' in updates and len(updates)==1: #Only insert one
                result = db_collection.insert_one(updates['$set'])
                ids = [result.inserted_id]
            else:
                if '$set' in updates: #Update many based on $set as the query
                q=updates.pop('$set') if '$set' in updates else {} #Update many based on $set as the query or update everything
                db_collection.update_many(q,updates,upsert=True);
                ids = [x['_id'] for x in db_collection.find(q,projection='_id')]

        elif len(query)>0 and len(updates)==0: #Insert many
            result = db_collection.insert_many(query)
            ids = result.inserted_ids

        elif len(query)>0 and len(updates)>0: #Bulk write update many
            ops=[];combined_query={}
            for cur_query in query:
                ops.append(UpdateMany(cur_query, updates,upsert=True))
                for key,val in cur_query.items():
                    if key in combined_query:
                        if combined_query[key]!=val:
                            if not isinstance(combined_query[key],dict) or '$or' not in combined_query[key]:
                                combined_query[key]={'$or':[combined_query[key]]}
                            if val not in combined_query[key]['$or']:
                                combined_query[key]['$or'].append(val)
                    else:
                        combined_query[key]=val
            result=db_collection.bulk_write(ops)
            ids=[x['_id'] for x in db_collection.find(combined_query,projection='_id')]

        return self.objects(_id__in=ids)

    def df_to_records(self,df,**metadata):
        df=df.reset_index()
        rows=df.to_dict(orient='records')
        return [{**metadata,**data} for data in rows]

    def store_df(self,df,**metadata):
        instances = [self(**x) for x in self.df_to_records(df,**metadata)]
        res=self.objects.insert(instances)
        return res
