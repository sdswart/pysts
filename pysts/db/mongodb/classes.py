import os
from io import BytesIO
import gridfs
import pymongo
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne, UpdateMany, IndexModel
from .process import get_buffer_metas, df_to_buffer, buffer_to_df
from .utils import get_client, to_list, obj_to_buffer, buffer_to_obj

INDEX_OPTIONS={x:getattr(pymongo,x) for x in ['ASCENDING', 'DESCENDING', 'GEO2D', 'GEOHAYSTACK', 'GEOSPHERE', 'HASHED', 'TEXT']}

class COLLECTION(object):
    _properties=None
    def __init__(self,collections,id):
        self._collections=collections
        self._id=id
    @property
    def properties(self):
        if self._properties is None:
            self._properties=self._collections._db_collection.find_one({'_id':self._id})
        return self._properties
    def __repr__(self):
        return f'{self._collections.name.capitalize()}({self._id})'
    def __str__(self):
        return str(self.properties)
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            assert name in self.properties, f'{self.__repr__()} does not have a meta attribute "{name}"'
            return self.properties[name]
    def update(self,*args,**kwargs):
        updates=self._collections._get_updates(*args,**kwargs)
        return self._collections._db_collection.update_one({'_id':self._id},updates)

    def delete(self,properties=None):
        if properties is not None:
            if type(properties) not in [list,tuple]: properties=[properties]
            unsets={}
            for prop in properties:
                unsets[prop]=1
                if self._properties is not None and prop in self._properties:
                    del self.properties[prop]
            return self._collections._db_collection.update_one({'_id':self._id},{'$unset':unsets})
        else:
            res = self._collections._db_collection.delete_one({'_id':self._id})
            del self
            return res
    def __delete__(self):
        return self.delete()

class COLLECTIONS(object):
    def __init__(self,collection,required_meta=None,create_index=None):
        self._db_collection=collection
        self._required_meta=required_meta
        self.child_class=COLLECTION
        if create_index is not None:
            options={}
            if isinstance(create_index,str):
                indexes=create_index
            elif isinstance(create_index,dict):
                indexes=[]
                for field,props in create_index.items():
                    indexes.append((field,INDEX_OPTIONS[props['type']]) if 'type' in props else field)
                    if 'options' in props:
                        options.update(props['options'])
            else:
                raise Exception('create_index must be one of [str,dict]')
            self._db_collection.create_index(indexes,**options)
    def __getattr__(self,name):
        try:
            object.__getattr__(self,name)
        except:
            return getattr(self._db_collection,name)

    @property
    def name(self):
        return self._db_collection._Collection__name
    def get(self,*args,**query):
        query.update(dict(pair for d in args if isinstance(d,dict) for pair in d.items()))
        return [self.child_class(self,x['_id']) for x in self._db_collection.find(query,projection='_id')]
    def _check_meta(self,query=None,*args):
        if self._required_meta is None:
            return True
        if query is None:
            query={}
        else:
            query=query.copy()
        for arg in args:
            query.update(arg['$set'] if '$set' in arg else arg)
        return all([x in query for x in self._required_meta])
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
    def update_or_create(self,query=None,*args,**kwargs):
        if query is None: query={}
        updates=self._get_updates(*args,**kwargs)
        if len(query)==0 and '$set' in updates:
            query=updates['$set']
        assert len(query)>0, "No attributes provided for the record"
        if len(updates)==0: updates['$set']=query

        if not isinstance(query,list):
            query=[query]

        ops=[];combined_query={}
        for cur_query in query:
            if self._check_meta(cur_query,updates):
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

        #bulk write ops
        ids=[]
        if len(ops)>0:
            result=self._db_collection.bulk_write(ops)
            ids=[x['_id'] for x in self._db_collection.find(combined_query,projection='_id')]

        return [self.child_class(self,x) for x in ids]

class FILE(COLLECTION):
    _buffer=None
    _file=None
    @property
    def buffer(self):
        if self._buffer is None:
            self._buffer= self._collections._gridfs.find_one({'_id':self._id})
        return self._buffer
    @property
    def file(self):
        if self._file is None:
            buffer=self.buffer
            props=self.properties
            varnames=self._collections._buffer_to_file.__code__.co_varnames
            send_props={key:val for key,val in props.items() if 'kwargs' in varnames or key in varnames}
            if 'props' in varnames: send_props['props']=props
            if 'properties' in varnames: send_props['properties']=props
            self._file=self._collections._buffer_to_file(buffer,**send_props)
        return self._file
    @property
    def obj(self):
        return self.file
    def save_to(self,path):
        if os.path.isdir(path):
            filename=self.info.filename
            if filename is None:
                filename=f'{self._collections.name.capitalize()}_{self._id}'
            path=os.path.join(path,filename)
        with open(path,'wb') as f:
            f.write(self.buffer.read())
        return path

class FILES(COLLECTIONS):
    def __init__(self,gridfs,extensions=None,path_processor=None,buffer_to_file=None,file_to_buffer=None,**kwargs):
        super().__init__(gridfs._GridFS__files,**kwargs)
        self._gridfs=gridfs
        self._buffer_to_file=buffer_to_obj if buffer_to_file is None else buffer_to_file
        self._extensions=extensions
        self._path_processor=path_processor
        self._file_to_buffer=obj_to_buffer if file_to_buffer is None else file_to_buffer
        self.child_class=FILE

    def update_or_create(self,files=None,query=None,*args,**kwargs):
        ids=[]
        if files is not None:
            if query is None: query={}
            updates={}
            for key,val in kwargs.items():
                if key=='$set':
                    updates.update(val)
                elif not key.startswith('$'):
                    updates[key]=val
            updates.update(dict(pair for d in args if isinstance(d,dict) for pair in d.items()))
            if len(query)==0:
                query=updates
                updates={}

            if not isinstance(files,list):
                files=[files]

            buffers=[]
            for file in files:
                if isinstance(file,str) and os.path.exists(file):
                    buffers.extend(self.process_path(file))
                else:
                    buffers.append(self._file_to_buffer(file))

            for buffer,meta in get_buffer_metas(buffers):

                #query and meta
                cur_query={**query,**meta}
                cur_meta={**cur_query,**updates}

                #Ensure we have required_meta
                if self._check_meta(cur_meta):
                    #delete file if it exists
                    if len(cur_query)>0:
                        existing=self._gridfs.find_one(cur_query)
                        if existing is not None:
                            self._gridfs.delete(existing._id)

                    #Store buffer and close
                    ids.append(self._gridfs.put(buffer,**cur_meta))
                    BytesIO.close(buffer)
            return [self.child_class(self,x) for x in ids]
        else:
            return super().update_or_create(query=query,*args,**kwargs)

    def process_file(self,path):
        if self._extensions is not None and not any([path.lower().endswith(x) for x in self._extensions]):
            return None
        return obj_to_buffer(path)

    def process_path(self,path):
        buffers=[]
        if self._path_processor:
            files=self._path_processor(path)
            buffers=[self._file_to_buffer(file) for file in files]
        else:
            paths=[path] if os.path.isfile(path) else [os.path.join(path,file) for file in os.listdir(path)]
            for cur_path in paths:
                buffer=self.process_file(cur_path)
                if buffer is not None:
                    buffers.append(buffer)
        return buffers

class DB(object):
    _collections={}
    def __init__(self,DB_name,url='l3-37:27017/',collections=None,username=None,password=None,srv=False):
        client = get_client(url=url,username=username,password=password,srv=srv)
        self.DB_name=DB_name
        self._db=client[DB_name]
        if collections is not None:
            #collections must be a dict of collections with relavent properties
            self._initialize_collections(collections)
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            if name not in self._collections:
                self._collections[name]=COLLECTIONS(getattr(self._db,name))
            return self._collections[name]
    def GridFS(self,name,collection_name=None,file_type=None,**kwargs):
        if name not in self._collections:
            if file_type is not None:
                if file_type.lower()=='dataframe':
                    kwargs['buffer_to_file']=buffer_to_df
                    kwargs['file_to_buffer']=df_to_buffer
                    kwargs['extensions']=['.csv']
                elif file_type.lower()=='image':
                    kwargs['extensions']=['.jpg','png','.jpeg']
            if collection_name is None:
                collection_name=name
            self._collections[name]=FILES(gridfs.GridFS(self._db,collection=collection_name),**kwargs)
        return self._collections[name]
    def Collection(self,name,collection_name=None,**kwargs):
        if name not in self._collections:
            if collection_name is None:
                collection_name=name
            self._collections[name]=COLLECTIONS(getattr(self._db,collection_name),**kwargs)
        return self._collections[name]
    def _initialize_collections(self,descriptions):
        common_props=['collection_name','required_meta','create_index']
        FILES_props=['file_type','extensions','path_processor','buffer_to_file','file_to_buffer']
        for name,props in descriptions.items():
            collection_type=props['collection_type'].lower() if 'collection_type' in props else 'standard'
            if any([x in props for x in FILES_props]):
                collection_type='gridfs'

            #get common props:
            kwargs={x:(props[x] if x in props else None) for x in common_props}

            if collection_type=='standard':
                _=self.Collection(name,**kwargs)
            elif collection_type=='gridfs':
                kwargs.update({x:(props[x] if x in props else None) for x in FILES_props})
                _=self.GridFS(name,**kwargs)
    def __repr__(self):
        return f'DB({self.DB_name})'
    def __str__(self):
        return f'{self.DB_name} with {self.tests._db_collection.count_documents({})} tests, {self.tables._db_collection.count_documents({})} tables, {self.images._db_collection.count_documents({})} images, and {self.files._db_collection.count_documents({})} files'
