import sqlalchemy as sql
import sqlalchemy.orm as orm
from copy import copy
from functools import partial
import os
import pandas as pd

from .db import *

def update_class(obj,new_class):
    for key,val in dict(new_class.__dict__).items():
        if key not in ['__module__','__dict__','__weakref__','__doc__']:
            setattr(obj,key,val)

def get_object_with_instance(obj,class_name=None):
    return [val for key,val in globals().items() if (hasattr(val,'__dict__') and obj in val.__dict__.values()) and (class_name is None or val.__class__.__name__==class_name)]

class tableMetaData:
    def __init__(self,tables):
        if type(tables) not in [list,tuple]: tables=[tables]
        self.tables=tables
    @property
    def columns(self):
        return [x for table in self.tables for x in list(table.columns)]
    @property
    def fields(self):
        props=['name','type','unique','default','nullable','comment','foreign_keys']
        df=pd.concat([pd.DataFrame([{x:getattr(col,x) for x in props} for col in table.columns]).assign(table=table.name).set_index(['name']) for table in self.tables])
        return df
    @property
    def field_names(self):
        return [col.name for table in self.tables for col in table.columns]

    @property
    def foreign_keys(self):
        res={}
        for col in self.columns:
            for fk in col.foreign_keys:
                if col.name not in res:
                    res[col.name]=[]
                res[col.name].append(fk)
        return res
    @property
    def foreign_tables(self):
        return [fk.column.table for col_name,fks in self.foreign_keys.items() for fk in fks]

    def related(self,related_type='any'): #related_type = ['any','upstream','downstream']
        related_type=related_type.lower()
        assert related_type in ['any','upstream','downstream'], "related_type must be one of: ['any','upstream','downstream']"
        res=[]
        if related_type in ['any','downstream']:
            for col_name,fks in self.foreign_keys.items():
                for fk in fks:
                    res.append({'column':col_name,'related_column':fk.column.name,'table':fk.column.table,'type':'downstream','fk':fk})
        if related_type in ['any','upstream']:
            for table in self.tables[0].tables:
                for col_name,fks in table._meta.foreign_keys.items():
                    for fk in fks:
                        if fk.column.table in self.tables:
                            res.append({'column':fk.parent.name,'related_column':col_name,'table':fk.parent.table,'type':'upstream','fk':fk})
        return pd.DataFrame(res)

def get_query_for_foreign_key(fk,objs,objs_for_parent=True):
    parent_table=orm.aliased(fk.parent.table)
    column_table=orm.aliased(fk.column.table)
    if objs_for_parent:
        rel_table=column_table
        filter_col=column_table.columns[fk.column.name]; objs_col=parent_table.columns[fk.parent.name]
        pks=list(parent_table.primary_key)
        filters=filter_for_rows(objs,pks)
    else:
        filters=[]
        rel_table=parent_table;
        filter_col=parent_table.columns[fk.parent.name]; objs_col=column_table.columns[fk.column.name]
    filters+=filter_for_rows(objs,[filter_col],[objs_col])
    return _query(rel_table,session=fk.parent.table.sess).filter(*filters)

class queryResult(object):
    def __init__(self,result,tables):
        self.tables=tables
        self.sess=tables[0].sess
        self._fields=result._fields
        self.__dict__.update({field:getattr(result,field) for field in self._fields})
        self._meta=tableMetaData(tables)
        self.all_tables=tables[0].tables
    def __str__(self):
        return str({field:getattr(self,field) for field in self._fields})
    def __repr__(self):
        return str(self)
    @property
    def objects(self):
        return _query(self.tables,session=self.sess)
    def search(self,*entities):
        return _query(*entities,session=self.sess)
    def related_queries(self,targets=None,related_type='any'): #related_type = ['any','upstream','downstream']
        related_df=self._meta.related(related_type=related_type)

        if targets is None: targets=list(self.all_tables)
        if type(targets) not in [list,tuple]: targets=[targets]
        targets=self.tables[0].tables.get(targets)

        related_df=related_df[related_df.table.isin(targets)]
        #assert related_df.shape[0]>0, f'No relationship were found for {", ".join([x.name for x in targets])}.'

        res=[]
        for i,row in related_df.iterrows():
            q=get_query_for_foreign_key(row.fk,[self],objs_for_parent=(row.type=='downstream'))
            res.append((row.column,row.table,row.type,q,q.count()))
        return pd.DataFrame(res,columns=['column','table','type','dbquery','count'])

    def relations(self,targets=None,related_type='upstream',only_one=True):
        res=self.related_queries(targets=targets,related_type=related_type)
        items=[x for q in res.dbquery.to_list() for x in q.all()]
        return items[0] if only_one and len(items)>0 else items

    def __getattr__(self, name):
        if name.startswith('get_'):
            fk=self._get_fk(name[4:])
            if fk is not None:
                return self.get_fk_item(fk)
        for table in self.tables:
            if name in table.extra_attribs:
                res=table.extra_attribs[name]
                if callable(res):
                    return res(self)
                else:
                    return res
        raise AttributeError(f'{self.__class__.__name__}.{name} is invalid.')
    @property
    def fks(self):
        if not hasattr(self,'_fks'):
            self._fks={fk.parent.name:fk for table in self.tables for fk in table.foreign_keys}
        return self._fks
    def _get_fk(self,fk_name):
        fks=self.fks
        return fks[fk_name] if fk_name in fks else None
    def get_fk_item(self,fk,limit=None):
        q=get_query_for_foreign_key(fk,[self],objs_for_parent=True)
        if limit is not None: q=q.limit(limit)
        items=q.all()
        return items

class _query(orm.query.Query):
    @property
    def tables(self):
        if not hasattr(self,'_tables'):
            tables=[]
            for entity in self._entities:
                table=entity.column.table
                tables.append(table.original if isinstance(table,sql.sql.selectable.Alias) else table)
            self._tables=list(set(tables))
        return self._tables
    def __getitem__(self, *args,**kwargs):
        results=super().all().__getitem__(*args,**kwargs)
        return self._add_related_to_results(results)
    def all(self, *args,**kwargs):
        results=super().all(*args,**kwargs)
        return self._add_related_to_results(results)
    @property
    def df(self):
        return query_to_df(self)
    def _add_related_to_results(self,results):
        is_single=False
        if not isinstance(results,list):
            results=[results]
            is_single=True
        new_results=[]
        for result in results:
            new_results.append(queryResult(result,self.tables))
        return new_results[0] if is_single else new_results

    def filter(self,*args,**kwargs):
        filters=convert_props_to_filters(self.tables,*args,**kwargs)
        return super().filter(*filters)

    def _table_with_column(self,col_name,assert_exists=False):
        return table_with_column(self.tables,col_name,assert_exists=assert_exists)

    def related(self,col_name):
        table=self._table_with_column(col_name,assert_exists=True)
        col=getattr(table.columns,col_name)
        fks=list(col.foreign_keys)
        return get_query_for_foreign_key(fks[0],self,objs_for_parent=True) if len(fks)>0 else None

class Column(sql.sql.schema.Column):
    def change_foreign_keys(self):
        for fk in self.foreign_keys:
            fk.parent.table=self.table.tables.get(fk.parent.table)
            fk.column.table=self.table.tables.get(fk.column.table)

class Table(sql.sql.schema.Table):
    def change_foreign_keys(self):
        for fk in self.foreign_keys:
            fk.parent.table=self.tables.get(fk.parent.table)
            fk.column.table=self.tables.get(fk.column.table)
    @property
    def _meta(self):
        return tableMetaData(self)
    def add_result_attribs(self,attribs):
        self.extra_attribs.update(attribs)
    @property
    def objects(self):
        return _query(self,session=self.sess)

class tables(object):
    iter_pos = 0
    table_names=[]
    def __init__(self,DB_instance):
        table_names=DB_instance.table_names
        self.sess=DB_instance.sess
        if table_names is None:
            table_names=get_table_names(DB_instance.meta)
        schema='' if DB_instance.schema is None else f'{DB_instance.schema}.'

        #add tables to self
        for table_name in table_names:
            if len(table_name.split('.'))==1:
                table_name=f'{schema}{table_name}'
            table=DB_instance.meta.tables[table_name]
            self.add_table(table)
    def __iter__(self):
        self.iter_pos = 0
        return self
    def __getitem__(self,name):
        return self.get(name)
    def __next__(self):
        if self.iter_pos < len(self.table_names):
            result=self.get(self.table_names[self.iter_pos])
            self.iter_pos += 1
            return result
        else:
            raise StopIteration

    def __str__(self):
        return ', '.join(self.table_names)
    def add_table(self,table):
        if table.name not in self.__dict__:
            self.table_names.append(table.name)
            table.__class__=Table
            table.tables=self
            table.sess=self.sess
            table.extra_attribs={}
            self.__dict__[table.name]=table
            table.change_foreign_keys()
            if table.name not in self.table_names:
                self.table_names.append(table.name)

    def get(self,table):
        if type(table) in [list,tuple]:
            return [self.get(x) for x in table]
        if isinstance(table,str):
            table_name = table.split('.')[-1]
        else:
            self.add_table(table)
            table_name=table.name
        return self.__dict__[table_name]

class DB:
    _engine=None
    _meta=None
    _sess=None
    _table_names=None
    def __init__(self,conn_str,schema=None,table_names=None):
        if os.path.isfile(conn_str):
            conn_str=f'sqlite:///{conn_str}'
        self.conn_str=conn_str
        self.schema=schema
        self._table_names=table_names
    def __str__(self):
        return f'{self.conn_str}'
    @property
    def table_names(self):
        if self._table_names is None:
            self._table_names=[x.split('.')[-1] for x in get_table_names(self.meta)]
        return self._table_names
    @property
    def engine(self,echo=False):
        if self._engine is None:
            self._engine=create_engine(self.conn_str,echo=echo)
        return self._engine
    @property
    def meta(self):
        if self._meta is None:
            self._meta=get_metadata(self.engine,schema=self.schema)
        return self._meta
    @property
    def sess(self):
        if self._sess is None:
            self._sess=get_session(self.engine)
        return self._sess
    def connect(self,verbose=1):
        if verbose>0:
            print(f'Connecting to {self.conn_str}...')
        if verbose>0:
            print('Connected!')
        return self
    def rollback(self):
        self.sess.rollback()
        return self
    @property
    def tables(self):
        if not hasattr(self,'_tables'):
            self._tables=tables(self)
        return self._tables
    def reset_tables(self):
        if hasattr(self,'_tables'):
            del self._tables
