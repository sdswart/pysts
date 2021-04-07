import sqlalchemy as sql
import sqlalchemy.orm as orm
import pandas as pd

def create_engine(conn_str,echo=False,**kwargs):
    engine=sql.create_engine(conn_str, echo=echo,**kwargs)
    return engine

def raw_sql(engine,query):
    with engine.connect() as con:
        rs = con.execute(query)
    return rs

def get_schemas(engine):
    inspector = sql.inspect(engine)
    return inspector.get_schema_names()

def get_metadata(engine,schema=None):
    meta = sql.MetaData()
    options={'oracle_resolve_synonyms':True} if engine.name=='oracle' else {}
    if schema is None and hasattr(engine,'conn_name') and engine.conn_name in schemas:
        schema=schemas[engine.conn_name]
    meta.reflect(bind=engine,schema=schema,**options)
    return meta

def get_session(engine):
    Session = orm.session.sessionmaker()
    return Session(bind=engine)

def get_table_names(meta):
    return list(meta.tables.keys())

def get_all_table_names(engine):
    res={}
    for schema in get_schemas(engine):
        meta=get_metadata(engine,schema=schema)
        if meta is not None:
            res[schema] = get_table_names(meta)
    return res

def query_to_df(q):
    try:
        df=pd.read_sql(q.statement, q.session.bind)
    except:
        df=pd.read_sql(str(q.statement), q.session.bind)
    return df

get_table_columns = lambda table: list(table.columns)
query_table=lambda table,session: session.query(table)
display_query=lambda q: display(HTML(query_to_df(q).to_html()))

def filter_for_rows(objs,filter_cols,objs_cols=None):
    if objs_cols is None:
        objs_cols=filter_cols
    filters=[]
    if orm.query.Query in type(objs).__mro__:
        for filter_col,objs_col in zip(filter_cols,objs_cols):
            filters.append(filter_col.in_(objs.with_entities(objs_col).subquery()))
    else:
        for filter_col,objs_col in zip(filter_cols,objs_cols):
            filters.append(filter_col.in_([getattr(obj,objs_col.name) for obj in objs]))
    return filters

def table_with_column(tables,col_name,assert_exists=False):
    tables=[table for table in tables if hasattr(table.columns,col_name)]
    table=tables[0] if len(tables)>0 else None
    if assert_exists:
        assert table is not None, f"No matching table in query exists with the column = {col_name}"
    return table

def convert_props_to_filters(tables,*args,exclude=False,**kwargs):
    args=list(args)
    for key,val in kwargs.items():
        props=key.split('__')
        table=table_with_column(tables,props[0])
        comparitors={'equal':lambda col,val:col==val,
                    'in':lambda col,val:col.in_(val)}
        if props[-1] in comparitors:
            comparison=comparitors[props[-1]]
            props=props[:-1]
        else:
            comparison=comparitors['equal']

        for i,prop in enumerate(props):
            col=getattr(table.columns,prop)
            lastprop=i==len(props)-1
            if not lastprop:
                fk=list(col.foreign_keys)[0]
                table=orm.aliased(fk.column.table)
                new_col=getattr(table.columns,fk.column.name)
                args.append(col==new_col)
            else:
                cur_arg=comparison(col,val)
                args.append(not cur_arg if exclude else cur_arg)
    return args
