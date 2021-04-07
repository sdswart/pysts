import re, os
import numpy as np
import pandas as pd
from itertools import groupby
from io import BytesIO, StringIO

def get_buffer_metas(buffers,split_buffer_meta=False,*args,**kwargs):
    if type(buffers) not in [list,tuple] or (len(buffers)==2 and isinstance(buffers[1],dict)):
        buffers=[buffers]
    res=[]
    kwargs.update(dict(pair for d in args if isinstance(d,dict) for pair in d.items()))
    for buffer in buffers:
        cur_meta=kwargs.copy()
        if type(buffer) in [list,tuple]:
            if len(buffer)>1:
                cur_meta.update(dict(pair for d in buffer[1:] for pair in d.items()))
            buffer=buffer[0]
        if hasattr(buffer,'_file'):
            cur_meta.update(buffer._file)
        res.append([buffer,cur_meta])
    if split_buffer_meta:
        return list(zip(*res))
    else:
        return res

def dfs_to_buffers(dfs):
    df_metas=get_buffer_metas(dfs)
    res=[]
    for df,meta in df_metas:
        #sort dfs
        if 'Time' in df.columns:
            df=df.sort_values(by=['Time'])
        elif 'RecordID' in df.columns:
            df=df.sort_values(by=['RecordID'])

        buffer = BytesIO()
        buffer.close=lambda: None
        #data=bytes(df.to_csv(index=False).encode("utf-8"))
        #buffer.write(data)
        data=df.to_pickle(buffer)

        buffer.seek(0)
        res.append([buffer,meta])
    return res

def df_to_buffer(df):
    return dfs_to_buffers(df)[0]

def buffers_to_dfs(buffers,single_df=False,**groupmeta):
    res=[]
    for buffer,meta in get_buffer_metas(buffers,**groupmeta):
        #file=StringIO()
        buffer.seek(0)
        #file.write(buffer.read().decode())
        #file.seek(0)
        #df=pd.read_csv(file)
        df=pd.read_pickle(buffer)
        res.append([df,meta])

    if single_df:
        dfs=[]
        for df,meta in res:
            dfs.append(df.assign(**{key:(', '.join([str(x) for x in val]) if isinstance(val,list) else val) for key,val in meta.items()}))
        res=pd.concat(dfs)
    return res

def buffer_to_df(buffer,**groupmeta):
    return buffers_to_dfs(buffer,single_df=True,**groupmeta)
