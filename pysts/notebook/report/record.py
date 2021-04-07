from ..utils import *
from ..notebook import get_notebook
import pandas as pd
import os, json

csv_path='reports.csv'

def get_report_hash(notebook_path,inputs):
    nb=[x['source'] for x in get_notebook(notebook_path).cells if 'source' in x]
    context=f'{str(nb)}-{str(inputs)}'
    return get_hash(context)

def get_previous_report(notebook_path,inputs,csv_path=csv_path):
    hash=get_report_hash(notebook_path,inputs)
    if os.path.isfile(csv_path):
        report_df=pd.read_csv(csv_path)
        report_df=report_df[report_df.hash==hash]
        if report_df.shape[0]>0:
            paths=report_df.path.iloc[-1].split('#')
            if all([os.path.isfile(x) for x in paths]):
                return paths
    return None

def get_previous_inputs(notebook=None,inputs=None,csv_path=csv_path):
    res={}
    if inputs is not None:
        if not isinstance(inputs,list):
            inputs=[inputs]
        res={x:[] for x in inputs}

    if os.path.isfile(csv_path):
        df=pd.read_csv(csv_path)
        if notebook is not None:
            df=df[df.notebook==notebook]
        for i,row in df.iterrows():
            vals=json.loads(row.inputs)
            for key,val in vals.items():
                if inputs is None and key not in res:
                    res[key]=[]
                if key in res and val not in res[key]:
                    res[key].append(val)
    if len(res)==1:
        res=res[list(res.keys())[0]]
    return res

def record_report(notebook_path,inputs,paths,csv_path=csv_path):
    hash=get_report_hash(notebook_path,inputs)
    paths='#'.join(paths)
    df=pd.DataFrame({'hash':[hash],'path':[paths],'inputs':json.dumps(inputs),'notebook':notebook_path})
    if os.path.isfile(csv_path):
        report_df=pd.read_csv(csv_path)
        report_df=report_df[report_df.hash!=hash]
        df=report_df.append(df, ignore_index=True)
    df.to_csv(csv_path,index=False)
