import uuid
import re
import os
import logging
import sys
import psutil
import numpy as np
import datetime
import pandas as pd

from . import functions

def is_json_serializable(obj):
    return type(obj) in [None,str,list,dict,int,float,bool,complex,datetime.date,datetime.datetime,datetime.timedelta]

def to_json_serializable(obj):
    if isinstance(obj,list):
        return [to_json_serializable(x) for x in obj]
    elif isinstance(obj,dict):
        return {key:to_json_serializable(val) for key,val in obj.items()}
    elif isinstance(obj,np.ndarray):
        return obj.tolist()
    elif isinstance(obj,np.generic):
        return obj.item()
    elif isinstance(obj,pd._libs.tslibs.timestamps.Timestamp):
        return obj.to_pydatetime()
    elif is_json_serializable(obj):
        return obj
    else:
        return str(obj)

def kill_proc_tree(pid, including_parent=True):
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()
    gone, still_alive = psutil.wait_procs(children, timeout=5)
    if including_parent:
        parent.kill()
        parent.wait(5)

def get_cwd():
    return os.path.abspath(os.getcwd())

def get_base_path():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

#file_dir = os.path.dirname(os.path.realpath(__file__))
def append_fcns(fcn,*fcns):
    def rtn(*args,**kwargs):
        res=[x(*args,**kwargs) for x in ([fcn]+fcns)]
        if any([x is False for x in res]):
            return False
    return rtn

def create_logger(name,level='INFO',format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
                change_if_exists=False,add_stdout_handler=False):
    logger_exists = name in logging.getLogger().manager.loggerDict
    logger = logging.getLogger(name)

    if not logger_exists or change_if_exists:
        logging.basicConfig(format=format)
        level=getattr(logging,level)
        logger.setLevel(level)

        if add_stdout_handler:
            ch = logging.StreamHandler(sys.stdout)
            if level is not None:
                ch.setLevel(level)
            formatter = logging.Formatter(format)
            ch.setFormatter(formatter)
            logger.addHandler(ch)
    return logger

def create_uuid(hex=True):
    res=uuid.uuid4()
    return res.hex if hex else str(res)

def is_list(obj):
    return type(obj) in [list,tuple]

def iterable(l):
    try:
        iter(l)
        return True
    except:
        return False

def natural_sort(l,by=None):
    convert = lambda text: int(text) if text.isdigit() else text.lower()

    pos=None
    if iterable(by) and len(by)==len(l):
        l=zip(l,by)
        pos=1
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key if pos is None else key[pos]) ]
    res=sorted(l, key = alphanum_key)
    if pos is not None: res=list(zip(*res))[0]
    return res

def combine_decorators(decorators):
    def rtn_func(func):
        func=func
        for decorator in decorators[::-1]:
            func=decorator(func)
        return func
    return rtn_func

comparitors=["exact","in","gte","lte","lt","gt","contains","icontains","istartswith","iendswith","startswith","endswith","year","month","day","hour","minute","second","isnull"]
def compare_input_to_value(input,value,comparison=None):
    if comparison is None or comparison=="exact":
        return input==value
    elif comparison=="in":
        return input in value
    elif comparison=="gte":
        return input >= value
    elif comparison=="lte":
        return input <= value
    elif comparison=="lt":
        return input < value
    elif comparison=="gt":
        return input > value
    elif comparison=="contains":
        return value in input
    elif comparison=="icontains":
        return value.lower() in input.lower()
    elif comparison=="startswith":
        return input.startswith(value)
    elif comparison=="endswith":
        return input.endswith(value)
    elif comparison=="istartswith":
        return input.lower().startswith(value.lower())
    elif comparison=="iendswith":
        return input.lower().endswith(value.lower())
    elif comparison in ["year","month","day","hour","minute","second"]:
        return getattr(input,comparison)==value
    elif comparison=="isnull":
        return input is None
    else:
        raise Exception("Comparison not allowed with: %s"%comparison)

def get_field(obj,prop,setting_field=False,method_kwargs=None):
    props=prop.replace(".","__").split("__")
    field=obj
    last_field=obj;last_type=None
    for i_prop,prop in enumerate(props):
        last_field=field
        if hasattr(field,prop):
            prop_val=getattr(field,prop)
            last_type="attr"

            if inspect.ismethod(prop_val):
                available_inputs=["self"]
                if method_kwargs is None:
                    method_kwargs={}
                else:
                    available_inputs.extend(list(method_kwargs))
                input_names=functions.need_more_inputs(prop_val,available_inputs=available_inputs,return_missing=True)
                if len(input_names)>0:
                    return "Missing inputs: %s"%", ".join(input_names)
                else:
                    field=prop_val(**method_kwargs)
            else:
                field=prop_val
        elif prop in comparitors:
            last_type="comparison"
            field=lambda compare_to: compare_input_to_value(field,compare_to,prop)
        elif isinstance(field, dict):
            last_type="dict"
            assert prop in field, f"Key {prop} is not in field"
            field=field[prop]
        elif isinstance(field, list): #prop must include "name#type" or "type" if adding a new index
            last_type="list"
            if is_integer(prop) and has_index(field,prop):
                field = field[int(prop)]
            elif prop=='min':
                field=min(field)
            elif prop=='max':
                field=max(field)
            elif prop=='mean':
                field=np.mean(field)
            elif ':' in prop:
                field=field[slice(*[int(x) for x in prop.split(':')])]
            else:
                raise Exception(f'{prop} not found in list or one of [min, max, mean]')
        else:
            field=None

    if setting_field:
        return last_field,prop,last_type,field
    else:
        return field
