from flask import Flask,make_response,session
import pandas as pd
import numpy as np
import uuid
import dash
import os

import webbrowser

def get_dynamic_values(id_type):
    res={}
    for inputs_list in dash.callback_context.inputs_list:
        if isinstance(inputs_list,list): #dynamic types
            for cur_input in inputs_list:
                if 'value' in cur_input and isinstance(cur_input['id'],dict) and cur_input['id']['type']==id_type:
                    res[cur_input['id']['index']]=cur_input['value']
    return res

def generate_unique_id():
    return uuid.uuid4().hex[:6].upper()

def open_browser():
    webbrowser.open_new('http://127.0.0.1:5000/cc/')

def load_environ_variables():
    with open('.env') as f:
        for line in f:
            if not line.startswith('#') and '=' in line:
                props=line.split('=')
                if len(props)==2 and not props[0] in os.environ:
                    os.environ[props[0]]=props[1]
