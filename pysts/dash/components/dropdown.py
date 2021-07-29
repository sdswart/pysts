from .base import *
from .base_component import Component

def add_to_dropdown(new_options,new_values,options,values,multi=True,keep_old_uploads=True):
    if keep_old_uploads:
        options=options+[x for x in new_options if x not in options]
    else:
        options=new_options

    if keep_old_uploads and multi:
        values=values+[x for x in new_values if x not in values]
    elif multi:
        values=new_values
    elif len(new_values)>0:
        values=new_values[0]

    return options,values

class Dropdown(Component):
    def __init__(self,multi=False,label=None,options=None,update_value_on_change=True,keep_old_values=True,value=None):

        if options is None: options=[]
        options = [{'label': x, 'value': x} if isinstance(x,str) else x for x in options]
        if value is None: value=[]

        self.dropdown=dcc.Dropdown(options=options,multi=multi,value=value)

        children=[html.Label([f'{label}: '])] if label else []
        children.append(self.dropdown)

        self.layout=html.Div(children)

        self.properties={'value':self.dropdown.value,
                        'options':self.dropdown.options,
                        'loading_state':self.dropdown.loading_state}
