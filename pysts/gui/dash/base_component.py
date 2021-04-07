import dash
from dash.dependencies import Input, Output, State, MATCH, ALL
import uuid

from dash.development.base_component import Component as BaseComponent, ComponentMeta
from plotly.basedatatypes import BaseFigure

import inspect
import threading
from copy import copy

BASE_COMPONENTS=[BaseComponent,ComponentMeta]

def generate_unique_id():
    return uuid.uuid4().hex[:6].upper()

def update_class(obj,new_class):
    for key,val in dict(new_class.__dict__).items():
        if key not in ['__module__','__dict__','__weakref__','__doc__']:
            setattr(obj,key,val)

to_list = lambda obj: [] if obj is None else list(obj) if type(obj) in [list,tuple] and (len(obj)==0 or type(obj[0]) in [list,tuple]) else [obj]

def iterable(obj):
    try:
        _=iter(obj)
        return True
    except:
        return False

class Callback(object):
    contexts = threading.local()
    fcn=outputs=inputs=states=format_output=None
    def __init__(self,fcn=None,outputs=None,inputs=None,states=None,format_output=None):
        self.fcn=fcn
        self.outputs=to_list(outputs)
        self.inputs=to_list(inputs)
        self.states=to_list(states)
        self.format_output=format_output
    def __str__(self):
        return f'Callback(fcn={self.fcn}, outputs={self.outputs}, inputs={self.inputs}, states={self.states}, format_output={self.format_output})'
    def __repr__(self):
        return str(self)
    def register(self):
        type(self).get_callbacks().append(self)
    def is_valid(self):
        valid_in_out=lambda vals: (isinstance(vals,list) and len(vals)>0 and
                all([type(x) in [list,tuple] and len(x)==2 for x in vals]))
        checks={'fcn':lambda x: (x is not None and callable(x)),
            'outputs':valid_in_out,
            'inputs':valid_in_out,
            'states':lambda vals: (isinstance(vals,list)),
            'format_output':lambda x: (x is None or callable(x))}
        return all([fcn(getattr(self,prop)) for prop,fcn in self.checks.items()])
    @classmethod
    def get_callbacks(cls):
        if not hasattr(cls.contexts, 'callbacks'):
            cls.contexts.callbacks = []
        return cls.contexts.callbacks

def Call(outputs, inputs, states=None, format_output=None):
    def rtn(fcn):
        Callback(fcn=fcn,outputs=outputs,inputs=inputs,states=states,format_output=format_output).register()
    return rtn

class Item(object):
    dynamic_id=obj=indexes=None
    def __init__(self,parent,name,obj):
        objID=f'{parent.id}-{name}'
        if inspect.isclass(obj):
            self.dynamic_id=objID
        else:
            obj.id=objID
        self.obj=obj
        self.indexes=[]
    def __call__(self,*args,index=None,**kwargs):
        assert self.dynamic_id is not None, 'Can only create dynamic components passed as an object class'
        if index is None:
            index=str(len(self.indexes))
        obj=self.obj(*args,**kwargs)
        obj.id={'type':self.dynamic_id,'index':index}
        self.indexes.append(index)
        return obj
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except Exception as e:
            if self.dynamic_id is not None:
                prop_type=ALL
                if name.endswith('_match'):
                    name=name.replace('_match','')
                    prop_type=MATCH
                elif name.endswith('_all'):
                    name=name.replace('_all','')
                return ({'type':self.dynamic_id,'index':prop_type},name)
            elif name in self.obj._prop_names:
                return (self.obj.id,name)
            else:
                raise e
    def __setattr__(self, name, val):
        obj=self.obj if hasattr(self.obj,name) else self
        object.__setattr__(obj,name, val)

class Component(object):
    properties=callbacks=_id=None
    def class_instances(self):
        instances=type(self).get_instances()
        if self.__class__.__name__ not in instances:
            instances[self.__class__.__name__]=[]
        if self not in instances[self.__class__.__name__]:
            instances[self.__class__.__name__].append(self)
        return instances[self.__class__.__name__]
    @property
    def id(self):
        if not self._id:
            instances=self.class_instances()
            self._id=f'{self.__class__.__name__}-{len(instances)}'
        return self._id
    def __call__(self, outputs, inputs, states=None, format_output=None):
        def rtn(fcn):
            self.add_callback(fcn=fcn,outputs=outputs,inputs=inputs,states=states,format_output=format_output)
        return rtn
    def className(self,names):
        class_names=self.layout.obj.className
        if class_names is None:
            class_names=""
        elif class_names!="" and not class_names.endswith(' '):
            class_names+=' '
        self.layout.obj.className=class_names+names.strip()
        return self
    def add_callback(self,fcn, outputs, inputs, states=None, format_output=None):
        if not self.callbacks: self.callbacks=[]
        self.callbacks.append(Callback(fcn=fcn,outputs=outputs,inputs=inputs,states=states,format_output=format_output))
        self.callbacks[-1].register()
    def __getattribute__(self, name):
        properties=object.__getattribute__(self, 'properties')
        if isinstance(properties,dict) and name in properties:
            return properties[name]
        return object.__getattribute__(self, name)
    def __setattr__(self, name, val):
        if any([isinstance(val,x) for x in BASE_COMPONENTS]):
            object.__setattr__(self, name, Item(self,name,val))
        else:
            return object.__setattr__(self, name, val)
    def process_layout_children(self,layout):
        def get_layout(val):
            #print('3)',val)
            if val is not None:
                if Component in type(val).__mro__:
                    val=val.generate_layout()
                elif isinstance(val,Item):
                    val=self.generate_layout(val.obj)
                else:
                    val=self.generate_layout(val)
            return val
        #print('1)',layout)
        if hasattr(layout,'children'):
            children=layout.children
            #print('2)',type(children),children)
            if children is not None:
                if isinstance(children,list):
                    layout.children=[get_layout(x) for x in children]
                else:
                    layout.children=get_layout(children)
    def generate_layout(self,layout=None):
        if layout is None:
            if isinstance(self.layout,Item):
                layout=self.layout.obj
            elif callable(self.layout):
                layout=self.layout()
                if isinstance(layout,Item):
                    layout=layout.obj
            else:
                layout=self.layout

        self.process_layout_children(layout)

        return layout
    @classmethod
    def get_instances(cls):
        if not hasattr(cls, 'instances'):
            cls.instances={}
        return cls.instances


#for comp in BASE_COMPONENTS:
#    update_class(comp,Component)
def inputs_match_trigger(inputs,states,callback_trigger):
    inputs=to_list(inputs)
    states=to_list(states)
    trigger_ids=[x['prop_id'] for x in callback_trigger] #['{"index":"Transfer","type":"folder-item-B4DAD1"}.n_clicks']
    input_in_trigger = lambda x,trigger: ((x[0]['type'] if isinstance(x[0],dict) and 'type' in x[0] else str(x[0])) in trigger) and (x[1] in trigger)
    return any([any([input_in_trigger(x,trigger_x) for trigger_x in trigger_ids]) for x in inputs+states]) #[{'prop_id': 'alert-interval-88C021.n_intervals', 'value': 5}]

def generate_callback(app,outputs,inputs,states,callbacks):
    @app.callback([Output(x[0],x[1]) for x in outputs],
                [Input(x[0],x[1]) for x in inputs],
                [State(x[0],x[1]) for x in states])
    def rtn(*values):
        values=list(values)
        inputs_states=inputs+states
        res=values[-len(outputs):]
        for callback in callbacks:
            if inputs_match_trigger(callback.inputs,callback.states,dash.callback_context.triggered):
                values_pos=[inputs_states.index(x) for x in (callback.inputs+callback.states)]
                output_pos=[outputs.index(x) for x in (callback.outputs)]
                fcn_res=callback.fcn(*[values[i] for i in values_pos])
                if fcn_res is not None:
                    callback_outputs=list(tuple(fcn_res)) if type(fcn_res) in [list,tuple] else [fcn_res]
                    if callback.format_output:
                        callback_outputs=callback.format_output(callback_outputs)
                    for callback_i,output_i in enumerate(output_pos):
                        res[output_i]=callback_outputs[callback_i]
                    break
        return tuple(res)

def generate_all_callbacks(app,verbose=0):
    callbacks=Callback.get_callbacks()
    if len(callbacks)==0 and callable(app.layout):
        _=app.layout()
        callbacks=Callback.get_callbacks()
    combined_callbacks=[] #list of {'outputs':[],'callbacks':[]}

    for callback in callbacks:
        added_to=None
        for i,combined_callback in enumerate(combined_callbacks):
            if combined_callback is not None and any([x in combined_callback['outputs'] for x in callback.outputs]):
                if added_to is not None:
                    combined_callbacks[added_to]['outputs']+=[x for x in combined_callbacks[i]['outputs'] if x not in combined_callbacks[added_to]['outputs']]
                    combined_callbacks[added_to]['callbacks'].extend(combined_callbacks[i]['callbacks'])
                    combined_callbacks[i]=None
                else:
                    added_to=i
                    combined_callbacks[i]['outputs']+=[x for x in callback.outputs if x not in combined_callbacks[i]['outputs']]
                    combined_callbacks[i]['callbacks'].append(callback)
        if added_to is None:
            combined_callbacks.append({'outputs':copy(callback.outputs),'callbacks':[callback]})

    for combined_callback in [x for x in combined_callbacks if x is not None]:
        output_states=combined_callback['outputs']

        unique_inputs=[];unique_states=[]
        for callback in combined_callback['callbacks']:
            unique_inputs.extend([x for x in callback.inputs if x not in unique_inputs])
            unique_states.extend([x for x in callback.states if x not in (unique_states+output_states)])
        unique_states.extend(output_states)
        unique_inputs_states=unique_inputs+unique_states

        cur_outputs=copy(output_states);cur_inputs=copy(unique_inputs);cur_states=copy(unique_states);cur_callbacks=copy(combined_callback["callbacks"])
        if verbose>0:
            app.logger.warning(f'All Outputs = {cur_outputs}\nAll Inputs = {cur_inputs}\nAll States = {cur_states}')
            app.logger.warning("Callbacks = \n\t"+"\n\t".join([str(x) for x in cur_callbacks])+"\n\n")

        generate_callback(app,cur_outputs,cur_inputs,cur_states,callbacks=cur_callbacks)
