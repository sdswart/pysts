import inspect
import functools
import numpy as np
import pandas as pd
from datetime import datetime
from flask import Flask
import json
import copy
from flask_restful import reqparse, abort, Api, Resource, fields, marshal_with, marshal
from collections import OrderedDict

def get_class_that_defined_method(meth):
    meth=meth.func if isinstance(meth, functools.partial) else meth
    if hasattr(meth,'__self__'):
        return meth.__self__ if inspect.isclass(meth.__self__) else meth.__self__.__class__
    meth = getattr(meth, '__func__', meth)  # fallback to __qualname__ parsing
    if inspect.isfunction(meth):
        cls = getattr(inspect.getmodule(meth),
                      meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0],
                      None)
        if isinstance(cls, type):
            return cls
    return getattr(meth, '__objclass__', None)  # handle special descriptor objects

get_is_classmethod = lambda fcn: type(fcn)==classmethod or (inspect.isclass(fcn.__self__) if hasattr(fcn,'__self__') else False)
def get_required_params(fcn,ignore_defaults=False):
    if isinstance(fcn,inspect.Signature):
        sig=fcn
    else:
        fcn=getattr(getattr(fcn,'__self__',None), '__func__', fcn)
        sig=inspect.signature(fcn)
    return {key:(param.annotation if param.annotation!=inspect._empty else None) for key,param in sig.parameters.items() if key not in ['self','cls'] and param.kind==1 and (ignore_defaults or param.default==inspect._empty)}

def join_url(*urls):
    parts=[x for url in urls for x in url.strip('/').split('/')]
    return '/'+'/'.join([x for x in parts if x!=''])


def convert_to_pytype(obj,recurse=True):
    if isinstance(obj,np.ndarray) or isinstance(obj,pd.Series):
        obj=obj.tolist()
    elif isinstance(obj,pd.DataFrame):
        obj=obj.values.tolist()
    elif 'numpy' in type(obj).__name__ and hasattr(obj,'item'):
        obj=obj.item()
    elif hasattr(obj,'to_pydatetime'):
        obj=obj.to_pydatetime()
    elif isinstance(obj, float) and np.isnan(obj):
        obj=None

    if recurse:
        if type(obj) in [list,tuple]:
            obj=[convert_to_pytype(x) for x in obj]
        elif type(obj) in [dict,OrderedDict]:
            obj={key:convert_to_pytype(val) for key,val in obj.items()}
    return obj

def is_json_serializable(obj):
    try:
        _=json.dumps(obj)
        return True
    except:
        return False

allowed_api_kinds=['get','post','put','patch','delete']

def get_current_resource(html_methods,**props):
    def get_result(self,_kind,*args,**kwargs):
        if _kind in self.html_methods:
            return self.html_methods[_kind](*args,**kwargs)
        return f'Method {_kind} is not avaialble for this resource',404
    return type(
        'APIResource',
        (Resource,),
        {
            'html_methods':html_methods,
            'get_result':get_result,
            'get': (lambda self,*args,**kwargs: self.get_result('get',*args,**kwargs)),
            'post': (lambda self,*args,**kwargs: self.get_result('post',*args,**kwargs)),
            'put': (lambda self,*args,**kwargs: self.get_result('put',*args,**kwargs)),
            'patch': (lambda self,*args,**kwargs: self.get_result('patch',*args,**kwargs)),
            'delete': (lambda self,*args,**kwargs: self.get_result('delete',*args,**kwargs)),
            **props
        }
    )

class APIFcn(object):
    def __init__(self,api=None,app=None,kind='get'):
        _=type(self).get_set_app_properties(app=app,api=api)
        self._kind=kind
    def generate_api_resources(self,app=None,api=None):
        type(self)._generate_api_resources(app=app,api=api)
    def generate_and_run(self,*args,**kwargs):
        if not self.api_generated:
            self.generate_api_resources()

        self.app.run(*args,**kwargs)
    @property
    def api_fcns(self):
        return type(self).get_api_fcns()
    @property
    def api_resources(self):
        return type(self).get_api_resources()
    @property
    def app(self):
        return type(self).get_set_app_properties()['app']
    @property
    def api(self):
        return type(self).get_set_app_properties()['api']
    @property
    def api_generated(self):
        return type(self).get_set_app_properties()['api_generated'] is True
    def __getattribute__(self,name):
        if name in ['get','post','put','delete','patch']:
            return type(self)(kind=name)
        else:
            return object.__getattribute__(self,name)

    @classmethod
    def add_standard_class_get_fcn(cls,mclass):
        api_fcns=cls.get_api_fcns()
        class_name=mclass.__name__
        endpoint=class_name.lower()
        if mclass not in api_fcns or endpoint not in api_fcns[mclass] or 'get' not in api_fcns[mclass][endpoint]:
            if mclass not in api_fcns:
                api_fcns[mclass]={}
            if endpoint not in api_fcns[mclass]:
                api_fcns[mclass][endpoint]={}
            endpoints={}

            for cur_endpoint,props in api_fcns[mclass].items():
                endpoint_methods={key:val for key,val in props.items() if key in allowed_api_kinds}
                if len(endpoint_methods)>0:
                    url=None
                    is_classmethods=[x['is_classmethod'] for x in endpoint_methods.values() if 'is_classmethod' in x]
                    if len(is_classmethods)>0:
                        url_details=cls.get_url_details(mclass=mclass,endpoint=cur_endpoint,is_classmethod=is_classmethods[0])
                    if url_details is not None and 'url' in url_details:
                        url=url_details['url']
                    endpoints[cur_endpoint]={
                        'url_format':url,
                        'methods':{key:val['fcn'].__name__ for key,val in endpoint_methods.items() if 'fcn' in val}
                    }
            def class_get(cls,*args,**kwargs):
                return cls._class_info
            setattr(mclass,'_class_info',dict(
                    description=f'Available endpoints for {class_name}',
                    endpoints=endpoints.copy()
                )
            )
            setattr(mclass,'class_get',classmethod(class_get))
            fcn_props=cls.get_fcn_defaults(fcn=mclass.class_get,
                class_name=mclass.__name__,
                mclass=mclass,
                is_classmethod=True,
                endpoint=endpoint,
                required_params=[]
            )
            fcn_props['mfields']={
                'description':fields.String,
                'endpoints':fields.Raw,
            }
            fcn_props['add_properties']=False
            api_fcns[mclass][endpoint]['get']=fcn_props

    @classmethod
    def process_api_fcns(cls): #defer processing until module is ready
        raw_api_fcns=cls.get_raw_api_fcns()
        api_fcns=cls.get_api_fcns()
        while len(raw_api_fcns)>0:
            props=raw_api_fcns.pop()
            fcn_props=cls.get_fcn_defaults(**props)
            mclass=fcn_props['mclass']
            endpoint=fcn_props['endpoint']
            kind=props['kind']

            if mclass not in api_fcns:
                api_fcns[mclass]={}
                cls.add_url_kwargs_to_apiclass_class_attrs(mclass) #Once per api class
            if endpoint not in api_fcns[mclass]:
                api_fcns[mclass][endpoint]={}

            assert kind not in api_fcns[mclass][endpoint], f'Attemped to add the {kind} API to the {endpoint} endpoint of {class_name} but it already exists'
            api_fcns[mclass][endpoint][kind]=fcn_props
        #make sure each class has the standard class get fcn if nothing added
        for mclass in api_fcns.keys():
            cls.add_standard_class_get_fcn(mclass)

    def __call__(self,*args,**kwargs):
        def new_fcn(fcn,kind=None,resource=None,mfields=None,add_properties=True,include=None,ignore=None,prefix=None,returns=None):
            kind=kind or self._kind
            raw_api_fcns=type(self).get_raw_api_fcns()
            raw_api_fcns.append(dict(fcn=fcn,kind=kind,resource=resource,mfields=mfields,
                add_properties=add_properties,include=include,ignore=ignore,
                prefix=prefix,returns=returns))
            self._kind='get'
            return fcn
        def decorator(fcn):
            return new_fcn(fcn,*args,**kwargs)

        if len(args)==1 and len(kwargs)==0:
            return new_fcn(args[0])
        else:
            return decorator

    @classmethod
    def get_set_app_properties(cls,app=None,api=None,api_generated=None):
        if not hasattr(cls, '_cls_app_properties'):
            cls._cls_app_properties = {'api_generated':False}
        if api is not None and api!=cls._cls_app_properties.get('api', None):
            cls._cls_app_properties['api_generated']=False
        cls._cls_app_properties['app']=app or cls._cls_app_properties.get('app', None) or Flask(__name__)
        cls._cls_app_properties['api']=api or cls._cls_app_properties.get('api', None) or Api(cls._cls_app_properties['app'])
        if api_generated is not None:
            cls._cls_app_properties['api_generated']=api_generated
        return cls._cls_app_properties

    @classmethod
    def _generate_api_resources(cls,app=None,api=None):
        api=cls.get_set_app_properties(app=app,api=api)['api']
        cls.process_api_fcns()
        api_fcns=cls.get_api_fcns()
        for mclass,mclass_props in api_fcns.items():
            print(f'Class) {mclass}')
            for endpoint,endpoint_props in mclass_props.items():
                print('  '+f'endpoint) {endpoint}')
                if len(endpoint_props)>0:
                    html_methods={}
                    for kind in list(endpoint_props.keys()):
                        if kind in allowed_api_kinds:
                            props=endpoint_props[kind]
                            props['kind']=kind #required for post_detials
                            props['refresh']=True #Override if needed
                            details=cls.get_url_details(**props) if kind=='get' else cls.get_post_details(**props)
                            #print(('  '*2)+f'kind) {kind} with details = {details}')
                            def final_api_fcn(url_result_fcn,api_props,*args,**kwargs):
                                fcn_result=url_result_fcn(*args,**kwargs)
                                return cls.fcn_result_to_api_output(fcn_result,**api_props)
                            final_api_fcn=functools.partial(final_api_fcn,url_result_fcn=details['url_result_fcn'],api_props=copy.deepcopy(props))
                            html_methods[kind]=final_api_fcn
                    url_details=cls.get_url_details(mclass=mclass,endpoint=endpoint)
                    print(f"    Registering API with endpoint = {url_details['endpoint']}, and url = {url_details['url']} and methods = {', '.join([key+':'+(val['fcn'].__name__ if 'fcn' in val else 'standard') for key,val in endpoint_props.items() if key in allowed_api_kinds])}")
                    CurrentResource=get_current_resource(
                        html_methods=html_methods.copy(),
                        source_details={key:cls.get_source_details(mclass=mclass,endpoint=endpoint,kind=key) for key in api_fcns[mclass][endpoint].keys() if key in allowed_api_kinds}
                    )
                    api.add_resource(CurrentResource, url_details['url'], endpoint=url_details['endpoint'])
                    endpoint_props['resource']=CurrentResource
        _=cls.get_set_app_properties(api_generated=True)

    @classmethod
    def get_raw_api_fcns(cls):
        if not hasattr(cls, '_cls_raw_api_fcns'):
            cls._cls_raw_api_fcns = []
        return cls._cls_raw_api_fcns

    @classmethod
    def get_api_fcn_names(cls,mclass=None):
        api_fcns=cls.get_api_fcns()
        if mclass is None:
            mclasses=list(api_fcns.keys())
        else:
            if type(mclass) not in [list,tuple]:
                mclass=[mclass]
            mclasses=mclass
        return [f'{mclass.__name__}.{props["fcn"].__name__}' for mclass,class_props in api_fcns.items() if mclass in mclasses for endpoint,endpoint_props in class_props.items() for kind,props in endpoint_props.items() if key in allowed_api_kinds and 'fcn' in props]

    @classmethod
    def get_api_fcns(cls):
        if not hasattr(cls, '_cls_api_fcns'):
            cls._cls_api_fcns = {}
        return cls._cls_api_fcns

    @classmethod
    def get_api_classes(cls):
        return {mclass.__name__:mclass for mclass in cls.get_api_fcns().keys()}

    @classmethod
    def get_api_resources(cls):
        return {mclass:{endpoint:props.get('resource',None) for endpoint,props in classprops.items()} for mclass,classprops in cls.get_api_fcns().items()}

    @classmethod
    def nested_field(cls,obj):
        for_objs=obj
        if type(obj) in [list,tuple]:
            if len(obj)==0:
                return {}
            obj=obj[0]

        api_fcns=cls.get_api_fcns()
        props=api_fcns.get(type(obj),{}).get(type(obj).__name__.lower()+'_instance',{}).get('get',{})
        include=props.get('include',None) or (None if (mfields:=props.get('mfields',None)) is None else list(mfields.keys()))
        mfields=cls.get_object_mfields(obj,add_properties=False,add_nested=False,for_objs=for_objs,
            include=include,ignore=props.get('ignore',None),check_api_props=False
        )
        fields_classes=[val for val in fields.__dict__.values() if inspect.isclass(val) and val!=fields.Nested]
        mfields={key:val for key,val in mfields.items() if val in fields_classes}

        mclass=type(obj)
        APIClasses=cls.get_api_classes()
        if mclass in list(APIClasses.values()): #Add url
            mfields['uri']=fields.Url(mclass.__name__.lower()+'_instance',absolute=True)
        return mfields

    @classmethod
    def get_object_mfields(cls,obj,process_if_class=True,add_nested=True,add_properties=True,mfields=None,include=None,ignore=None,check_api_props=True,
                        add_fields=None,is_nested=False):
        if type(mfields) in [dict,OrderedDict]:
            if not is_nested:
                return fields.Nested(mfields)
            elif ignore is None:
                include=list(mfields.keys())

        prop_mapping={
            str:fields.String,
            datetime:fields.DateTime,
            int:fields.Integer,
            float:fields.Float,
            bool:fields.Boolean,
        }
        obj=convert_to_pytype(obj,recurse=False)

        dict_res={}
        if type(obj) in prop_mapping:
            return prop_mapping[type(obj)]
        elif type(obj) in [list,tuple] and len(obj)>0 and all([type(x)==type(obj[0]) for x in obj]):
            for cur_obj in obj:
                res=cls.get_object_mfields(cur_obj,process_if_class=process_if_class,
                    add_nested=False,add_properties=False,
                    mfields=mfields,include=include,ignore=ignore,check_api_props=check_api_props,
                    add_fields=add_fields,is_nested=True)
                if res is not None:
                    return fields.List(res)
        elif type(obj) in [dict,OrderedDict]:
            for key,val in obj.items():
                if key not in dict_res and not key.startswith('_') and (include is None or key in include) and (ignore is None or key not in ignore):
                    val_res=cls.get_object_mfields(val,process_if_class=process_if_class, add_nested=False,
                        add_properties=False,check_api_props=check_api_props,is_nested=True)
                    if val_res is not None:
                        dict_res[key]=val_res
        elif process_if_class and hasattr(obj,'__dict__'):
            #Check if obj in APIFcns
            api_fcns=cls.get_api_fcns()
            if type(obj) in api_fcns:
                props=api_fcns.get(type(obj),{}).get(type(obj).__name__.lower()+'_instance',{}).get('get',{})
                if mfields is None and check_api_props:
                    mfields=props.get('mfields',None)
                if include is None and check_api_props:
                    include=props.get('include',None)
                if ignore is None and check_api_props:
                    ignore=props.get('ignore',None)
                if is_nested:
                    if add_fields is None:
                        add_fields={}
                    add_fields['uri']=fields.Url(type(obj).__name__.lower()+'_instance',absolute=True)

            classes=type(obj).__mro__
            obj_dicts=[obj.__dict__]+[x.__dict__ for x in classes if hasattr(x,'__dict__')]
            for obj_dict in obj_dicts:
                for key,val in obj_dict.items():
                    if key not in dict_res and not key.startswith('_') and (include is None or key in include) and (ignore is None or key not in ignore):
                        if add_properties and type(val)==property:
                            val=getattr(obj,key)
                        val_res=cls.get_object_mfields(val, process_if_class=add_nested, add_nested=False,add_properties=False,
                            check_api_props=check_api_props,is_nested=True)
                        if val_res is not None:
                            dict_res[key]=val_res

        if len(dict_res)>0:
            if add_fields is not None:
                dict_res.update(add_fields)
            if is_nested:
                dict_res=fields.Nested(dict_res)
            return dict_res

    @classmethod
    def add_url_kwargs_to_apiclass_class_attrs(cls,mclass):
        mclass.__getattribute__=lambda self,name: cls.add_url_kwargs_to_apiclass_instance(super(mclass,self).__getattribute__(name))

    @classmethod
    def add_url_kwargs_to_apiclass_instance(cls,obj):
        """
            Important: This assumes that the kwargs passed to the mclass init method is stored with the same name in the instance attributes
        """
        if type(obj) in [list,tuple]:
            return [cls.add_url_kwargs_to_apiclass_instance(x) for x in obj]
        elif isinstance(obj,dict):
            return {key:cls.add_url_kwargs_to_apiclass_instance(val) for key,val in obj.items()}

        api_fcns=cls.get_api_fcns()
        APIClasses=cls.get_api_classes()
        mclass=type(obj)
        get_props=api_fcns.get(mclass,{}).get(mclass.__name__.lower()+'_instance',{}).get('get',{})
        ignore=get_props.get('ignore',None)
        if ignore is None:
            get_props['ignore']=ignore=[]
        if mclass in list(APIClasses.values()):
            url_details=cls.get_url_details(mclass,fcn=mclass.__init__)
            url_kwargs_from_instance=url_details['url_kwargs_from_instance'] #{url_param: class init param}
            for key,val in url_kwargs_from_instance.items():
                if not hasattr(obj,key):
                    props=val.split('.')
                    cur_obj=obj
                    add_to_obj=True
                    for prop in props:
                        if hasattr(cur_obj,prop):
                            cur_obj=getattr(cur_obj,prop)
                        else:
                            add_to_obj=False
                            break
                    if add_to_obj:
                        setattr(obj,key,cur_obj)
                        ignore.append(key)
        return obj

    @classmethod
    def fcn_result_to_api_output(cls,fcn_result,kind='get',html_code=None,add_properties=True,add_nested=True,**kwargs):
        if type(fcn_result) in [list,tuple] and len(fcn_result)==2 and isinstance(fcn_result[1],int): #already processed as an html output
            html_code=html_code[1]
            fcn_result=fcn_result[0]
        if fcn_result is None:
            fcn_result=''
        APIClasses=cls.get_api_classes()
        if html_code is None:
            if fcn_result=='':
                html_code=204
            elif kind == 'put' and type(fcn_result) in ([dict]+list(APIClasses.values())):
                html_code=201
            else:
                html_code=200
        if isinstance(fcn_result,str):
            return fcn_result, html_code

        mfields=cls.get_object_mfields(fcn_result,add_properties=add_properties,add_nested=add_nested,check_api_props=True)
        if mfields is None and is_json_serializable(fcn_result):
            return fcn_result, html_code
        elif mfields is not None:
            fcn_result=cls.add_url_kwargs_to_apiclass_instance(fcn_result)
            if isinstance(mfields,dict):
                return marshal(fcn_result,mfields)
            else:
                return marshal(dict(marshal_envelope=fcn_result),dict(marshal_envelope=mfields))['marshal_envelope']
        return str(fcn_result), html_code

    @classmethod
    def get_class_from_name(cls,class_name):
        APIClasses={name.lower():mclass for name,mclass in cls.get_api_classes().items()}
        return APIClasses.get(class_name.lower(),None)

    @classmethod
    def get_params_for_fcn(cls,fcn,ignore_defaults=False,is_classmethod=None,mclass=None,required_params=None):
        params = required_params or get_required_params(fcn,ignore_defaults=ignore_defaults)
        res=[]
        for param,annotation in params.items():
            if annotation is not None and inspect.isclass(annotation):
                matching_class=annotation
            else:
                matching_class=cls.get_class_from_name(param)

            if matching_class is not None:
                matching_class_res=cls.get_params_for_fcn(matching_class.__init__,ignore_defaults=False,is_classmethod=False,mclass=matching_class)
                res.append({'param':param,
                            'class':matching_class,
                            'class_params':matching_class_res})
            else:
                res.append(param)
        return res

    @classmethod
    def get_fcn_defaults(cls,fcn=None,class_name=None,mclass=None,is_classmethod=None,resource=None,prefix=None,returns=None,endpoint=None,required_params=None,**kwargs):
        if class_name is None and isinstance(mclass, str):
            class_name=mclass
            mclass=None
        if mclass is None:
            if class_name is None and fcn is not None and not isinstance(fcn,str):
                mclass=get_class_that_defined_method(fcn)
            elif class_name is not None:
                mclass=cls.get_class_from_name(class_name)
        if class_name is None:
            class_name=mclass.__name__

        if type(fcn)==classmethod:
            fcn=fcn.__func__

        if isinstance(fcn,str):
            fcn=getattr(mclass,fcn)
        elif fcn is None:
            fcn=mclass.__call__ if is_classmethod is None or is_classmethod is True else mclass.__init__ #Default to the standard call fcn
        else: #Make sure fcn is bound to mclass
            fcn=getattr(mclass,fcn.__name__)

        assert fcn is not None, f'Could not identify the default function with mclass = None'
        if is_classmethod is None:
            is_classmethod=get_is_classmethod(fcn)

        inner_fcn=getattr(getattr(fcn,'__self__',None), '__func__', fcn)
        sig=inspect.signature(inner_fcn)
        required_params=required_params or get_required_params(sig)
        returns=returns if returns is not None else (sig.return_annotation if sig.return_annotation!=inspect._empty else None)
        resource=resource or ('_class' if is_classmethod else '_instance')
        if endpoint is None:
            endpoint=class_name.lower()
            if resource!='_class':
                endpoint+=('' if resource.startswith('_') else '_')+resource
        prefix=prefix or "/".join([x for x in endpoint.split('_') if x not in ['index','class','instance'] and (is_classmethod or x!=class_name.lower())])
        res=dict(
            class_name=class_name,
            mclass=mclass,
            fcn=fcn,
            is_classmethod=is_classmethod,
            inner_fcn=inner_fcn,
            sig=sig,
            required_params=required_params,
            returns=returns,
            resource=resource,
            endpoint=endpoint,
            prefix=prefix
        )
        res.update({key:val for key,val in kwargs.items() if key not in res})
        return res
    @classmethod
    def get_url_result_fcn(cls,get_obj,fcn_name,kwargs_sources,params_from_kwargs):
        def rtn(get_obj,fcn_name,kwargs_sources,params_from_kwargs, *args, **kwargs):
            if not isinstance(fcn_name,str):
                fcn_name=fcn_name.__name__
            obj=get_obj(*args, **kwargs)
            if fcn_name=='__call__':
                return obj
            for kwargs_fcn in kwargs_sources:
                kwargs=kwargs_fcn(*args, **kwargs)
            for params in params_from_kwargs:
                kwargs.update({key:(kwargs[val] if isinstance(val,str) else val(**kwargs)) for key,val in params.items()})
            fcn=getattr(obj,fcn_name)

            sig=inspect.signature(fcn)
            params={key:param.kind for key,param in sig.parameters.items() if key not in ['self','cls']}
            if 4 not in list(params.values()): #Has **keyword param
                kwargs={key:val for key,val in kwargs.items() if key in params}
            if fcn_name=='__init__':
                fcn=obj.__call__
            return fcn(**kwargs)
        return functools.partial(rtn,get_obj=get_obj,fcn_name=fcn_name,kwargs_sources=kwargs_sources,params_from_kwargs=params_from_kwargs)
    @classmethod
    def get_source_details(cls,get_details_fcn=None,kind='get',mclass=None,endpoint=None,fcn=None,prefix=None,is_classmethod=None,required_params=None,resource=None,refresh=False):
        fcn_props=cls.get_fcn_defaults(fcn=fcn,mclass=mclass,is_classmethod=is_classmethod,resource=resource,prefix=prefix,endpoint=endpoint,required_params=required_params)
        mclass=fcn_props['mclass']
        fcn=fcn_props['fcn']
        is_classmethod=fcn_props['is_classmethod']
        endpoint=fcn_props['endpoint']
        prefix=fcn_props['prefix']
        required_params=fcn_props['required_params']

        api_fcns=cls.get_api_fcns()
        res=api_fcns.get(mclass,{}).get(endpoint,{}).get(kind,{}).get('source_details',None)
        if get_details_fcn is not None and (refresh or res is None):
            res=get_details_fcn(**fcn_props)
            api_fcns[mclass]=api_fcns.get(mclass,{})
            api_fcns[mclass][endpoint]=api_fcns[mclass].get(endpoint,{})
            api_fcns[mclass][endpoint][kind]=api_fcns[mclass][endpoint].get(kind,{})
            api_fcns[mclass][endpoint][kind]['source_details']=res
        return copy.deepcopy(res)
    @classmethod
    def get_url_details(cls,mclass,endpoint=None,fcn=None,prefix=None,is_classmethod=None,required_params=None,resource=None,refresh=False,**kwargs):
        def get_details_fcn(fcn,is_classmethod,mclass,required_params,endpoint,prefix,**kwargs):
            fcn_params=cls.get_params_for_fcn(fcn,ignore_defaults=False,is_classmethod=is_classmethod,mclass=mclass,required_params=required_params)
            #determine url url_details
            url='/'
            if is_classmethod:
                get_obj=lambda *args,**kwargs: mclass
                param_order=[]
                params_from_url={}
                url_kwargs_from_instance={}
            else:
                class_url_details=cls.get_url_details(mclass,fcn=mclass.__call__,refresh=True)
                prefix=url if prefix is None else join_url(class_url_details['url'],prefix)
                get_obj=lambda *args,**kwargs: class_url_details['url_result_fcn'](*args,**kwargs)
                param_order=class_url_details['param_order']
                params_from_url=class_url_details['params_from_url']
                url_kwargs_from_instance=class_url_details['url_kwargs_from_instance']
                #remove params already from base class
                fcn_params=[x for x in fcn_params if (x if isinstance(x,str) else x['param']) not in params_from_url]

            # URL PARAMS
            ordered_params=sorted(fcn_params,key=lambda x: isinstance(x,str)*1)
            add_fcn_prefix = prefix is not None and prefix != ''
            for param in ordered_params:
                if isinstance(param,str):
                    if add_fcn_prefix:
                        url = join_url(url,prefix)
                        add_fcn_prefix=False
                    url_param=f'{endpoint}_{param}'
                    url = join_url(url,f'/<{url_param}>')
                    params_from_url[param]=url_param
                    param_order.append(url_param)
                    url_kwargs_from_instance[url_param]=param
                else:
                    class_details=cls.get_url_details(param['class'],fcn=param['class'].__init__)
                    url = join_url(url,class_details['url'])
                    params_from_url[param['param']]=class_details['url_result_fcn']
                    for key,val in class_details['url_kwargs_from_instance'].items():
                        url_kwargs_from_instance[key]=param['param']+"."+val
                    param_order.extend(class_details['param_order'])

            get_kwargs_from_url=lambda *args,**kwargs: {**kwargs,**{param_order[i]:args[i] for i in range(len(args)) if i<len(param_order)}}
            if add_fcn_prefix:
                url = join_url(url,prefix)

            url_result_fcn=cls.get_url_result_fcn(get_obj,fcn,kwargs_sources=[get_kwargs_from_url],params_from_kwargs=[params_from_url])
            return dict(
                get_obj=get_obj,
                url=url,
                endpoint=endpoint,
                get_kwargs_from_url=get_kwargs_from_url,
                param_order=param_order,
                params_from_url=params_from_url,
                url_result_fcn=url_result_fcn,
                url_kwargs_from_instance=url_kwargs_from_instance
            )
        return cls.get_source_details(get_details_fcn,kind='get',mclass=mclass,endpoint=endpoint,fcn=fcn,prefix=prefix,is_classmethod=is_classmethod,required_params=required_params,resource=resource,refresh=refresh)
    @classmethod
    def get_post_details(cls,mclass,kind,endpoint=None,fcn=None,prefix=None,is_classmethod=None,required_params=None,resource=None,refresh=False,**kwargs):
        def get_details_fcn(fcn,is_classmethod,mclass,required_params,endpoint,prefix,class_name=None,**kwargs):
            url_details=cls.get_url_details(mclass,endpoint=endpoint,prefix=prefix,is_classmethod=is_classmethod,resource=resource,refresh=refresh)
            get_kwargs_from_url=url_details['get_kwargs_from_url']
            params_from_url=url_details['params_from_url']
            get_obj=url_details['get_obj']
            fcn_name=fcn.__name__
            if class_name is None:
                class_name=mclass.__name__

            fcn_params=cls.get_params_for_fcn(fcn,ignore_defaults=False,is_classmethod=is_classmethod,mclass=mclass,required_params=required_params)
            fcn_params=[x for x in fcn_params if (x if isinstance(x,str) else x['param']) not in params_from_url]

            post_parser = reqparse.RequestParser()
            def add_params_to_parser(params,sub_class_name=None):
                res={}
                for param in params:
                    if isinstance(param,str):
                        if sub_class_name is None:
                            res[param]=param
                            post_parser.add_argument(param, help=f'{param.capitalize()} for the {class_name}\'s {fcn_name} function')
                        else:
                            post_param=f'{sub_class_name.lower()}_{param}'
                            res[param]=post_param
                            post_parser.add_argument(post_param, help=f'{param.capitalize()} of the {sub_class_name} for the {class_name}\'s {fcn_name} function')
                    else:
                        class_param_res=add_params_to_parser(param['class_params'],param['class'].__name__)
                        res[param['param']]=lambda *args,**kwargs: param['class'](**{key:(kwargs[val] if isinstance(val,str) else val(**kwargs)) for key,val in class_param_res.items()})
                return res
            params_from_post=add_params_to_parser(fcn_params)
            get_kwargs_from_post=lambda *args,**kwargs: post_parser.parse_args()

            url_result_fcn=cls.get_url_result_fcn(get_obj,fcn,kwargs_sources=[get_kwargs_from_url,get_kwargs_from_post],params_from_kwargs=[params_from_url,params_from_post])
            return dict(
                get_kwargs_from_post=get_kwargs_from_post,
                params_from_post=params_from_post,
                url_result_fcn=url_result_fcn
            )
        return cls.get_source_details(get_details_fcn,kind=kind,mclass=mclass,endpoint=endpoint,fcn=fcn,prefix=prefix,is_classmethod=is_classmethod,required_params=required_params,resource=resource,refresh=refresh)
