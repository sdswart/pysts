import inspect

def is_list(obj):
    return type(obj) in [list,tuple]

def copy_function_with_params(fcn, add_args=None, **kwargs):
    if add_args is None: add_args=[]
    if type(add_args) not in [list,tuple]: add_args=[add_args]
    new_args={key:val for key,val in kwargs.items() if key in fcn.__code__.co_varnames}
    varnames=add_args+[x for x in fcn.__code__.co_varnames if x not in new_args and x not in add_args]
    defaults={x:fcn.__defaults__[i] for i,x in enumerate(fcn.__code__.co_varnames[fcn.__code__.co_argcount-len(fcn.__defaults__):]) if x in varnames}
    fcn_args=add_args+[x for x in fcn.__code__.co_varnames[:fcn.__code__.co_argcount-len(fcn.__defaults__)] if x in varnames]
    def y():
        mylocals=locals()
        vars={x:mylocals.get(x) for x in varnames}
        vars.update(new_args)
        args=[vars[x] for x in fcn_args]
        kwargs={key:val for key,val in vars.items() if key not in fcn_args}
        return fcn(*args,**kwargs)

    #code(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring,constants, names, varnames, filename, name, firstlineno,lnotab[, freevars[, cellvars]])
    fcn_code = types.CodeType(len(varnames),
                            y.__code__.co_kwonlyargcount,
                            y.__code__.co_nlocals,
                            y.__code__.co_stacksize,
                            y.__code__.co_flags,
                            y.__code__.co_code,
                            y.__code__.co_consts,
                            y.__code__.co_names,
                            tuple(varnames),
                            y.__code__.co_filename,
                            fcn.__code__.co_name,
                            y.__code__.co_firstlineno,
                            y.__code__.co_lnotab,
                            y.__code__.co_freevars,
                            y.__code__.co_cellvars)

    res=types.FunctionType(fcn_code, {**fcn.__globals__,**y.__globals__}, fcn.__code__.co_name,
                            argdefs=tuple([defaults[key] for key in varnames if key in defaults]),
                            closure=y.__closure__)
    res.__doc__=fcn.__doc__
    return res

class Spec(object):
    _defaults=None
    def __init__(self,fcn):
        self._fcn=fcn.__code__.co_varnames
        self.varnames=fcn.__code__.co_varnames
        self.name=fcn.__code__.co_name
        self.spec=inspect.getfullargspec(fcn)
        for spec_var,self_var in ({'args':'args','kwonlyargs':'kwargs','varargs':'varargs','varkw':'varkwargs','annotations':'annotations'}).items():
            setattr(self,self_var,list(spec_var) if is_list(spec_var) else spec_var)
        self.varnames_with_defaults=self.args[:(len(self.spec.defaults) if self.spec.defaults else 0)]+self.kwargs[:(len(self.spec.kwonlydefaults) if self.spec.kwonlydefaults else 0)]
        self.required_args=list(set(self.args+self.kwargs)-set(self.varnames_with_defaults))
    @property
    def defaults(self):
        if self._defaults is None:
            vals=(list(self.spec.defaults) if self.spec.defaults else [])+(list(self.spec.kwonlydefaults) if self.spec.kwonlydefaults else [])
            self._defaults={var:vals[self.varnames_with_defaults.index(var)] for var in self.varnames_with_defaults}
        return self._defaults
    def copy_function_with_params(self,add_args=None, **kwargs):
        return copy_function_with_params(self._fcn, add_args=add_args, **kwargs)
    def need_more_inputs(self,available_inputs=None,return_missing=False):
        if available_inputs is None: available_inputs=['self']
        missing=list(set(self.required_args)-set(available_inputs))
        return missing if return_missing else len(missing)==0

def need_more_inputs(fcn,available_inputs=None,return_missing=False):
    return Spec(fcn).need_more_inputs(available_inputs=available_inputs,return_missing=return_missing)

def describe_fcn(fcn):
    ''' returns a spec for the function with the properties: name, varnames, defaults (for both args and kwargs), args, kwargs, vararg, varkwarg, variables={"name":{"default","type"}} (type is one of [arg, kwarg, vararg, varkwarg])
    '''
    Spec(fcn)
