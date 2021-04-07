import os
import subprocess
import sys
import pkg_resources
import requests
from datetime import datetime

from .utils import get_cwd

class Lazy_Module(object):
    _released=None
    def __init__(self,module,name=None):
        self._mod_module=module if isinstance(module,Module) else Module(module)
        self._mod_name=name if name else self._mod_module.name
        self.self_names=['_mod_module','_mod_name','_released','_release']
    @property
    def _release(self):
        if not self._released:
            module=self._mod_module.load()
            globals()[self._mod_name]=module
            self._released=module
        return self._released
    def __getattribute__(self,name):
        try:
            return object.__getattribute__(self,name)
        except:
            released=object.__getattribute__(self,'_release')
            return getattr(released,name)
    def __setattribute__(self,name,val):
        try:
            return object.__setattribute__(self,name,val)
        except:
            released=object.__getattribute__(self,'_release')
            return setattr(released,name,val)
    def __call__(self,*args,**kwargs):
        return self._release(*args,**kwargs)
    def __getitem__(self,val):
        return self._release[val]

def lazy_import(obj,*objs,from_path=None):
    objs=[obj]+list(objs)
    if from_path:
        objs=[os.path.join(from_path,x) for x in objs]
    mods=[Lazy_Module(x) for x in objs]
    return mods if len(mods)>1 else mods[0]

class Version(object):
    major=minor=micro=None
    def __init__(self,val):
        labels={0:'major',1:'minor',2:'micro'}
        self.version=val
        if isinstance(val,str):
            self.is_dist=True
            vals=val.split('.')
            for i in range(len(vals)):
                setattr(self,labels[i],int(vals[i]))
        elif isinstance(val,float):
            self.is_dist=False
            d=datetime.fromtimestamp(val)
            for i,attr in enumerate(['year','month','day']):
                setattr(self,labels[i],getattr(d,attr))
    def __repr__(self):
        return '.'.join([str(getattr(self,x)) for x in ['major','minor','micro'] if getattr(self,x) is not None])
    def __str__(self):
        return f'Version ({self.__repr__()})'
    def __lt__(self,val):
        return self.version<val
    def __gt__(self,val):
        return self.version>val
    def __eq__(self,val):
        return self.version==val

class Module(object):
    _path=None
    _name=None
    _module=None
    _dist=None
    def __init__(self,package):
        self.package=package
    def _set_name_path(self):
        if os.path.isdir(self.package) and os.path.isfile(os.path.join(self.package,'__init__.py')):
            self._path=self.package
        elif self.package.endswith('__init__.py') and os.path.isfile(self.package):
            self._path=os.path.dirname(self.package)
        else:
            self._path,self._name=self.find_module(self.package)
        if self._path and not self._name:
            self._name=os.path.basename(self._path)
    @property
    def path(self):
        if not self._path: self._set_name_path()
        return self._path
    @property
    def name(self):
        if not self._name: self._set_name_path()
        return self._name
    @property
    def mtime(self):
        return os.path.getmtime(self.path)
    @property
    def file_loader(self):
        return self._get_file_loader(os.path.dirname(self.path),self.name)
    def _get_file_loader(self,path,name):
        if os.path.isdir(path):
            finder=pkg_resources.get_importer(path)
            return finder.find_module(name)
    @property
    def is_dist(self):
        return self.dist is not None
    @property
    def dist(self):
        if not self._dist:
            try:
                cur_dist = pkg_resources.get_distribution(self.name)
                if os.path.normpath(self.path)==os.path.normpath(cur_dist.location):
                    self._dist = cur_dist
            except:
                pass
        return self._dist
    def find_module(self,name):
        for path in list(sys.path):
            loader=self._get_file_loader(path,name)
            if loader:
                return os.path.dirname(loader.path),loader.name
    @property
    def pypi_info(self):
        return requests.get(f'https://pypi.org/pypi/{self.name}/json').json()['info']
    @property
    def version(self):
        return Version(self.dist.version if self.is_dist else self.mtime)
    @property
    def is_latest(self):
        return self.version==self.latest_version
    @property
    def latest_version(self):
        if self.is_dist:
            return Version(self.pypi_info['version'])
    def load(self):
        if not self._module:
            try:
                self._module=self.file_loader.load_module()
            except Exception as e:
                print(e)
                self._module=__import__(self.name)
        return self._module
    def import_module(self):
        return self.load()
    @property
    def children(self):
        return Modules(paths=[self.path])

class Modules(object):
    def __init__(self,paths=None):
        if paths is None: paths=list(sys.path)
        self.paths=paths
        self._modules=None
    @property
    def modules(self):
        return list(self.get_modules().keys())
    def get_modules(self,refresh=False):
        if self._modules is None or refresh:
            self._modules={}
            for root in self.paths:
                if root=='':
                    root=get_cwd()
                if os.path.isdir(root):
                    for obj in os.listdir(root):
                        path=os.path.join(root,obj)
                        if os.path.isdir(path) and os.path.isfile(os.path.join(path,'__init__.py')):
                            module=Module(path)
                            self._modules[module.name]=module
        return self._modules
    def get(self,name):
        modules=self.get_modules()
        return modules[name] if name in modules else None
    def __getitem__(self,name):
        return self.get(name)
    def __getattribute__(self,name):
        try:
            return object.__getattribute__(self,name)
        except:
            return self.get(name)

def install(package):
    return subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", package])
