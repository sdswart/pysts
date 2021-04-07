import logging
import types
import os
import io

from .object_wrapper import put_object, get_object, list_objects, copy_object, delete_object, delete_objects, put_acl, get_acl, object_exists, open_object
from .bucket_wrapper import bucket_exists, get_buckets, get_bucket
from .utils import data_to_bytes

logger = logging.getLogger(__name__)

class S3Storage(object):
    def __init__(self,config,verify=True):
        self.region=config.AWS_DEFAULT_REGION
        self.bucket=self.get_bucket(config.BUCKET_NAME,verify=verify)
    def get_bucket(self,name,verify=True):
        if not verify or bucket_exists(name):
            return get_bucket(name,self.region)
        logger.warning("Verification of bucket '%s' failed",name)
        raise

    def get_buckets(self):
        return get_buckets()
    def exists(self,path):
        return object_exists(self.bucket,path)
    def open(self,path,mode='rb'):
        return open_object(self.bucket,path,mode=mode)
    def save(self,path,data):
        put_object(self.bucket,path,data)
    def delete(self,path):
        return delete_object(self.bucket,path)
    def list(self,prefix=None):
        return list_objects(self.bucket,prefix)

class LocalStorage(object):
    exists=lambda self,*args,**kwargs: os.path.exists(*args,**kwargs)
    open=lambda self,path,mode='r': io.open(path,mode)
    delete=lambda self,*args,**kwargs: os.remove(*args,**kwargs)
    list=lambda self,*args,**kwargs: os.listdir(*args,**kwargs)
    def save(self,path,data):
        data = data_to_bytes(data)
        with open(path,'wb') as f:
            f.write(data)
