import pickle
import os 
import hashlib
from datetime import datetime
import pandas as pd

def cache_output(path=None,max_cache_seconds=259200):
    if path is None:
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'cache')
        if not os.path.isdir(path):
            os.mkdir(path)
            
    def wrap(fcn):
        def rtn(*args,**kwargs):
            # Remove old cached files
            max_time_path=os.path.join(path,'max times.csv')
            if os.path.isfile(max_time_path):
                df=pd.read_csv(max_time_path)
                remove_pos=df.remove_timestamp<=datetime.now().timestamp()
                if df[remove_pos].shape[0]>0:
                    for index,row in df[remove_pos].iterrows():
                        file_path=os.path.join(path,row.file)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                    df[~remove_pos].to_csv(max_time_path,index=False)
            else:
                with open(max_time_path, 'w') as f:
                    f.write('file,remove_timestamp\n')
            
            #Check if hashed file already exists
            identifier_text=f'{fcn.__name__}-{type(fcn).__mro__}-{args}-{kwargs}'
            hash = hashlib.md5(identifier_text.encode()).hexdigest()
            save_file=f'{hash}.pkl'
            full_path=os.path.join(path,save_file)
            if os.path.isfile(full_path):
                with open(full_path, 'rb') as f:
                    return pickle.load(f)
                
            #Get and writer results to hash file
            res=fcn(*args,**kwargs)
            with open(full_path, 'wb') as f:
                pickle.dump(res, f, pickle.HIGHEST_PROTOCOL)
            
            #Append removal time for hashed file
            if max_cache_seconds is not None:
                with open(max_time_path, 'a') as f:
                    f.write(f'{save_file},{datetime.now().timestamp()+max_cache_seconds}\n')
            return res
        return rtn
    return wrap