import os
import secrets

env_file_variables=None
def get_variable(name,default=None):
    global env_file_variables
    if env_file_variables is None:
        env_file_variables={}
        for file in os.listdir('.'):
            if file.lower().endswith('.env'):
                with open(file,'r') as f:
                    lines=f.readlines()
                env_file_variables={splits[0].strip():"".join([x.strip() for x in splits[1:]]) for line in lines if '=' in line for splits in line.split('=')}
                break

    if name in env_file_variables: return env_file_variables[name]
    return os.environ.get(name,default)

class Config(object):
    SECRET_KEY=get_variable('SECRET_KEY',secrets.token_urlsafe(16))
    def get(self,name,default=None):
        if hasattr(self,name):
            return getattr(self,name)
        return default
