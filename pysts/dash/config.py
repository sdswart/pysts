import os

env_variables={}
for file in os.listdir('.'):
    if file.lower().endswith('.env'):
        with open(file,'r') as f:
            lines=f.readlines()
        env_variables={splits[0].strip():"".join([x.strip() for x in splits[1:]]) for line in lines if '=' in line for splits in line.split('=')}
        break

def get_variable(name,default=None):
    if name in env_variables: return env_variables[name]
    return os.environ.get(name,default)

class Config(object):
    SECRET_KEY=get_variable('SECRET_KEY','adsfyiu3S3!jE%$axbjhwa195sxc@S')
    def get(self,name,default=None):
        if hasattr(self,name):
            return getattr(self,name)
        return default

config=Config()
