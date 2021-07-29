import pandas as pd
from mongoengine import *
import json

from mongoengine.queryset.queryset import QuerySet
def to_df(self,exclude=None):
    if exclude is None: exclude=['_id']
    return pd.DataFrame.from_records(json.loads(self.to_json()),exclude=exclude)
QuerySet.to_df=to_df

def Document(DynamicDocument):
    pass
