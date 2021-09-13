import pytest
import uuid
import pandas as pd
from datetime import datetime, timedelta, timezone

from pysts.db.influxdb import InfluxDB


@pytest.fixture(scope='class')
def db():
    return InfluxDB()

@pytest.fixture(scope='class')
def bucket_name(db):
    _bucket_name='pysts_test'
    bucket=db.get_bucket(_bucket_name,retention_seconds=86400)
    assert bucket.name==_bucket_name,f'Bucket {_bucket_name} not found!'
    yield _bucket_name
    db.delete_bucket(_bucket_name)
    assert not db.bucket_exists(_bucket_name),f'Bucket {bucket_name} not deleted!'


class TestClass:
    '''
    Test MongoEngine
    '''

    def test_store_data(self,db,bucket_name):
        name=str(uuid.uuid4())
        now=datetime.now(timezone.utc)
        times=[now-timedelta(seconds=x) for x in range(5)]
        objs=[{'data':111,'beta':i,'time':t,'location':('A' if i<3 else 'B')} for i,t in enumerate(times)]
        db.add_data(bucket_name,data=objs,measurement=name,tag_columns=['location'])

        df=db.get_data(bucket_name,measurements=name,return_dataframe=True)
        assert df.shape[0]==5, f"5 records expected but returned df.shape = {df.shape}"

        assert all([x in df.columns for x in ['data','beta']]), f'Missing columns in dataframe, which has columns: {df.columns}'

        t0, t1 = db.get_time_range(bucket_name,measurements=name)
        assert t0==min(times) and t1==max(times), f'Start and end times should have been ({min(times)}, {max(times)}) but got ({t0}, {t1}) instead'

    def test_dataframe(self,db,bucket_name):
        print(bucket_name)
        name=str(uuid.uuid4())
        now=datetime.now(timezone.utc)
        times=[now-timedelta(seconds=x) for x in range(5)]
        objs=[{'data':111,'beta':i,'time':t,'location':('A' if i<3 else 'B')} for i,t in enumerate(times)]
        df=pd.DataFrame(objs)
        db.add_data(bucket_name,data=df,measurement=name,tag_columns=['location'])

        df=db.get_data(bucket_name,measurements=name,return_dataframe=True)
        assert df.shape[0]==5, f"5 records expected but returned df.shape = {df.shape}"

        assert all([x in df.columns for x in ['data','beta']]), f'Missing columns in dataframe, which has columns: {df.columns}'

        t0, t1 = db.get_time_range(bucket_name,measurements=name)
        assert t0==min(times) and t1==max(times), f'Start and end times should have been ({min(times)}, {max(times)}) but got ({t0}, {t1}) instead'
