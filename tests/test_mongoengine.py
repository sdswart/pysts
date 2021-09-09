import pytest
import uuid
import pandas as pd
from datetime import datetime, timedelta

from pysts.db.mongodb import Document, fields, QuerySet
from mongoengine import connect

class TestClass:
    '''
    Test MongoEngine
    '''
    def test_connect(self):
        dbname='pysts_tests'
        self.con=connect(dbname, host='l3-37:27017', port=27017)
        self.db=self.con.get_database(dbname)
        assert self.con is not None, 'Connection is None!'

    def test_document(self):
        if not hasattr(self,'con'):
            self.test_connect()

        class TestDoc(Document):
            meta = {'collection': 'TestDoc'}
            name=fields.StringField(max_length=120, required=True)

        self.TestDoc=TestDoc

    def test_update(self):
        if not hasattr(self,'TestDoc'):
            self.test_document()
        name=str(uuid.uuid4())
        doc=self.TestDoc(name=name,info={'a':1}).save()
        #retrieve Doc
        docs=list(self.TestDoc.objects(name=name))
        assert len(docs)>0, f"The new document with name '{name}' could not be found. Result = {docs}"

    def test_update_or_create(self):
        if not hasattr(self,'TestDoc'):
            self.test_document()
        name=str(uuid.uuid4())
        res=self.TestDoc.objects.update_or_create({'name':name},meta_var='hello')
        res=list(res)
        assert len(res)==1, f"update_or_create did not return 1 result for the new doc named '{name}': res = {res}"
        doc=res[0]
        assert res[0].name==name and res[0].meta_var=='hello', f"The returned doc did not have the correct name and metavar"

        docs=list(self.TestDoc.objects(name=name))
        assert len(docs)>0, f"The new document created with update_or_create with name '{name}' could not be found. Result = {docs}"

        #Update meta_var
        res=self.TestDoc.objects.update_or_create({'name':name},meta_var='world')
        docs=list(self.TestDoc.objects(name=name))
        assert len(docs)>0 and docs[-1].meta_var=='world', f"The doc's meta_var could not be updated to 'world'"

        #Test multiple docs
        name2=str(uuid.uuid4())
        res=self.TestDoc.objects.update_or_create([{'name':name},{'name':name2}],meta_var='hello')
        res=list(res)
        assert len(res)==2, f"update_or_create returned {len(res)} docs but should have been 2!"

        docs=list(self.TestDoc.objects(name__in=[name,name2]))
        assert len(docs)==2, f"Finding the 2 docs returned {len(docs)} docs!"

        assert docs[0].meta_var=='hello' and docs[1].meta_var=='hello', "The meta_var for the two docs were not changed to 'hello'!"

    def test_update_or_create_timeseries(self):
        if not hasattr(self,'con'):
            self.test_connect()
        if 'timerow' not in self.db.list_collection_names():
            self.db.create_collection("timerow", timeseries= {'timeField': "Time",'metaField':'table'})
        if not hasattr(self,'Timerow'):
            class Timerow(Document):
                table = fields.StringField()
                Time = fields.DateTimeField(required=True)
            self.Timerow=Timerow

        table1=str(uuid.uuid4())
        now=datetime.now()
        times=[now-timedelta(seconds=x) for x in range(5,0,-1)]
        res=self.Timerow.objects.update_or_create([{'Time':x,'table':table1,'data':i} for i,x in enumerate(times)])
        res=list(res)
        assert len(res)==5, f"update_or_create returned {len(res)} timeseries docs but should have been 5!"

        docs=list(self.Timerow.objects(table=table1))
        assert len(docs)==5, f"Finding the 5 timeseries docs returned {len(docs)} documents!"

        #try offset the time slightly and save again
        table2=str(uuid.uuid4())
        res=self.Timerow.objects.update_or_create([{'Time':x+timedelta(microseconds=1),'table':table2,'data':i} for i,x in enumerate(times)],unique_keys=['Time'])
        res=list(res)
        assert len(res)==5, f"update_or_create returned {len(res)} timeseries docs after slight time offset but should have been 5!"

        #Make sure none of the new offset docs were saved
        docs=list(self.Timerow.objects(table=table2))
        assert len(docs)==0, f"None of the 5 offset timeseries docs with table = {table2} should have been saved, but {len(docs)} were found!"

        #Add times and try save again
        table3=str(uuid.uuid4())
        times=times+[times[-1]+timedelta(seconds=x) for x in range(1,4)]
        res=self.Timerow.objects.update_or_create([{'Time':x,'table':table3,'data':i} for i,x in enumerate(times)],unique_keys=['Time'])
        assert len(res)==8, f"update_or_create returned {len(res)} timeseries docs but should have been 8!"

        docs=list(self.Timerow.objects(table=table3))
        assert len(docs)==3, f"Only 3 docs with table = {table3} should have been saved, but {len(docs)} were found!"

        #Try again but make the time and table unique
        res=self.Timerow.objects.update_or_create([{'Time':x,'table':table3,'data':i} for i,x in enumerate(times)],unique_keys=['Time','table'])
        assert len(res)==8, f"update_or_create returned {len(res)} timeseries docs but should have been 8 (time and table unique)!"

        docs=list(self.Timerow.objects(table=table3))
        assert len(docs)==8, f"All 8 docs with table = {table3} should now have been saved, but {len(docs)} were found!"

        #Test a DataFrame
        table4=str(uuid.uuid4())
        df=pd.DataFrame([{'Time':x,'table':table4,'data':i} for i,x in enumerate(times)])
        res=self.Timerow.objects.update_or_create(files=df,unique_keys=['Time','table'])
        assert len(res)==8, f"update_or_create returned {len(res)} timeseries docs but should have been 8 (Dataframe)!"

        docs=list(self.Timerow.objects(table=table4))
        assert len(docs)==8, f"All 8 docs from the dataframe with table = {table4} should now have been saved, but {len(docs)} were found!"


    def test_dataframes(self):
        if not hasattr(self,'TestDoc'):
            self.test_document()
        records=[dict(name=str(uuid.uuid4()),a=i,b=2*i) for i in range(10)]
        uuids=[x['name'] for x in records]
        df=pd.DataFrame(records)

        self.TestDoc.objects.store_df(df,mata_var='df');

        qs=self.TestDoc.objects(name__in=uuids)
        docs=list(qs)
        assert len(docs)==len(uuids), f"store_df: len of df ({len(uuids)}) doesn't match len of found docs ({len(docs)})!"

        assert all([all([hasattr(x,name) for name in ['a','b','mata_var']]) for x in docs]), f"store_df: found docs do not have all attributes!"

        assert all([x.mata_var=='df' for x in docs]), f"store_df: meta_var not equal to 'df'"

        qs.delete()
        docs=list(self.TestDoc.objects(name__in=uuids))
        assert len(docs)==0, f"All docs should have been deleted, but {len(docs)} were found!"

        res=self.TestDoc.objects.update_or_create(files=[(df,dict(mata_var='df'))],mata_var2='update_or_create')
        res=list(res)
        assert len(res)==len(uuids), f"update_or_create: len of df ({len(uuids)}) doesn't match len of returned docs ({len(res)})!"

        qs=self.TestDoc.objects(name__in=uuids)
        docs=list(qs)
        assert len(docs)==len(uuids), f"update_or_create: len of df ({len(uuids)}) doesn't match len of found docs ({len(docs)})!"

        assert all([all([hasattr(x,name) for name in ['a','b','mata_var','mata_var2']]) for x in docs]), f"update_or_create: found docs do not have all attributes!"

        assert all([x.mata_var=='df' for x in docs]), f"update_or_create: meta_var not equal to 'df'"

        assert all([x.mata_var2=='update_or_create' for x in docs]), f"update_or_create: meta_var2 not equal to 'update_or_create'"

        qs.delete()
        docs=list(self.TestDoc.objects(name__in=uuids))
        assert len(docs)==0, f"All docs should have been deleted, but {len(docs)} were found!"

    def test_properties(self):
        if not hasattr(self,'TestDoc'):
            self.test_document()
        name=str(uuid.uuid4())
        res=self.TestDoc(name=name,meta_var='hello').save()

        doc=self.TestDoc.objects(name=name)[0]
        props=doc.properties
        assert props['name']==name and props['meta_var']=='hello', f'Properties for doc incorrect: {props}'

    # def test_drop_collection(self):
    #     self.TestDoc.drop_collection()
    #
    #     docs=list(self.TestDoc.objects())
    #     assert len(docs)==0, f"All docs should have been dropped, but {len(docs)} were found!"
