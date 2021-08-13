import pytest
import uuid
import pandas as pd

from pysts.db.mongodb import Document, fields, QuerySet
from mongoengine import connect

class TestClass:
    '''
    Test MongoEngine
    '''
    def test_connect(self):
        self.con=connect('pysts_tests', host='l3-37:27017', port=27017)
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
        assert len(docs)==2, f"Finding the 2 docs returned {len(res)} docs!"

        assert docs[0].meta_var=='hello' and docs[1].meta_var=='hello', "The meta_var for the two docs were not changed to 'hello'!"

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

    # def test_drop_collection(self):
    #     self.TestDoc.drop_collection()
    #
    #     docs=list(self.TestDoc.objects())
    #     assert len(docs)==0, f"All docs should have been dropped, but {len(docs)} were found!"
