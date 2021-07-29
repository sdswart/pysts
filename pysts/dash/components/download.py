from .base import *
from .base_component import Component, Item
from flask import send_file,make_response
import pandas as pd
import threading
import weakref

def serve_df(name,df):
    resp = make_response(df.to_csv(index=False))
    resp.headers["Content-Disposition"] = f"attachment; filename={name}"
    resp.headers["Content-Type"] = "text/csv"
    return resp

class Download(Component):
    registered=False
    def __init__(self,filename=None,data=None,style='button',small=False):
        self.filename=filename
        self.data=data
        instances=type(self).get_instances()
        instances[self.id]=self#weakref.proxy(self)

        children=''
        if filename is not None:
            children=f'Download {filename}'
        self.layout = html.A(children=children,target='_blank',href=f'fetch/{self.id}')

        if style=='button':
            self.layout.obj.className="btn btn-primary"+(' btn-sm' if small else '')

        self.properties={'url':self.layout.href,
                        'label':self.layout.children}

    @classmethod
    def register(cls,app,prefix='/'):
        @app.server.route(prefix+"fetch/<string:download_id>")
        def download_table(download_id):
            return cls.serve(download_id)
        cls.registered=True
    @classmethod
    def serve(cls,download_id):
        downloads=cls.get_instances()
        if download_id in downloads:
            download=downloads[download_id]
            data=download.data
            filename=download.filename
            if isinstance(data,pd.DataFrame):
                return serve_df(filename,data)
            elif isinstance(data,str) and os.path.isfile(data):
                return send_file(data,attachment_filename=filename)
        return 'Resource not found',404
    @classmethod
    def get_instances(cls):
        if not hasattr(cls, 'instances'):
            cls.instances = {}
        return cls.instances
