from .base import *
from .base_component import Component
from .dropdown import add_to_dropdown
import json, os, base64
from copy import copy

def parse_contents(content, filename, date,upload_to):
    data = content.encode("utf8").split(b";base64,")[1]
    full_path=os.path.join(upload_to,filename)
    with open(full_path, "wb") as fp:
        fp.write(base64.decodebytes(data))
    return filename,full_path

def upload_to_folder(contents, names, dates,upload_to, allowed_extensions=None):
    res={}
    if contents is not None:
        for c, n, d in zip(contents, names, dates):
            if allowed_extensions is None or any([n.lower().endswith(ext) for ext in allowed_extensions]):
                if upload_to is None:
                    new_name=new_path=n
                else:
                    new_name,new_path=parse_contents(c, n, d, upload_to)
                res[new_name]=new_path
    return res

class Uploader(Component):
    def __init__(self,type='area',multi=False,label=None,style=None,allowed_extensions=None,upload_to=None,dropdown=None,keep_old_uploads=True,condition_check=None,alert=None):
        if label is None:
            label='Files' if multi else 'File'

        if style is None: style={}
        if type=='area':
            children=html.Div(['Drag and Drop or ',html.A(f'Select {label}')])
            style.update({
                'width': '100%',
                'height': '60px',
                'lineHeight': '60px',
                'borderWidth': '1px',
                'borderStyle': 'dashed',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin-top':'10px'
            })
        elif type=='link':
            children=html.A(f'Select {label}')
        else:
            children=html.A(f'Upload {label}', className="btn btn-primary", style={'color':'#FFFFFF'})

        self.upload=dcc.Upload(children=children,style=style,multiple=multi)
        self.upload_info=html.Div('{}',style={'display':'none'})

        self.layout=html.Div([self.upload_info,self.upload])

        self.properties={'contents':self.upload.contents,
                        'filename':self.upload.filename,
                        'last_modified':self.upload.last_modified,
                        'info':self.upload_info.children}

        #callbacks
        @self([self.info]+([alert.alerts] if alert is not None else []),[self.contents,self.filename,self.last_modified],[self.info]+([alert.alerts] if alert is not None else []))
        def _upload_fcn(contents, names, dates, upload_data, alerts=None):
            if alerts is None: alerts=[]
            if not multi:
                contents=[contents]; names=[names]; dates=[dates]
            res=upload_to_folder(contents, names, dates,upload_to, allowed_extensions=allowed_extensions)

            msg='';color='primary'
            if condition_check is not None:
                msg,color,new_res=condition_check(copy(res))
                for path in list(res.values()):
                    if os.path.isfile(path) and path not in list(new_res.values()):
                        os.remove(path)
                    res=new_res

            if msg == '' and len(res)>0:
                color='success'
                msg=f'{", ".join(list(res))} {"were" if len(res)>1 else "was"} added successfully!'

            if keep_old_uploads:
                res.update(json.loads(upload_data))

            res=[json.dumps(res)]
            if alert is not None:
                alerts.append(alert.create(msg,color))
                res.append(alerts)

            return tuple(res)

        if dropdown is not None:
            self.properties.update({'value':dropdown.value,
                                    'options':dropdown.options})

            @self([dropdown.options,dropdown.value],self.info,[dropdown.options,dropdown.value])
            def upload_add_to_dropdown(upload_data,options,values):
                upload_data=json.loads(upload_data)
                new_options = [{'label': name, 'value': path} for name,path in upload_data.items()]
                new_values = list(upload_data.values())
                return add_to_dropdown(new_options,new_values,options,values,multi=multi,keep_old_uploads=keep_old_uploads)
