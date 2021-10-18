from .base import *
from .base_component import Component
from .uploader import Uploader
from .dropdown import Dropdown, add_to_dropdown
from ..utils import get_dynamic_values
import os

class File_Browser(Component):
    def __init__(self,label=None,multi=True,starting_folders=None, #list of paths or list of (label,path) [('Transfer','\\\\HOTH\\Transfer\\'),('Server','E:\\Shared Folders\\GTLDMS\\')]
            allowed_extensions=None,upload_to=None,keep_old_uploads=True):

        if label is None:
            label='Files' if multi else 'File'
        if starting_folders is None:
            if 'EXTERNAL_ROOT' in os.environ and os.path.isdir(os.environ['EXTERNAL_ROOT']):
                EXTERNAL_ROOT=os.environ['EXTERNAL_ROOT']
                starting_folders=[os.path.join(EXTERNAL_ROOT,x) for x in os.listdir(EXTERNAL_ROOT)]
            else:
                starting_folders=[]

        starting_folders_dict={}
        for fol in starting_folders:
            if isinstance(fol,str):
                starting_folders_dict[fol]=fol
            else:
                starting_folders_dict[fol[0]]=fol[1]

        self.path=dcc.Input(value='',className='w-100',disabled=True)
        self.folder_list=html.Div()
        self.folder_item=dbc.ListGroupItem

        children = [self.path,self.folder_list]

        self.dropdown=Dropdown(multi=multi,label=f'Selected {label}')

        if upload_to is not None:
            self.uploader=Uploader(type='area',multi=multi,label=label,allowed_extensions=allowed_extensions,upload_to=upload_to,dropdown=self.dropdown,keep_old_uploads=keep_old_uploads)
            children.append(self.uploader)

        children.append(self.dropdown)
        self.layout = html.Div(children)

        self.properties={'path':self.path.value,
                        'dir':self.folder_list.children,
                        'items':self.folder_item.children,
                        'value':self.dropdown.value,
                        'options':self.dropdown.options}

        #Base_callbacks
        #Show files in current folder
        get_list_item = lambda x,name=None: self.folder_item(x,index=x,style={'padding': '3px 10px','cursor': 'pointer'},className="list-group-item-action",n_clicks=0)
        @self(self.dir,self.path,self.dir)
        def show_folder_contents(path,folders):
            names=list(starting_folders_dict)
            if path is not None:
                if os.path.isfile(path):
                    return folders
                elif os.path.isdir(path):
                    names=['...']+[x for x in os.listdir(path) if allowed_extensions is None or any([x.lower().endswith(ext) for ext in allowed_extensions]) or os.path.isdir(os.path.join(path,x))]

            children=[get_list_item(x) for x in list(names)]
            return [dbc.ListGroup(children,style={"maxHeight": "300px", "overflow-y": "auto"})]

        #change dropdown
        @self([self.dropdown.options,self.dropdown.value],self.path,[self.dropdown.options,self.dropdown.value])
        def file_browser_add_to_dropdown(full_path,options,values):
            if os.path.isfile(full_path):
                new_values=[full_path]
                new_options = [{'label': os.path.split(full_path)[1], 'value': full_path}]
                return add_to_dropdown(new_options,new_values,options,values,multi=multi,keep_old_uploads=keep_old_uploads)
            return None

        #Change full-path text
        @self(self.path,self.folder_item.n_clicks,self.path)
        def change_full_path(inputs,full_path_root):
            if full_path_root is None: full_path_root=''
            if len(inputs)>0 and max(inputs)>0:
                index_vals=get_dynamic_values(self.folder_item.id[0]['type'])
                text=[index for index,val in index_vals.items() if val>0][0]
                full_path_root_isdir=os.path.isdir(full_path_root)
                if text in starting_folders_dict:
                    full_path_root = starting_folders_dict[text]
                elif full_path_root_isdir or os.path.isfile(full_path_root):
                    full_path=os.path.join(full_path_root,text)
                    if text=='...':
                        text=os.path.dirname(full_path_root if full_path_root_isdir else os.path.dirname(full_path_root))
                        folders=text.split(os.sep)
                        if text==full_path_root or not os.path.isdir(text) or (folders[0].lower().startswith('e:') and len(folders)<3):
                            text=''
                        full_path_root=text
                    elif os.path.isdir(full_path) or os.path.isfile(full_path):
                        full_path_root=full_path
                elif os.path.isdir(text):
                    full_path_root=text
            return full_path_root
