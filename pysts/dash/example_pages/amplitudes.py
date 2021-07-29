import pandas as pd
import numpy as np
from urllib.parse import quote as urlquote

from .. import components
from ..components.base import *
from ..config import *
from ..utils import spectra_and_baseline

class Amplitudes(components.Component):
    def __init__(self,main_app):
        self.properties={}
        self.main_app=main_app
    def layout(self):
        self.alert=components.Alert()
        self.location=dcc.Location(id='url',refresh=False)

        self.selected_spectra=components.File_Browser(label='Spectra',multi=True,
                allowed_extensions=SPECTRA_EXTENSIONS,upload_to=SPECTRA_PATH,keep_old_uploads=True)
        self.background=dcc.Checklist(options=[{'label': 'Remove background', 'value': 'rm'}],value=[],className="p-2")

        # Configuration and regions
        default_config,default_value,config_options,configs=self.load_configs()
        self.cur_regions=pd.DataFrame({},columns=CONFIG_COLUMNS) if default_value is None else pd.read_csv(default_value)

        self.config_dropdown=components.Dropdown(value=default_value,options=config_options,multi=False,label=None)

        def check_config_uploads(file_paths):
            msg='';color='success'
            res={}
            for filename,path in file_paths.items():
                df=pd.read_csv(path)
                if set(df.columns)!=set(CONFIG_COLUMNS):
                    msg=f'{filename} did not have the correct columns, which should be: {", ".join(CONFIG_COLUMNS)}'
                    color='danger'
                else:
                    res[filename]=path
            return msg,color,res

        self.config_upload=components.Uploader(type='button',multi=False,label='Config',style=None,allowed_extensions=['.csv'],upload_to=CONFIG_PATH,dropdown=self.config_dropdown,keep_old_uploads=True,condition_check=check_config_uploads,alert=self.alert)
        self.config_upload.layout.obj.style={'display':'inline-block'}

        self.download_config = components.Download(filename=default_config,data=self.cur_regions).className('ml-2 mr-2')
        self.config_save_label=html.Label('Save configuration as:')
        self.config_save_input=dcc.Input(value=default_config,className='w-100')
        self.config_save_modal=components.Modal(open_label='Save As',header=f'Save {default_config}',body=html.Div([self.config_save_label,self.config_save_input]),action_label='Save',show_cancel=True)

        self.regions=dash_table.DataTable(columns=[{"name": i, "id": i} for i in self.cur_regions.columns],data=self.cur_regions.to_dict('records'),editable=True,
                                row_selectable='multi',selected_rows=list(range(self.cur_regions.shape[0])),css=[{'selector': '.row', 'rule': 'margin: 0'}])
        config_tools=html.Div([self.config_save_modal,
                            dbc.Row([dbc.Col(self.config_dropdown,width=4),dbc.Col([self.config_save_modal.open,self.download_config,self.config_upload])],
                            className='pb-2',justify="start")])

        #Results
        self.graph=dcc.Graph(figure=go.Figure(layout={'height':700,'xaxis_title':'Wavelength (µm)','yaxis_title':'Intensity'}))
        self.results_table_download=components.Download(filename='Results.csv',small=True)
        self.results_table=dash_table.DataTable(editable=False,css=[{'selector': '.row', 'rule': 'margin: 0'}])
        self.result_card=components.Tabs({'Spectra Chart':dcc.Loading(self.graph),
                                        'Table Results':dcc.Loading([
                                            dbc.Row(self.results_table_download,justify="start",className='pb-2'),
                                            dbc.Row(self.results_table)
                                        ])})

        self.combiner=html.Div('Not ready',style={'display':'None'})
        self.full_layout=html.Div([self.combiner,
                self.alert,
                self.location,
                dbc.Row([
                    dbc.Col(html.Div([self.selected_spectra,self.background]),width=4),
                    dbc.Col(html.Div([config_tools,html.Div(self.regions)]))
                ]),
                dbc.Row(dbc.Col(self.result_card))
            ],className='p-3')

        self.load_callbacks()
        return self.full_layout
    def load_configs(self,default_config='default.csv'):
        configs={x:os.path.join(CONFIG_PATH,x) for x in os.listdir(CONFIG_PATH) if x.endswith('.csv')}
        default_value=configs[default_config] if default_config in configs else None
        return default_config,default_value,[{'label': label, 'value': path} for label,path in configs.items()],configs
    def load_callbacks(self):

        @self([self.config_dropdown.value,self.config_dropdown.options],self.main_app.tabs.active_tab,self.config_dropdown.value)
        def change_configs(active_tab,cur_value):
            if active_tab=='Amplitudes':
                default_config,default_value,config_options,configs=self.load_configs()
                if cur_value not in list(configs.values()):
                    cur_value=default_value
                return cur_value,config_options

        #Load new config into regions table
        @self([self.regions.columns,self.regions.data,self.regions.selected_rows,self.download_config.label,self.config_save_input.value,self.config_save_modal.header],self.config_dropdown.value)
        def change_config(config_path):
            if isinstance(config_path,str) and os.path.isfile(config_path):
                filename=os.path.split(config_path)[1]
                df=pd.read_csv(config_path)
                self.download_config.data=df
                self.download_config.filename=filename
                return [{"name": i, "id": i} for i in df.columns],df.to_dict('records'),list(range(df.shape[0])),f'Download {filename}',filename, f'Save {filename}'

        #Load cur_regions
        @self(self.combiner.children,[self.regions.columns,self.regions.data,self.regions.selected_rows])
        def load_cur_regions(regions_cols,regions_data,selected_rows):
            self.cur_regions=pd.DataFrame.from_records([x for i,x in enumerate(regions_data) if i in selected_rows],columns=[x['name'] for x in regions_cols])
            return 'Config Ready'

        #Save config
        @self([self.config_dropdown.options,self.config_dropdown.value],self.config_save_modal.action.n_clicks,[self.config_save_input.value,self.config_dropdown.options,self.config_dropdown.value,self.regions.columns,self.regions.data])
        def save_config_as(action_clicks,name,options,values,regions_cols,regions_data):
            if name.endswith('.csv') and self.cur_regions is not None:
                path=os.path.join(CONFIG_PATH,name)
                df=pd.DataFrame.from_records(regions_data,columns=[x['name'] for x in regions_cols])
                df.to_csv(path,index=False)
                new_options=[{'label': name, 'value': path}]
                new_values=[path]
                return components.add_to_dropdown(new_options,new_values,options,values,multi=False,keep_old_uploads=True)

        #load Spectra
        self.spectra=[]
        @self(self.combiner.children,self.selected_spectra.value)
        def load_spectra(paths):
            for data in self.spectra:
                data['show']=data['path'] in paths
            for path in paths:
                if os.path.isfile(path):
                    if any([path==x['path'] for x in self.spectra]):
                        continue
                    filename=os.path.split(path)[1]
                    spectra,wl,spectra_nb,baseline=spectra_and_baseline(path)
                    self.spectra.append({'name':filename,
                                        'path':path,
                                        'spectra':spectra,
                                        'wl':wl,
                                        'spectra_nb':spectra_nb,
                                        'baseline':baseline,
                                        'show':True})
            return 'Spectra Ready'


        #RESULTS

        #Figure
        @self([self.graph.figure],[self.combiner.children,self.background.value])
        def display_fig(info,remove_background):
            fig=go.Figure(layout={'height':700,'xaxis_title':'Wavelength (µm)','yaxis_title':'Intensity'})

            for data in self.spectra:
                if data['show']:
                    y=data['spectra'] if len(remove_background)==0 else data['spectra_nb']
                    fig.add_trace(go.Scatter(x=data['wl'], y=y, name=data['name']))

            fig.layout.update(
            shapes=[dict(type="rect",xref="x",yref="paper",x0=float(row['Lower WL']),y0=0,x1=float(row['Upper WL']),y1=1,
                    fillcolor="LightSalmon",line=dict(color="Red",width=1,),opacity=0.5,layer="below") for i,row in self.cur_regions.iterrows()])
            fig.layout.update(showlegend=True)
            return fig

        #Table
        @self([self.results_table.columns,self.results_table.data],[self.combiner.children])
        def display_table(info):
            res_table={'Name':[],'wl start':[],'wl end':[],
                       'Min peak':[],'Peak':[],'Max peak':[],
                       'Min background':[],'Background':[],'Max background':[],
                       'Min S/B':[],'S/B':[],'Max S/B':[],'Pass':[]}

            for data in self.spectra:
               if data['show']:
                   for i,row in self.cur_regions.iterrows():
                       row_val=lambda name,val_if_nan: val_if_nan if row[name] is None or np.isnan(row[name]) else row[name]
                       wl_s=float(row['Lower WL']);wl_e=float(row['Upper WL'])
                       pos=np.logical_and(data['wl']>=wl_s,data['wl']<=wl_e)
                       wl_max=np.argmax(data['spectra'][pos])
                       peak=data['spectra'][pos][wl_max];peak_nb=data['spectra_nb'][pos][wl_max]
                       cur_baseline=peak-peak_nb #baseline[pos][wl_pos]
                       stb=peak/cur_baseline
                       res_table['Name'].append(data['name'])
                       res_table['wl start'].append(wl_s)
                       res_table['wl end'].append(wl_e)
                       res_table['Min peak'].append(row['Min Peak'])
                       res_table['Peak'].append(peak)
                       res_table['Max peak'].append(row['Max Peak'])
                       res_table['Min background'].append(row['Min Background'])
                       res_table['Background'].append(cur_baseline)
                       res_table['Max background'].append(row['Max Background'])
                       res_table['Min S/B'].append(row['Min S/B'])
                       res_table['S/B'].append(stb)
                       res_table['Max S/B'].append(row['Max S/B'])
                       res_table['Pass'].append('Yes' if row_val('Min Peak',-np.inf)<=peak<=row_val('Max Peak',np.inf) and
                                                         row_val('Min Background',-np.inf)<=cur_baseline<=row_val('Max Background',np.inf) and
                                                         row_val('Min S/B',-np.inf)<=stb<=row_val('Max S/B',np.inf) else 'No')

            df=pd.DataFrame(res_table).round(1)
            self.results_table_download.data=df
            return [{"name": i, "id": i} for i in df.columns],df.to_dict('records')
