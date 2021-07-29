from .. import components
from ..components.base import *
from ..config import *
from ..utils import spectra_and_baseline
import pandas as pd
import numpy as np
from datetime import datetime

class EditFiles(components.Component):
    def __init__(self,name):
        self.dropdown=components.Dropdown(multi=True,options=None,update_value_on_change=True,keep_old_values=True,value=None)
        self.uploader=components.Uploader(type='area',multi=True,allowed_extensions=SPECTRA_EXTENSIONS,upload_to=SPECTRA_PATH,dropdown=self.dropdown,keep_old_uploads=True)
        self.layout=html.Div([
                        html.Label(f'{name}:'),
                        self.uploader,
                        dcc.Loading(self.dropdown)
                    ],className='border')

class Edit(components.Component):
    def __init__(self,overview):
        self.alert=components.Alert()

        self.new_unit_name=dcc.Input(value='',placeholder='Enter unit name...',style={'width':'600px'},className='ml-2')
        self.new_unit_loading=html.Div()
        self.add_unit=html.A('Add Unit',href='#',className="btn btn-primary mr-3",style={'width':'100px'})
        self.new_unit_clear=html.A('Clear',href='#',className="btn btn-secondary mr-3")

        self.coupons=EditFiles('Coupons')
        self.particles=EditFiles('Particles')

        self.results_table_download=components.Download(filename='Results.csv')
        self.delete_units=html.Div('')
        self.delete_modal=components.Modal(open_label='Delete Rows',open_color='danger',header='Delete Rows',body=html.Div(['Are you sure you want to delete the following units:',self.delete_units]),action_label='Delete',action_color='danger',show_cancel=True,small=True)
        self.download_results=components.Download(filename='Results.csv',data=overview.metrics,small=True).className('ml-3 mr-3')
        self.results_table=dash_table.DataTable(columns=[{"name": i, "id": i} for i in overview.metrics.columns],data=overview.metrics.to_dict('records'),
                                                row_selectable='multi',selected_rows=[],editable=False,css=[{'selector': '.row', 'rule': 'margin: 0'}])

        self.layout=html.Div([
            self.alert,
            dbc.Row(
                dbc.Card([
                    dbc.CardHeader(dbc.Row([
                        dbc.Col(['New Unit:',self.new_unit_name]),
                        dbc.Col(dcc.Loading(self.new_unit_loading))
                    ])),
                    dbc.CardBody([
                        dbc.Row([dbc.Col(self.coupons),dbc.Col(self.particles)]),
                        dbc.Row([dbc.Col(self.new_unit_clear),self.add_unit],justify='between',className='pt-3')
                    ])
                ,],className='w-100 m-4')),
            dbc.Row(
                dbc.Card([
                    dbc.CardHeader(dbc.Row(['Results:',self.delete_modal.open,self.download_results])),
                    dbc.CardBody([
                        #dbc.Row([self.delete_modal.open,self.download_results],justify="start",className='pb-2'),
                        dbc.Row(dcc.Loading(self.results_table)),
                    ])
                ,],className='w-100 m-4')),
            self.delete_modal
        ])

        #Clear uploads
        self.clear_clicks=0
        @self([self.coupons.dropdown.value,self.coupons.dropdown.options,self.particles.dropdown.value,self.particles.dropdown.options],[self.new_unit_clear.n_clicks,overview.info],[self.coupons.dropdown.value,self.particles.dropdown.value])
        def clear_uploads(clicks,info,coupon_paths,particle_paths):
            clear_now=self.clear_clicks!=clicks or info.startswith('Added')
            self.clear_clicks=clicks
            if clear_now:
                for path in coupon_paths+particle_paths:
                    if os.path.isfile(path):
                        os.remove(path)
                return [],[],[],[]

        #Add rows to delete modal
        @self(self.delete_units.children,[self.results_table.columns,self.results_table.data,self.results_table.selected_rows])
        def change_row_selection(cols,data,selected_rows):
            df=pd.DataFrame.from_records([x for i,x in enumerate(data) if i in selected_rows],columns=[x['name'] for x in cols])
            return ', '.join(list(df.SN.unique()))

        #Delete rows
        @self([overview.info,self.alert.alerts],self.delete_modal.action.n_clicks,[self.results_table.columns,self.results_table.data,self.results_table.selected_rows,self.alert.alerts])
        def delete_rows(clicks,cols,data,selected_rows,alerts):
            if alerts is None: alerts=[]
            df=pd.DataFrame.from_records([x for i,x in enumerate(data) if i not in selected_rows],columns=[x['name'] for x in cols])
            n_rows=overview.metrics.shape[0]-df.shape[0]
            overview.metrics=df
            alerts.append(self.alert.create(f'Successfully deleted {n_rows} rows','success'))
            return f'Deleted {n_rows} rows',alerts

        #get Results
        @self([overview.info,self.alert.alerts,self.new_unit_loading.children],self.add_unit.n_clicks,[self.new_unit_name.value,self.coupons.dropdown.value,self.particles.dropdown.value,self.alert.alerts])
        def get_results(clicks,name,coupons,particles,alerts):
            if alerts is None: alerts=[]
            if len(name)<4:
                alerts.append(self.alert.create('Please enter a longer name for the new unit','info'))
                return 'missing name',alerts,''
            if ',' in name:
                alerts.append(self.alert.create('You cannot use commas in the unit name','info'))
                return 'commas',alerts,''
            def path_res(path):
                spectra,wl,spectra_nb,baseline=spectra_and_baseline(path)
                wl_s=315;wl_e=316
                pos=np.logical_and(wl>=wl_s,wl<=wl_e)
                wl_max=np.argmax(spectra[pos])
                peak=spectra[pos][wl_max];peak_nb=spectra_nb[pos][wl_max]
                background=peak-peak_nb
                return peak,peak_nb,background,peak_nb/background
            def mean_res(paths):
                peaks=[];peak_nbs=[];backgrounds=[];sbrs=[]
                for i,path in enumerate(paths):
                    peak,peak_nb,background,sbr=path_res(path)
                    peaks.append(peak);peak_nbs.append(peak_nb);backgrounds.append(background);sbrs.append(sbr)
                return np.mean(peaks),np.mean(peak_nbs),np.mean(backgrounds),np.mean(sbrs)

            cpn_res=mean_res(coupons)
            ptl_res=mean_res(particles)

            now=datetime.now().replace(microsecond=0)
            df=pd.DataFrame({'SN':[name],'Date':[now],
                            'Coupon':[cpn_res[1]],'Coupon Background':[cpn_res[2]],'Coupon SBR':[cpn_res[3]],
                            'Particle':[ptl_res[1]],'Particle Background':[ptl_res[2]],'Particle SBR':[ptl_res[3]]}).round(2)

            overview.metrics=pd.concat([overview.metrics,df])
            alerts.append(self.alert.create(f'{name} added successfully','success'))
            return f'Added {name}',alerts,''

        #Change table and save metrics
        @self([self.results_table.columns,self.results_table.data,self.results_table.selected_rows],overview.info)
        def metrics_changed(info):
            overview.metrics.to_csv(METRICS_PATH,index=False)
            self.download_results.data=overview.metrics
            return [{"name": i, "id": i} for i in overview.metrics.columns],overview.metrics.to_dict('records'),[]
