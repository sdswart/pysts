from dash.dependencies import Input, Output, State, MATCH, ALL

from flask import Flask, session
from ..config import *

def add_callbacks(app):
    get_list_item = lambda x,name=None: dbc.ListGroupItem(x,id={'type':'folder-item','index': x},style={'padding': '3px 10px','cursor': 'pointer'},className="list-group-item-action",n_clicks=0)



    @app.callback([Output('folder-list', 'children')],
                  [Input('full_path_root', 'value')])
    def full_path_root(path):
        if path is not None and os.path.isdir(path):
            names=['...']+[x for x in os.listdir(path) if any([x.lower().endswith(ext) for ext in SPECTRA_EXTENSIONS]) or os.path.isdir(os.path.join(path,x))]
        else:
            names=['Transfer','Server']
        children=[get_list_item(x) for x in list(names)]
        return [dbc.ListGroup(children,style={"maxHeight": "300px", "overflow-y": "auto"})]

    def parse_contents(content, filename, date):
        data = content.encode("utf8").split(b";base64,")[1]
        full_path=os.path.join(SPECTRA_PATH,filename)
        with open(full_path, "wb") as fp:
            fp.write(base64.decodebytes(data))
        return filename,full_path


    @app.callback([Output('full_path_root', 'value'),Output('selected_spectra', 'value'),Output('selected_spectra', 'options')],
                  [Input({'type':'folder-item','index':ALL}, 'n_clicks'),Input('upload-data', 'contents')],
                  [State('full_path_root', 'value'),State('selected_spectra', 'value'),State('selected_spectra', 'options'),State('upload-data', 'filename'),State('upload-data', 'last_modified')])
    def change_folder(inputs,list_of_contents,full_path_root,values,options,list_of_names, list_of_dates):
        if full_path_root is None:
            full_path_root=''
        if list_of_contents is None or len(list_of_contents)==0:
            if len(inputs)>0 and max(inputs)>0:
                trigger=[x for x in dash.callback_context.inputs_list[0] if x['value']>0][0]
                text=trigger['id']['index']
                if os.path.isdir(full_path_root):
                    full_path=os.path.join(full_path_root,text)
                    if any([text.lower().endswith(ext) for ext in SPECTRA_EXTENSIONS]) and os.path.isfile(full_path):
                        if full_path not in values:
                            values.append(full_path)
                        option = {'label': text, 'value': full_path}
                        if option not in options:
                            options.append(option)
                    elif text=='...':
                        text=os.path.dirname(full_path_root)
                        folders=text.split(os.sep)
                        if text==full_path_root or not os.path.isdir(text) or (folders[0].lower().startswith('e:') and len(folders)<3):
                            text=''
                        full_path_root=text
                    elif os.path.isdir(full_path):
                        full_path_root=full_path
                elif text=='Transfer':
                    full_path_root='\\\\HOTH\\Transfer\\'
                elif text=='Server':
                    full_path_root='E:\\Shared Folders\\GTLDMS\\'
                elif os.path.isdir(text):
                    full_path_root=text
        else:
            for c, n, d in zip(list_of_contents, list_of_names, list_of_dates):
                if any([n.lower().endswith(ext) for ext in SPECTRA_EXTENSIONS]):
                    new_name,new_path=parse_contents(c, n, d)
                    option = {'label': new_name, 'value': new_path}
                    if option not in options:
                        options.append(option)
                    if new_path not in values:
                        values.append(new_path)
        return (full_path_root,values,options)

    def get_table_results_df(spectras,regions):
        #['Lower WL','Upper WL','Min Peak','Max Peak','Min Background','Max Background']
        res_table={'name':[],'wl start':[],'wl end':[],
                   'min peak':[],'peak':[],'max peak':[],
                   'min background':[],'background':[],'max background':[],
                   'signal to background':[],'pass':[]}
        for name,(wl,spectra,spectra_nb,baseline) in spectras.items():
            for i,row in regions.iterrows():
                wl_s=float(row['Lower WL']);wl_e=float(row['Upper WL'])
                pos=np.logical_and(wl>=wl_s,wl<=wl_e)
                wl_max=np.argmax(spectra[pos])
                peak=spectra[pos][wl_max];peak_nb=spectra_nb[pos][wl_max]
                cur_baseline=peak-peak_nb #baseline[pos][wl_pos]
                res_table['name'].append(name)
                res_table['wl start'].append(wl_s)
                res_table['wl end'].append(wl_e)
                res_table['min peak'].append(row['Min Peak'])
                res_table['peak'].append(peak)
                res_table['max peak'].append(row['Max Peak'])
                res_table['min background'].append(row['Min Background'])
                res_table['background'].append(cur_baseline)
                res_table['max background'].append(row['Max Background'])
                res_table['signal to background'].append(peak/cur_baseline)
                res_table['pass'].append('Yes' if row['Min Peak']<=peak<=row['Max Peak'] and row['Min Background']<=cur_baseline<=row['Max Background'] else 'No')
        return pd.DataFrame(res_table).round(1)

    def get_spectra(path):
        ref=list(spectra.open_spectra(path))
        ref_nb,baseline=spectra.remove_baseline(ref[0],lam=100, p=0.0001)
        return ref[1],ref[0],ref_nb.reshape(-1),baseline.reshape(-1)

    @app.callback([Output('chart', 'figure'),Output('results_table', 'children')],
                  [Input('selected_spectra', 'value'),Input('remove_background', 'value'),Input('regions', 'data')],
                  [State('chart', 'figure'),State('selected_spectra', 'options')])
    def get_results(selected_spectra,remove_background,regions_data,fig_data,spectra_options):
        spectra_options={x['value']:x['label'] for x in spectra_options}
        cur_regions=pd.DataFrame.from_records(regions_data)

        #Chart
        spectras={}
        fig=go.Figure(layout={'height':700,'xaxis_title':'Wavelength (µm)','yaxis_title':'Intensity'})
        fig_data={x['name']:{'x':x['x'],'y':x['y']} for x in fig_data['data']}
        for path in selected_spectra:
            name=spectra_options[path]
            wl,spectra,spectra_nb,baseline=get_spectra(path)
            spectras[name]=(wl,spectra,spectra_nb,baseline)
            y=spectra if len(remove_background)==0 else spectra_nb
            fig.add_trace(go.Scatter(x=wl, y=y, name=name))

        #Fig lines
        fig.layout.update(
        shapes=[dict(type="rect",xref="x",yref="paper",x0=float(row['Lower WL']),y0=0,x1=float(row['Upper WL']),y1=1,
                fillcolor="LightSalmon",line=dict(color="Red",width=1,),opacity=0.5,layer="below") for i,row in cur_regions.iterrows()])

        #Table
        df=get_table_results_df(spectras,cur_regions)

        table_results=[html.A('Download Results',href=,id='download_results',target="_blank",className='btn btn-sm'),
            dash_table.DataTable(
                columns=[{"name": i, "id": i} for i in df.columns],
                data=df.to_dict('records')
            )]
        return (fig,table_results)
